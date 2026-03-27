import os
import json
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.dialects.postgresql import JSONB
from google.cloud import storage
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
ROOT_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = ROOT_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH)

# -------------------------
# GCP CONFIG
# -------------------------
GCS_BUCKET = os.environ.get("GCS_BUCKET")
GCP_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
DATASET = os.environ.get("BQ_DATASET")

storage_client = storage.Client.from_service_account_json(GCP_CREDENTIALS)
bucket = storage_client.bucket(GCS_BUCKET)

# -------------------------
# GCS UPLOAD
# -------------------------
def upload_json(data, gcs_path):
    blob = bucket.blob(gcs_path)
    if blob.exists():
        print(f"Skipping {gcs_path}, already exists")
        return
    blob.upload_from_string(json.dumps(data), content_type="application/json")
    print(f"Uploaded {gcs_path}")

# -------------------------
# POSTGRES CONFIG
# -------------------------
PG_USER = os.environ.get("PG_USER", "root")
PG_PASS = os.environ.get("PG_PASS", "root")
PG_HOST = os.environ.get("PG_HOST", "pgdatabase")
PG_PORT = os.environ.get("PG_PORT", 5432)
PG_DB   = os.environ.get("PG_DB", "fhir_omop")

engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

# -------------------------
# HELPER: ENSURE TABLE COLUMNS
# -------------------------
# def ensure_table_columns(table_name, df, engine):
#     inspector = inspect(engine)

#     # If table doesn't exist, it will be created automatically by df.to_sql
#     if table_name not in inspector.get_table_names():
#         return

#     existing_cols = [col["name"] for col in inspector.get_columns(table_name)]
#     for col in df.columns:
#         if col not in existing_cols:
#             # Add missing column; JSONB if dict/list, else TEXT
#             dtype = "JSONB" if df[col].apply(lambda x: isinstance(x, (dict, list))).any() else "TEXT"
#             engine.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {dtype};'))
#             print(f'Added missing column "{col}" to {table_name}')
from sqlalchemy import inspect, text

def ensure_table_columns(table_name, df, engine):
    inspector = inspect(engine)

    # If table doesn't exist, it will be created automatically by df.to_sql
    if table_name not in inspector.get_table_names():
        return

    existing_cols = [col["name"] for col in inspector.get_columns(table_name)]

    with engine.begin() as conn:
        for col in df.columns:
            if col not in existing_cols:
                # Detect JSON vs TEXT
                dtype = "JSONB" if df[col].apply(lambda x: isinstance(x, (dict, list))).any() else "TEXT"

                # Execute ALTER TABLE safely
                conn.execute(
                    text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {dtype};')
                )

                print(f'Added missing column "{col}" to {table_name}')

# -------------------------
# LOAD BATCH TO POSTGRES
# -------------------------
def load_batch_to_postgres(batch_name: str):
    ignore_fields = ["extension", "identifier", "text_status", "text_div"]

    for resource_type, table_name in [("patient", "raw_patients"), ("condition", "raw_conditions")]:
        prefix = f"{batch_name}/{resource_type}/"
        blobs = list(bucket.list_blobs(prefix=prefix))
        all_records = []

        for blob in tqdm(blobs, desc=f"Reading {resource_type}", unit="file"):
            try:
                data = json.loads(blob.download_as_text())
                all_records.append(data)
            except Exception as e:
                print(f"Skipping {blob.name}: {e}")

        if not all_records:
            continue

        # Flatten top-level fields
        df = pd.json_normalize(all_records)

        # Normalize column names
        df.columns = [c.replace(".", "_") for c in df.columns]

        # Drop ignored fields
        df = df.drop(columns=[c for c in ignore_fields if c in df.columns], errors="ignore")

        # Detect JSON columns (dicts/lists)
        json_cols = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, (dict, list))).any()]
        dtype_map = {col: JSONB for col in json_cols}

        # Ensure table has all columns
        ensure_table_columns(table_name, df, engine)

        # Insert into Postgres
        df.to_sql(
            table_name,
            con=engine,
            if_exists="append",
            index=False,
            dtype=dtype_map
        )

        print(f"Inserted {len(df)} rows into {table_name}")

# -------------------------
# TRACK COMPLETED BATCHES
# -------------------------
def mark_batch_as_uploaded_to_gcs(batch_name):
    """Mark the batch as uploaded to GCS and store the timestamp."""
    try:
        # Get the current timestamp with timezone (if needed)
        upload_gcs_datetime = datetime.now()  # Or datetime.utcnow() for UTC time

        with engine.connect() as conn:
            query = text("""
                INSERT INTO batch_tracking (batch_name, upload_gcs_complete, upload_gcs_datetime)
                VALUES (:batch_name, TRUE, :upload_gcs_datetime)
                ON CONFLICT (batch_name) 
                DO UPDATE 
                SET upload_gcs_complete = TRUE, upload_gcs_datetime = :upload_gcs_datetime
            """)

            # Execute query with parameters
            conn.execute(query, {
                'batch_name': batch_name,
                'upload_gcs_datetime': upload_gcs_datetime
            })

            # Commit the transaction to ensure changes are saved
            conn.commit()

            print(f"Batch {batch_name} successfully marked as uploaded.")
    except Exception as e:
        print(f"Error marking batch {batch_name} as uploaded: {e}")
        raise

def mark_batch_as_loaded_to_postgres(batch_name):
    """Mark the batch as loaded into Postgres and store the timestamp."""
    try:
        # Get the current timestamp with timezone (if needed)
        upload_postgres_datetime = datetime.now()  # Or datetime.utcnow() for UTC time

        with engine.connect() as conn:
            query = text("""
                INSERT INTO batch_tracking (batch_name, upload_postgres_complete, upload_postgres_datetime)
                VALUES (:batch_name, TRUE, :upload_postgres_datetime)
                ON CONFLICT (batch_name) 
                DO UPDATE 
                SET upload_postgres_complete = TRUE, upload_postgres_datetime = :upload_postgres_datetime
            """)

            # Execute query with parameters
            conn.execute(query, {
                'batch_name': batch_name,
                'upload_postgres_datetime': upload_postgres_datetime
            })

            # Commit the transaction to ensure changes are saved
            conn.commit()

            print(f"Batch {batch_name} successfully marked as loaded into Postgres.")
    except Exception as e:
        print(f"Error marking batch {batch_name} as loaded into Postgres: {e}")
        raise
 
def is_batch_uploaded_to_gcs(batch_name):
    """Check if the batch has already been uploaded to GCS."""
    with engine.connect() as conn:
        query = text("SELECT upload_gcs_complete FROM batch_tracking WHERE batch_name = :batch_name")
        
        # Execute the query with the provided batch name (using a dictionary)
        result = conn.execute(query, {'batch_name': batch_name}).fetchone()
    
    # Return True if the result is found and the upload_gcs_complete is True
    return result and result[0]

def is_batch_loaded_to_postgres(batch_name):
    """Check if the batch has already been loaded into Postgres."""
    with engine.connect() as conn:

        query = text("SELECT upload_postgres_complete FROM batch_tracking WHERE batch_name = :batch_name")
        
        # Execute the query with the provided batch name
        result = conn.execute(query, {'batch_name': batch_name}).fetchone()
    
    # Return True if the result is found and the upload_postgres_complete is True
    return result and result[0]