import os
import json
import argparse
from helper_functions import upload_json, load_batch_to_postgres, mark_batch_as_uploaded_to_gcs, mark_batch_as_loaded_to_postgres, is_batch_uploaded_to_gcs, is_batch_loaded_to_postgres

# -------------------------
# ARGUMENT PARSING
# -------------------------
parser = argparse.ArgumentParser(description="Ingest FHIR batches into GCS/Postgres")
parser.add_argument("--batch", type=str, default=None,
                    help="Specific batch to process. If not provided, all batches are processed.")
args = parser.parse_args()
batch_arg = args.batch

# -------------------------
# CONFIG
# -------------------------
DATA_DIR = "/opt/airflow/data"
INGEST_DIR = os.path.dirname(__file__)
FHIR_ROOT = os.path.join(DATA_DIR, "Synthetic Denver")

# -------------------------
# SELECT BATCHES TO PROCESS
# -------------------------
if batch_arg:
    batches_to_process = [batch_arg]
else:
    batches_to_process = [b for b in os.listdir(FHIR_ROOT) if os.path.isdir(os.path.join(FHIR_ROOT, b))]

# -------------------------
# PROCESS BATCHES
# -------------------------
for batch_name in batches_to_process:
    batch_path = os.path.join(FHIR_ROOT, batch_name)
    print(f">>> Processing batch: {batch_name}")

    # ----- Upload JSON to GCS if not done -----
    if not is_batch_uploaded_to_gcs(batch_name):
        print(f">>> Uploading batch {batch_name} to GCS")
        for filename in os.listdir(batch_path):
            file_path = os.path.join(batch_path, filename)

            with open(file_path) as f:
                bundle = json.load(f)

            for entry in bundle.get("entry", []):
                resource = entry.get("resource", {})
                rtype = resource.get("resourceType")
                rid = resource.get("id")
                if rtype not in ["Patient", "Condition"] or not rid:
                    continue

                gcs_path = f"{batch_name}/{rtype.lower()}/{rid}.json"
                upload_json(resource, gcs_path)

        # Mark batch as uploaded in Postgres
        mark_batch_as_uploaded_to_gcs(batch_name)
    else:
        print(f">>> Batch {batch_name} already uploaded to GCS, skipping upload.")

    # ----- Load the batch into Postgres if not done -----
    if not is_batch_loaded_to_postgres(batch_name):
        print(f">>> Loading batch {batch_name} into Postgres")
        load_batch_to_postgres(batch_name)

        # Mark batch as loaded into Postgres
        mark_batch_as_loaded_to_postgres(batch_name)
    else:
        print(f">>> Batch {batch_name} already loaded into Postgres, skipping PG load.")

print("All requested batches processed successfully!")