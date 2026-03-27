import os
import pandas as pd
import psycopg2

from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from google.cloud import bigquery

# ----------------------------
# Load .env
# ----------------------------
load_dotenv()

# ----------------------------
# DAG default args
# ----------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ----------------------------
# Config
# ----------------------------
TABLES = [
    "stg_patients",
    "stg_conditions",
    "omop_person",
    "omop_condition_occurrence",
    "omop_concept",
    "mart_condition_summary",
    "mart_condition_prevalence",
    "mart_condition_by_gender",
    "mart_patient_demographics",
    "mart_patient_age_distribution"
]

BQ_PROJECT = os.getenv("GCP_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET")

print("GCP PROJECT ID:", BQ_PROJECT)
print("BQ DATASET:", BQ_DATASET)

# ----------------------------
# Function: start
# ----------------------------
def start_pipeline():
    print("Starting Postgres → BigQuery pipeline...")

# ----------------------------
# Function: load ONE table
# ----------------------------
def load_single_table(table_name: str):
    print(f"Loading table: {table_name}")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/creds/keys.json"

    client = bigquery.Client()

    with psycopg2.connect(
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASS"),
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DB")
    ) as conn:

        query = f"SELECT * FROM analytics.{table_name}"
        df = pd.read_sql_query(query, conn)

        print(f"Rows fetched from {table_name}: {len(df)}")

        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True
        )

        job = client.load_table_from_dataframe(
            df,
            table_id,
            job_config=job_config
        )

        job.result()

        print(f"Loaded {table_name} → {table_id}")

# ----------------------------
# DAG definition
# ----------------------------
dag = DAG(
    dag_id='fhir_omop_prod',
    default_args=default_args,
    description='Load Postgres analytics tables to BigQuery (parallel)',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

# ----------------------------
# Start task
# ----------------------------
start = PythonOperator(
    task_id='start',
    python_callable=start_pipeline,
    dag=dag,
)

# ----------------------------
# Dynamically create tasks
# ----------------------------
tasks = []

for table in TABLES:
    task = PythonOperator(
        task_id=f"load_{table}",
        python_callable=load_single_table,
        op_kwargs={"table_name": table},
        dag=dag,
    )
    start >> task   
    tasks.append(task)