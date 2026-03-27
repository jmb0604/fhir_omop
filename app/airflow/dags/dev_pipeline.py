import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import subprocess

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
# Paths inside Docker container
# ----------------------------
FHIR_ROOT = "/opt/airflow/data/Synthetic Denver"
INGEST_SCRIPT = "/opt/airflow/data/ingest/ingest_data.py"
DBT_PROJECT_DIR = "/opt/airflow/app/dbt/fhir_omop"

# ----------------------------
# DAG definition
# ----------------------------
dag = DAG(
    dag_id='fhir_omop_dev',
    default_args=default_args,
    description='Per-batch ingestion to GCS/Postgres and dbt run',
    schedule=None,  # manual trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

# ----------------------------
# Task: start
# ----------------------------
start = BashOperator(
    task_id='start',
    bash_command='echo "Starting per-batch pipeline"',
    dag=dag,
)

# ----------------------------
# Task: create batch tracking table (one-time)
# ----------------------------
def create_batch_tracking_table():
    """Run SQL to create batch tracking table in Postgres."""
    print("Creating batch tracking table...")

    # Define the CREATE TABLE SQL query
    create_table_query = """
        CREATE TABLE IF NOT EXISTS batch_tracking (
            batch_name VARCHAR(255) PRIMARY KEY,
            upload_gcs_complete BOOLEAN DEFAULT FALSE,
            upload_gcs_datetime TIMESTAMP,
            upload_postgres_complete BOOLEAN DEFAULT FALSE,
            upload_postgres_datetime TIMESTAMP
        );
    """

    # Connect to the PostgreSQL database
    try:
        conn = psycopg2.connect(
            user=os.getenv("PG_USER", "root"),
            password=os.getenv("PG_PASS", "root"),
            host=os.getenv("PG_HOST", "pgdatabase"),
            port=os.getenv("PG_PORT", 5432),
            dbname=os.getenv("PG_DB", "fhir_omop")
        )
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()
            print("Batch tracking table created successfully!")
    except Exception as e:
        print(f"Error creating table: {e}")
    finally:
        if conn:
            conn.close()

create_table_task = PythonOperator(
    task_id='create_batch_tracking_table',
    python_callable=create_batch_tracking_table,
    dag=dag,
)


# ----------------------------
# Task: dbt seed (one-time)
# ----------------------------
dbt_seed = BashOperator(
    task_id='dbt_seed',
    bash_command=f'cd {DBT_PROJECT_DIR} && dbt seed --target dev',
    dag=dag,
)

# ----------------------------
# Dynamically create tasks per batch
# ----------------------------
for batch_name in os.listdir(FHIR_ROOT):
    batch_path = os.path.join(FHIR_ROOT, batch_name)
    if not os.path.isdir(batch_path):
        continue

    safe_batch_name = batch_name.replace(" ", "_")

    # PythonOperator: ingest batch
    def ingest_batch(batch=batch_name):
        subprocess.run(['uv', 'run', INGEST_SCRIPT, '--batch', batch], check=True)

    ingest_task = PythonOperator(
        task_id=f'ingest_{safe_batch_name}',
        python_callable=ingest_batch,
        dag=dag,
    )

    # BashOperator: run dbt models for this batch
    dbt_run_task = BashOperator(
        task_id=f'dbt_run_{safe_batch_name}',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --target dev --full-refresh',
        dag=dag,
    )

    # Task dependencies
    start >> create_table_task >> dbt_seed >> ingest_task >> dbt_run_task