FROM apache/airflow:3.1.8

USER airflow

# Install dbt adapters for Postgres and BigQuery
RUN pip install --user --no-cache-dir dbt-postgres dbt-bigquery uv

# Set PATH to include user's local bin (where pip --user installs packages)
ENV PATH="$PATH:/home/airflow/.local/bin"

# Copy uv project dependency files
WORKDIR /opt/airflow
COPY pyproject.toml uv.lock .python-version ./

# Sync dependencies from uv.lock
RUN uv sync --locked