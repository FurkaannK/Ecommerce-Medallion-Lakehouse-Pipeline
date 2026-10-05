FROM apache/airflow:2.9.2-python3.10

USER root
# Install system-level tools if required
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install custom Lakehouse dependencies (boto3, faker, pyarrow, dbt-duckdb, pandas)
COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt
