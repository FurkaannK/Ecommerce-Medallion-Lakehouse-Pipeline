"""
DAG: E-Commerce Medallion Lakehouse Orchestration Pipeline
Modern Airflow 2.x TaskFlow API implementation.
Orchestrates Bronze Ingestion -> Silver Parquet Transformation -> Gold dbt Marts.
"""

from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task

# Modular business logic from src/
from src.ingestion.generate_mock_orders import ingest_batch_to_bronze
from src.transformation.bronze_to_silver import process_all_bronze_to_silver

logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "Furkan",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="ecommerce_medallion_lakehouse_pipeline",
    default_args=default_args,
    description="End-to-End Modern Medallion Lakehouse Pipeline with TaskFlow API, MinIO, DuckDB, and dbt",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "lakehouse", "medallion", "dbt", "minio", "taskflow"],
)
def ecommerce_lakehouse_pipeline():

    @task(task_id="ingest_orders_to_bronze")
    def task_ingest() -> str:
        """Ingests a fresh batch of synthetic orders into MinIO Bronze Layer."""
        logger.info("Starting Bronze layer ingestion...")
        s3_key = ingest_batch_to_bronze(batch_size=35)
        logger.info(f"Ingested batch saved to: {s3_key}")
        return s3_key

    @task(task_id="transform_bronze_to_silver")
    def task_transform() -> list:
        """Reads raw JSONs from Bronze, flattens structures, and writes Parquet to Silver."""
        logger.info("Starting Silver transformation...")
        processed_keys = process_all_bronze_to_silver()
        logger.info(f"Successfully processed {len(processed_keys)} files to Silver layer.")
        return processed_keys

    @task.bash(task_id="dbt_run_gold_models")
    def task_dbt() -> str:
        """Executes dbt models using DuckDB to materialize Gold dimensional tables into MinIO."""
        return "dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"

    # Define clean pipeline execution dependencies
    ingest_result = task_ingest()
    transform_result = task_transform()
    dbt_result = task_dbt()

    ingest_result >> transform_result >> dbt_result


# Instantiate the DAG
lakehouse_dag = ecommerce_lakehouse_pipeline()
