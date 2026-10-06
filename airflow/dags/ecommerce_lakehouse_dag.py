"""
DAG: E-Commerce Medallion Lakehouse Orchestration Pipeline
Modern Airflow 2.x TaskFlow API implementation with Data Quality Firewall & Quarantine Monitoring.
Orchestrates Bronze Ingestion -> Silver Transformation (with DLQ Quarantine) -> Gold dbt Marts.
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
    description="End-to-End Modern Medallion Lakehouse Pipeline with Data Quality Quarantine Firewall",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "lakehouse", "medallion", "dbt", "minio", "quarantine"],
)
def ecommerce_lakehouse_pipeline():

    @task(task_id="ingest_orders_to_bronze")
    def task_ingest() -> str:
        """Ingests a fresh batch of synthetic orders (with occasional anomalies) into Bronze."""
        logger.info("Starting Bronze layer ingestion...")
        s3_key = ingest_batch_to_bronze(batch_size=40)
        logger.info(f"Ingested batch saved to: {s3_key}")
        return s3_key

    @task(task_id="transform_and_quarantine_silver")
    def task_transform() -> dict:
        """
        Validates records, writes clean Parquet to orders/,
        and routes anomalous records to quarantine/ folder.
        """
        logger.info("Starting Silver transformation & Data Quality validation...")
        stats = process_all_bronze_to_silver()
        logger.info(f"Silver results: Clean={stats['total_clean']}, Quarantined={stats['total_quarantined']}")
        return stats

    @task(task_id="monitor_quarantine_metrics")
    def task_monitor_quarantine(stats: dict) -> None:
        """Inspects quarantine counts and triggers warnings if anomalous records were detected."""
        quarantined_count = stats.get("total_quarantined", 0)
        clean_count = stats.get("total_clean", 0)
        
        if quarantined_count > 0:
            logger.warning(
                f"🚨 DATA QUALITY ALERT: {quarantined_count} records failed validation and were routed to "
                f"s3://ecommerce-silver/quarantine/! (Clean records processed: {clean_count})"
            )
        else:
            logger.info("✅ Perfect batch quality: 0 records routed to quarantine.")

    @task.bash(task_id="dbt_build_gold_models_and_tests")
    def task_dbt() -> str:
        """Executes dbt build (runs models + data quality tests) via DuckDB into MinIO."""
        return "dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"

    # Define clean pipeline execution lineage
    ingest_key = task_ingest()
    transform_stats = task_transform()
    quarantine_alert = task_monitor_quarantine(transform_stats)
    dbt_run = task_dbt()

    # Ingestion triggers Transformation
    ingest_key >> transform_stats
    
    # Transformation triggers both Quarantine Monitor and Gold Modeling concurrently
    transform_stats >> [quarantine_alert, dbt_run]


# Instantiate the DAG
lakehouse_dag = ecommerce_lakehouse_pipeline()
