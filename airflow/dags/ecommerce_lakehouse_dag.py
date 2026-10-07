"""
DAG: E-Commerce Medallion Lakehouse Orchestration Pipeline
Modern Airflow 2.x TaskFlow API implementation with Resilience Engineering:
  - Smart Exponential Backoff Retry Policy 
  - Data Quality Firewall & Quarantine DLQ 
  - End-to-End Incremental & Idempotent Processing 
"""

from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task

# Modular business logic from src/
from src.ingestion.generate_mock_orders import ingest_batch_to_bronze
from src.transformation.bronze_to_silver import process_all_bronze_to_silver

logger = logging.getLogger("airflow.task")


def on_pipeline_failure(context: dict) -> None:
    """
    Critical Failure Alerting Callback.
    Executes automatically when a task exhausts all retries and permanently fails.
    Extracts failure metadata, exception details, and formats a production-grade alert.
    """
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown_task"
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    execution_date = context.get("execution_date")
    exception = context.get("exception", "No exception message provided")
    log_url = task_instance.log_url if task_instance else "N/A"

    alert_banner = (
        "\n"
        "================================================================================\n"
        "🚨🚨🚨 CRITICAL PIPELINE ALERT: TASK PERMANENTLY FAILED 🚨🚨🚨\n"
        "================================================================================\n"
        f"  * DAG Name       : {dag_id}\n"
        f"  * Failed Task    : {task_id}\n"
        f"  * Execution Time : {execution_date}\n"
        f"  * Max Retries    : {task_instance.max_tries if task_instance else 'N/A'} attempts exhausted\n"
        f"  * Exception      : {exception}\n"
        f"  * Airflow Log URI: {log_url}\n"
    )
    logger.error(alert_banner)

    # Persist critical alert into a dedicated audit log file
    try:
        log_file_path = "/opt/airflow/logs/pipeline_failures.log"
        with open(log_file_path, "a", encoding="utf-8") as f:
            f.write(alert_banner)
        logger.info(f"Permanent failure record successfully written to: {log_file_path}")
    except Exception as err:
        logger.error(f"Failed to write failure banner to file: {err}")


default_args = {
    "owner": "Furkan",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # ---- Smart Exponential Backoff Retry Policy ----
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    # ----  Permanent Failure Alerting Callback ----
    "on_failure_callback": on_pipeline_failure,
}


@dag(
    dag_id="ecommerce_medallion_lakehouse_pipeline",
    default_args=default_args,
    description="End-to-End Modern Medallion Lakehouse Pipeline with Full Resilience & Quarantine Firewall",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "lakehouse", "medallion", "dbt", "minio", "quarantine", "resilient"],
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
        Incremental processing: Reads only new Bronze JSONs,
        validates records, writes clean Parquet to orders/,
        and routes anomalous records to quarantine/ folder.
        """
        logger.info("Starting Incremental Silver transformation & Data Quality validation...")
        stats = process_all_bronze_to_silver()
        logger.info(
            f"Silver results: New Files={stats.get('new_files_processed', 0)}, "
            f"Clean={stats['total_clean']}, Quarantined={stats['total_quarantined']}"
        )
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
        """Executes dbt build (runs incremental models + data quality tests) via DuckDB into MinIO."""
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
