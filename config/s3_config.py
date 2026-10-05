import os
import logging
from typing import Optional
import boto3
from botocore.client import BaseClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("S3Config")

MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "minioadminpassword")
AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")

BRONZE_BUCKET: str = os.getenv("BRONZE_BUCKET", "ecommerce-bronze")
SILVER_BUCKET: str = os.getenv("SILVER_BUCKET", "ecommerce-silver")
GOLD_BUCKET: str = os.getenv("GOLD_BUCKET", "ecommerce-gold")

_s3_client: Optional[BaseClient] = None


def get_s3_client() -> BaseClient:
    """
    Initializes and returns a reusable singleton Boto3 S3 client
    configured for MinIO object storage.
    """
    global _s3_client
    if _s3_client is None:
        logger.info(f"Connecting to MinIO/S3 endpoint at {MINIO_ENDPOINT}...")
        _s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name=AWS_REGION,
        )
    return _s3_client
