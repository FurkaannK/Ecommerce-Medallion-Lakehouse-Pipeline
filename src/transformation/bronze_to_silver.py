"""
Bronze to Silver Transformation Module
Reads raw JSON batches from Bronze S3 bucket, flattens nested structures,
enforces schema and data quality, and writes optimized Parquet files into Silver S3 bucket.
"""

import io
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config.s3_config import BRONZE_BUCKET, SILVER_BUCKET, get_s3_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("SilverTransformation")


def list_bronze_json_files(prefix: str = "orders/") -> List[str]:
    """Lists all JSON file keys from the Bronze bucket under a given prefix."""
    s3_client = get_s3_client()
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=prefix)
    
    file_keys = []
    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                if obj["Key"].endswith(".json"):
                    file_keys.append(obj["Key"])
    return file_keys


def flatten_order_batch(raw_orders: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Flattens nested JSON orders into a granular DataFrame (one row per order item).
    Extracts customer, payment, item info and casts appropriate data types.
    """
    rows = []
    
    for order in raw_orders:
        order_id = order.get("order_id")
        created_at = order.get("created_at")
        order_status = order.get("order_status")
        
        customer = order.get("customer", {})
        customer_id = customer.get("customer_id")
        customer_name = f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip()
        customer_email = customer.get("email")
        customer_city = customer.get("city")
        customer_state = customer.get("state")
        customer_country = customer.get("country")
        
        payment = order.get("payment", {})
        payment_method = payment.get("payment_method")
        currency = payment.get("currency")
        order_total_amount = float(payment.get("total_amount", 0.0))
        shipping_fee = float(payment.get("shipping_fee", 0.0))
        tax_amount = float(payment.get("tax_amount", 0.0))
        
        # Unnest items (Order Line Items)
        items = order.get("items", [])
        for item in items:
            product_id = item.get("product_id")
            product_name = item.get("product_name")
            category = item.get("category")
            unit_price = float(item.get("unit_price", 0.0))
            quantity = int(item.get("quantity", 1))
            line_item_total = round(unit_price * quantity, 2)
            
            rows.append({
                "order_id": order_id,
                "order_status": order_status,
                "order_timestamp": created_at,
                "customer_id": customer_id,
                "customer_name": customer_name,
                "customer_email": customer_email,
                "customer_city": customer_city,
                "customer_state": customer_state,
                "customer_country": customer_country,
                "product_id": product_id,
                "product_name": product_name,
                "product_category": category,
                "unit_price": unit_price,
                "quantity": quantity,
                "line_item_total": line_item_total,
                "payment_method": payment_method,
                "currency": currency,
                "order_total_amount": order_total_amount,
                "shipping_fee": shipping_fee,
                "tax_amount": tax_amount,
            })
            
    df = pd.DataFrame(rows)
    
    if df.empty:
        return df

    # Enforce data types & data hygiene
    df["order_timestamp"] = pd.to_datetime(df["order_timestamp"], utc=True)
    df["quantity"] = df["quantity"].astype("int32")
    df["unit_price"] = df["unit_price"].astype("float64")
    df["line_item_total"] = df["line_item_total"].astype("float64")
    df["order_total_amount"] = df["order_total_amount"].astype("float64")
    df["shipping_fee"] = df["shipping_fee"].astype("float64")
    df["tax_amount"] = df["tax_amount"].astype("float64")
    
    # Audit column for lineage
    df["silver_processed_at"] = datetime.now(timezone.utc)
    
    return df


def process_bronze_file_to_silver(s3_key: str) -> str:
    """
    Downloads a single raw JSON from Bronze, transforms it to Parquet,
    and uploads to Silver bucket using hive partitioning.
    """
    s3_client = get_s3_client()
    logger.info(f"Processing Bronze file: s3://{BRONZE_BUCKET}/{s3_key}")
    
    # Download JSON content from Bronze
    response = s3_client.get_object(Bucket=BRONZE_BUCKET, Key=s3_key)
    raw_content = response["Body"].read().decode("utf-8")
    orders_data = json.loads(raw_content)
    
    # Transform & Flatten
    df = flatten_order_batch(orders_data)
    if df.empty:
        logger.warning(f"No orders found in {s3_key}. Skipping.")
        return ""
    
    # Generate Silver Parquet Key
    # Replace .json with .parquet and change bucket namespace
    parquet_key = s3_key.replace(".json", ".parquet")
    
    # Convert DataFrame to Parquet in-memory buffer
    table = pa.Table.from_pandas(df)
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer, compression="SNAPPY")
    parquet_buffer.seek(0)
    
    # Upload to Silver bucket
    logger.info(f"Writing Silver Parquet ({len(df)} records) to s3://{SILVER_BUCKET}/{parquet_key}")
    s3_client.put_object(
        Bucket=SILVER_BUCKET,
        Key=parquet_key,
        Body=parquet_buffer.getvalue(),
        ContentType="application/octet-stream"
    )
    
    logger.info(f"Successfully written to Silver: s3://{SILVER_BUCKET}/{parquet_key}")
    return parquet_key


def process_all_bronze_to_silver() -> List[str]:
    """Batch processes all available Bronze JSON files into Silver Parquet files."""
    json_files = list_bronze_json_files()
    if not json_files:
        logger.info("No Bronze files found to process.")
        return []
    
    logger.info(f"Found {len(json_files)} Bronze files to process.")
    silver_keys = []
    for key in json_files:
        silver_key = process_bronze_file_to_silver(key)
        if silver_key:
            silver_keys.append(silver_key)
    return silver_keys


if __name__ == "__main__":
    process_all_bronze_to_silver()
