"""
Bronze to Silver Transformation Module with Data Quality Firewall & Incremental State Tracking
Reads ONLY NEW (unprocessed) raw JSON batches from Bronze S3 bucket using a checkpoint metadata file,
flattens nested structures, validates against data quality rules, and writes clean records to Silver
while routing anomalies to quarantine (DLQ).
"""

import io
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Set, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from config.s3_config import BRONZE_BUCKET, SILVER_BUCKET, get_s3_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("SilverTransformation")

VALID_ORDER_STATUSES = {"completed", "cancelled", "returned"}
METADATA_CHECKPOINT_KEY = "_metadata/processed_files.json"


def get_processed_files() -> Set[str]:
    """
    Reads the incremental checkpoint file from Silver bucket to identify
    which Bronze files have already been processed.
    """
    s3_client = get_s3_client()
    try:
        response = s3_client.get_object(Bucket=SILVER_BUCKET, Key=METADATA_CHECKPOINT_KEY)
        data = json.loads(response["Body"].read().decode("utf-8"))
        return set(data.get("processed_files", []))
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code in ("NoSuchKey", "404"):
            logger.info("No checkpoint metadata found. Initializing empty processed file registry.")
            return set()
        logger.error(f"Error fetching checkpoint metadata: {e}")
        return set()


def save_processed_files(processed_files: Set[str]) -> None:
    """Updates the incremental checkpoint file in Silver bucket."""
    s3_client = get_s3_client()
    payload = {
        "last_updated_at": datetime.now(timezone.utc).isoformat(),
        "total_files_processed": len(processed_files),
        "processed_files": sorted(list(processed_files)),
    }
    s3_client.put_object(
        Bucket=SILVER_BUCKET,
        Key=METADATA_CHECKPOINT_KEY,
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"Updated incremental checkpoint with {len(processed_files)} processed files.")


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
        
        customer = order.get("customer") or {}
        customer_id = customer.get("customer_id")
        customer_name = f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip()
        customer_email = customer.get("email")
        customer_city = customer.get("city")
        customer_state = customer.get("state")
        customer_country = customer.get("country")
        
        payment = order.get("payment") or {}
        payment_method = payment.get("payment_method")
        currency = payment.get("currency")
        order_total_amount = float(payment.get("total_amount", 0.0))
        shipping_fee = float(payment.get("shipping_fee", 0.0))
        tax_amount = float(payment.get("tax_amount", 0.0))
        
        items = order.get("items") or []
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
                "customer_name": customer_name if customer_name else None,
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

    df["order_timestamp"] = pd.to_datetime(df["order_timestamp"], utc=True)
    df["quantity"] = df["quantity"].astype("int32")
    df["unit_price"] = df["unit_price"].astype("float64")
    df["line_item_total"] = df["line_item_total"].astype("float64")
    df["order_total_amount"] = df["order_total_amount"].astype("float64")
    df["shipping_fee"] = df["shipping_fee"].astype("float64")
    df["tax_amount"] = df["tax_amount"].astype("float64")
    df["silver_processed_at"] = datetime.now(timezone.utc)
    
    return df


def validate_and_split_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Data Quality Firewall:
    Validates records against business rules and splits into clean_df and quarantine_df.
    """
    if df.empty:
        return df, pd.DataFrame()

    rejection_reasons = []

    for _, row in df.iterrows():
        reasons = []
        if pd.isna(row["customer_id"]) or str(row["customer_id"]).strip() == "":
            reasons.append("MISSING_CUSTOMER_ID")
        if row["order_total_amount"] <= 0:
            reasons.append("NEGATIVE_OR_ZERO_ORDER_TOTAL")
        if row["line_item_total"] <= 0:
            reasons.append("NEGATIVE_OR_ZERO_LINE_ITEM")
        if row["order_status"] not in VALID_ORDER_STATUSES:
            reasons.append(f"INVALID_STATUS_{row['order_status']}")

        rejection_reasons.append("; ".join(reasons) if reasons else None)

    df["rejection_reason"] = rejection_reasons

    clean_df = df[df["rejection_reason"].isna()].copy().drop(columns=["rejection_reason"])
    quarantine_df = df[df["rejection_reason"].notna()].copy()

    return clean_df, quarantine_df


def upload_dataframe_to_parquet(df: pd.DataFrame, bucket: str, s3_key: str):
    """Encodes a DataFrame into Snappy Parquet and uploads to S3."""
    s3_client = get_s3_client()
    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="SNAPPY")
    buffer.seek(0)
    
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )


def process_bronze_file_to_silver(s3_key: str) -> Dict[str, Any]:
    """Processes a single Bronze file and isolates anomalies into quarantine/."""
    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket=BRONZE_BUCKET, Key=s3_key)
    raw_content = response["Body"].read().decode("utf-8")
    orders_data = json.loads(raw_content)

    df = flatten_order_batch(orders_data)
    if df.empty:
        return {"clean_key": None, "quarantine_key": None, "clean_count": 0, "quarantine_count": 0}

    clean_df, quarantine_df = validate_and_split_data(df)

    clean_key = None
    if not clean_df.empty:
        clean_key = s3_key.replace(".json", ".parquet")
        upload_dataframe_to_parquet(clean_df, SILVER_BUCKET, clean_key)
        logger.info(f"Clean records ({len(clean_df)}) written to s3://{SILVER_BUCKET}/{clean_key}")

    quarantine_key = None
    if not quarantine_df.empty:
        quarantine_key = s3_key.replace("orders/", "quarantine/").replace(".json", "_quarantined.parquet")
        upload_dataframe_to_parquet(quarantine_df, SILVER_BUCKET, quarantine_key)
        logger.warning(
            f"QUARANTINE ALERT: {len(quarantine_df)} anomalous records isolated in s3://{SILVER_BUCKET}/{quarantine_key}"
        )

    return {
        "clean_key": clean_key,
        "quarantine_key": quarantine_key,
        "clean_count": len(clean_df),
        "quarantine_count": len(quarantine_df),
    }


def process_all_bronze_to_silver() -> Dict[str, int]:
    """
    Incremental Processing:
    Fetches only NEW/UNPROCESSED Bronze JSON files, transforms them,
    and updates the checkpoint state in Silver bucket.
    """
    all_json_files = set(list_bronze_json_files())
    processed_files = get_processed_files()

    # Calculate delta: only files that haven't been processed yet
    new_files = sorted(list(all_json_files - processed_files))

    if not new_files:
        logger.info("Incremental Check: No new Bronze files to process. Everything is up to date!")
        return {"total_clean": 0, "total_quarantined": 0, "new_files_processed": 0}

    logger.info(f"Incremental Check: Found {len(new_files)} new unprocessed Bronze files (out of {len(all_json_files)} total).")
    
    total_clean = 0
    total_quarantined = 0

    for key in new_files:
        stats = process_bronze_file_to_silver(key)
        total_clean += stats["clean_count"]
        total_quarantined += stats["quarantine_count"]
        processed_files.add(key)

    # Persist updated state checkpoint
    save_processed_files(processed_files)

    logger.info(
        f"Incremental Transformation complete: {len(new_files)} files processed, "
        f"{total_clean} clean records, {total_quarantined} quarantined records."
    )
    return {
        "total_clean": total_clean,
        "total_quarantined": total_quarantined,
        "new_files_processed": len(new_files),
    }


if __name__ == "__main__":
    process_all_bronze_to_silver()
