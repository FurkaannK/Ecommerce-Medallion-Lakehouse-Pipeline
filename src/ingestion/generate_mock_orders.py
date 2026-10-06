"""
E-Commerce Order Generator & Bronze Ingestion Module
Generates synthetic nested order transactions with controlled data anomalies (dirty data)
and ingests raw JSON batches into MinIO Bronze Layer.
"""

import json
import logging
import random
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

from faker import Faker

from config.s3_config import BRONZE_BUCKET, get_s3_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("BronzeIngestion")

fake = Faker()

PRODUCT_CATALOG = [
    {"product_id": "prod_101", "name": "Wireless Noise-Canceling Headphones", "category": "Electronics", "unit_price": 149.99},
    {"product_id": "prod_102", "name": "Mechanical Gaming Keyboard", "category": "Electronics", "unit_price": 89.50},
    {"product_id": "prod_103", "name": "Ultra-Wide Gaming Monitor 27-inch", "category": "Electronics", "unit_price": 329.00},
    {"product_id": "prod_201", "name": "Men's Waterproof Trail Running Shoes", "category": "Apparel & Footwear", "unit_price": 120.00},
    {"product_id": "prod_202", "name": "Breathable Performance T-Shirt", "category": "Apparel & Footwear", "unit_price": 34.99},
    {"product_id": "prod_203", "name": "Fleece Zip Hoodie", "category": "Apparel & Footwear", "unit_price": 59.90},
    {"product_id": "prod_301", "name": "Insulated Stainless Steel Water Bottle", "category": "Home & Kitchen", "unit_price": 24.95},
    {"product_id": "prod_302", "name": "Ceramic Pour-Over Coffee Maker", "category": "Home & Kitchen", "unit_price": 42.00},
    {"product_id": "prod_401", "name": "Organic Bamboo Bed Sheet Set", "category": "Home Goods", "unit_price": 95.00},
    {"product_id": "prod_501", "name": "USB-C Fast Charging Cable (2m)", "category": "Accessories", "unit_price": 19.99},
]

VALID_PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "crypto"]
VALID_ORDER_STATUSES = ["completed", "completed", "completed", "cancelled", "returned"]


def generate_single_order() -> Dict[str, Any]:
    """
    Generates a realistic nested e-commerce order payload with occasional
    intentional anomalies for data quality & quarantine verification.
    """
    order_id = f"ord_{uuid.uuid4().hex[:10]}"
    customer_id = f"cust_{fake.random_int(min=1000, max=9999)}"
    
    # 1 to 3 items per order
    num_items = random.randint(1, 3)
    selected_products = random.sample(PRODUCT_CATALOG, k=num_items)
    
    items = []
    subtotal = 0.0
    for prod in selected_products:
        quantity = random.randint(1, 2)
        item_total = round(prod["unit_price"] * quantity, 2)
        subtotal += item_total
        items.append({
            "product_id": prod["product_id"],
            "product_name": prod["name"],
            "category": prod["category"],
            "unit_price": prod["unit_price"],
            "quantity": quantity,
        })
    
    subtotal = round(subtotal, 2)
    tax_amount = round(subtotal * 0.08, 2)
    shipping_fee = 0.0 if subtotal > 100 else 5.99
    total_amount = round(subtotal + tax_amount + shipping_fee, 2)
    order_status = random.choice(VALID_ORDER_STATUSES)
    
    # ---- CONTROLLED DATA ANOMALY INJECTION (~8% rate) ----
    anomaly_dice = random.random()
    if anomaly_dice < 0.03:
        # Anomaly 1: Negative / Corrupted financial transaction
        total_amount = -round(random.uniform(10.0, 99.0), 2)
    elif anomaly_dice < 0.06:
        # Anomaly 2: Missing customer identity (Null Foreign Key)
        customer_id = None
    elif anomaly_dice < 0.08:
        # Anomaly 3: Invalid domain status string
        order_status = "UNKNOWN_OR_SUSPICIOUS"

    now = datetime.now(timezone.utc)
    
    order = {
        "order_id": order_id,
        "customer": {
            "customer_id": customer_id,
            "first_name": fake.first_name() if customer_id else None,
            "last_name": fake.last_name() if customer_id else None,
            "email": fake.ascii_email() if customer_id else None,
            "city": fake.city() if customer_id else None,
            "state": fake.state_abbr() if customer_id else None,
            "country": "USA",
        },
        "items": items,
        "payment": {
            "payment_method": random.choice(VALID_PAYMENT_METHODS),
            "currency": "USD",
            "subtotal": subtotal,
            "tax_amount": tax_amount,
            "shipping_fee": shipping_fee,
            "total_amount": total_amount,
        },
        "order_status": order_status,
        "created_at": now.isoformat(),
    }
    return order


def generate_order_batch(batch_size: int = 50) -> List[Dict[str, Any]]:
    """Generates a batch of orders."""
    return [generate_single_order() for _ in range(batch_size)]


def ingest_batch_to_bronze(batch_size: int = 50) -> str:
    """
    Generates an order batch and uploads it to MinIO Bronze bucket
    with hive-partitioned key naming: orders/year=YYYY/month=MM/day=DD/orders_timestamp.json
    """
    orders = generate_order_batch(batch_size=batch_size)
    now = datetime.now(timezone.utc)
    
    partition_path = f"year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}"
    file_name = f"orders_{now.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}.json"
    s3_key = f"orders/{partition_path}/{file_name}"
    
    json_payload = json.dumps(orders, indent=2)
    
    s3_client = get_s3_client()
    logger.info(f"Ingesting batch of {len(orders)} orders into s3://{BRONZE_BUCKET}/{s3_key}")
    
    s3_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=s3_key,
        Body=json_payload.encode("utf-8"),
        ContentType="application/json"
    )
    
    logger.info("Successfully ingested order batch to Bronze layer!")
    return s3_key


if __name__ == "__main__":
    ingest_batch_to_bronze(batch_size=30)
