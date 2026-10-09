{{ config(
    materialized='external',
    location='s3://ecommerce-gold/fct_orders.parquet'
) }}

with orders as (
    select * from {{ ref('stg_orders') }}
)

select
    order_id,
    order_status,
    order_timestamp,
    customer_id,
    product_id,
    quantity,
    unit_price,
    line_item_total,
    payment_method,
    shipping_fee,
    tax_amount,
    order_total_amount
from orders
