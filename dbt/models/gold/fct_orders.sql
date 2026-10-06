{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert'
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

{% if is_incremental() %}
    -- Incremental filtering: Only process records newer than the most recent timestamp in target table
    where order_timestamp > (select coalesce(max(order_timestamp), '1970-01-01'::timestamptz) from {{ this }})
{% endif %}
