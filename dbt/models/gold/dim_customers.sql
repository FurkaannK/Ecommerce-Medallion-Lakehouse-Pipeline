{{ config(
    materialized='external',
    location='s3://ecommerce-gold/dim_customers.parquet'
) }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

-- Aggregate customer lifetime metrics
customer_metrics as (
    select
        customer_id,
        min(order_timestamp) as first_order_at,
        max(order_timestamp) as last_order_at,
        count(distinct order_id) as total_orders_placed,
        round(sum(line_item_total), 2) as lifetime_spend
    from orders
    group by customer_id
),

-- Get the latest customer profile info (deduplicate customer attributes by most recent order)
latest_customer_profile as (
    select
        customer_id,
        customer_name,
        customer_email,
        customer_city,
        customer_state,
        customer_country,
        row_number() over (partition by customer_id order by order_timestamp desc) as rn
    from orders
)

select
    m.customer_id,
    p.customer_name,
    p.customer_email,
    p.customer_city,
    p.customer_state,
    p.customer_country,
    m.first_order_at,
    m.last_order_at,
    m.total_orders_placed,
    m.lifetime_spend
from customer_metrics m
join latest_customer_profile p
    on m.customer_id = p.customer_id
    and p.rn = 1
