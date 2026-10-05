{{ config(
    materialized='external',
    location='s3://ecommerce-gold/dim_customers.parquet'
) }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers_aggregated as (
    select
        customer_id,
        customer_name,
        customer_email,
        customer_city,
        customer_state,
        customer_country,
        min(order_timestamp) as first_order_at,
        max(order_timestamp) as last_order_at,
        count(distinct order_id) as total_orders_placed,
        round(sum(line_item_total), 2) as lifetime_spend
    from orders
    group by
        customer_id,
        customer_name,
        customer_email,
        customer_city,
        customer_state,
        customer_country
)

select * from customers_aggregated
