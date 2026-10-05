{{ config(
    materialized='external',
    location='s3://ecommerce-gold/mart_daily_sales.parquet'
) }}

with orders as (
    select * from {{ ref('stg_orders') }}
)

select
    cast(order_timestamp as date) as order_date,
    count(distinct order_id) as total_orders,
    count(distinct customer_id) as unique_customers,
    sum(quantity) as total_items_sold,
    round(sum(line_item_total), 2) as gross_sales,
    round(avg(line_item_total), 2) as avg_order_item_value,
    count(case when order_status = 'completed' then 1 end) as completed_orders,
    count(case when order_status = 'cancelled' then 1 end) as cancelled_orders,
    count(case when order_status = 'returned' then 1 end) as returned_orders
from orders
group by
    cast(order_timestamp as date)
