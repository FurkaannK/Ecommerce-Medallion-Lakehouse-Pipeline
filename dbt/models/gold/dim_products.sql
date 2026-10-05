{{ config(
    materialized='external',
    location='s3://ecommerce-gold/dim_products.parquet'
) }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

products_aggregated as (
    select
        product_id,
        product_name,
        product_category,
        round(avg(unit_price), 2) as current_unit_price,
        sum(quantity) as total_units_sold,
        round(sum(line_item_total), 2) as total_revenue_generated
    from orders
    group by
        product_id,
        product_name,
        product_category
)

select * from products_aggregated
