with source as (
    select * from {{ source('silver', 'orders') }}
),

renamed_and_cleaned as (
    select
        order_id,
        order_status,
        order_timestamp,
        customer_id,
        customer_name,
        customer_email,
        customer_city,
        customer_state,
        customer_country,
        product_id,
        product_name,
        product_category,
        unit_price,
        quantity,
        line_item_total,
        payment_method,
        currency,
        order_total_amount,
        shipping_fee,
        tax_amount,
        silver_processed_at
    from source
)

select * from renamed_and_cleaned
