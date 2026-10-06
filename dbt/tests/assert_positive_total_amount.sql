-- Singular test: Assert that line_item_total is strictly greater than 0
-- If this query returns ANY rows, the test fails.

select
    order_id,
    product_id,
    line_item_total
from {{ ref('fct_orders') }}
where line_item_total <= 0
