/*
Dimension model for orders.
This model provides order dimension data from the staging orders table.
*/
WITH orders AS (

    SELECT * FROM {{ ref('src_orders') }}

)

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_estimated_delivery_date

FROM orders