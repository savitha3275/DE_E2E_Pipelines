/*
Staging model for orders.
This model selects order data from the raw orders table.
*/
WITH raw_orders AS (

    SELECT *
    FROM BRAZIL_ECOMMERCE.RAW.RAW_ORDERS

)

SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date

FROM raw_orders