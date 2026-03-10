/*
Intermediate model for enriched orders.
This ephemeral model joins orders and payments to provide enriched order data including payment values.
*/
{{ config(materialized='ephemeral') }}

WITH orders AS (

SELECT *
FROM {{ ref('src_orders') }}

),

payments AS (

SELECT *
FROM {{ ref('src_payments') }}

)

SELECT
o.order_id,
o.customer_id,
o.order_purchase_timestamp,
p.payment_value

FROM orders o
LEFT JOIN payments p
ON o.order_id = p.order_id