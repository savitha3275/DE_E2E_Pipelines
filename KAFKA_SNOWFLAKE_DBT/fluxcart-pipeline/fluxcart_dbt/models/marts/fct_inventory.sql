{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_events'
) }}

WITH cleaned AS (
    SELECT
        inventory_time,
        order_events,
        units_ordered,
        units_delivered,
        units_cancelled,
        fulfillment_rate,
        top_warehouse,
        top_product,
        top_category,
        orders_placed,
        orders_delivered,
        orders_cancelled
    FROM {{ ref('stg_inventory') }}
)

SELECT *
FROM cleaned

{% if is_incremental() %}
WHERE inventory_time > (SELECT MAX(inventory_time) FROM {{ this }})
{% endif %}