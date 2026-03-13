{{ config(
    materialized='incremental',           
    incremental_strategy='merge',     
    unique_key='event_time'              
) }}
---- incremental table
---- merge new rows
-- unique per row

WITH cleaned AS (
    SELECT
        event_time,
        behavior_events,
        top_action,
        top_product,
        top_category,
        order_events,
        avg_order_value,
        total_revenue,
        orders_placed,
        orders_delivered,
        orders_cancelled
    FROM {{ ref('stg_analytics') }}       -- staging view
)

SELECT *
FROM cleaned

{% if is_incremental() %}
WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})
{% endif %}