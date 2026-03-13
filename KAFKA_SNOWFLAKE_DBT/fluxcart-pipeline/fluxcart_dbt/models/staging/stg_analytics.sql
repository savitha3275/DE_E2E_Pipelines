{{ 
    config(
        materialized='view'  
    ) 
}}

-- staging views: small, read-only, transformed from raw data
-- Source table: raw_analytics (RAW schema)
-- Already TIMESTAMP_NTZ, no TRY_CAST needed
SELECT
    timestamp AS event_time,          
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
FROM {{ source('raw', 'raw_analytics') }}

--Removes rows with behavior_events = 0.
--materialized='view' is okay here; use ephemeral if you only join it once in another model.