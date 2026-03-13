{{ 
    config(
        materialized='view'  
    ) 
}}
-- staging views for inventory
-- Source table: raw_inventory (RAW schema)
SELECT
    timestamp AS inventory_time,
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
FROM {{ source('raw', 'raw_inventory') }}

-- Filters out rows with zero order_events.
-- Standardizes numeric types for proper aggregation in marts.