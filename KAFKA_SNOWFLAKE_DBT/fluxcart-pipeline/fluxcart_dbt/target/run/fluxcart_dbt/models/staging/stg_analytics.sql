
  create or replace   view FLUXCART.DEV.stg_analytics
  
  
  
  
  as (
    

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
FROM FLUXCART.RAW.raw_analytics
  );

