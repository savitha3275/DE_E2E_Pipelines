
  create or replace   view FLUXCART.DEV.stg_inventory
  
  
  
  
  as (
    
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
FROM FLUXCART.RAW.raw_inventory
  );

