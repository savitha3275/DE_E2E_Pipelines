
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
    FROM FLUXCART.DEV.stg_analytics       -- staging view
)

SELECT *
FROM cleaned


WHERE event_time > (SELECT MAX(event_time) FROM FLUXCART.DEV.fct_analytics)
