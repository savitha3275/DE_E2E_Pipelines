

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
    FROM FLUXCART.DEV.stg_inventory
)

SELECT *
FROM cleaned


WHERE inventory_time > (SELECT MAX(inventory_time) FROM FLUXCART.DEV.fct_inventory)
