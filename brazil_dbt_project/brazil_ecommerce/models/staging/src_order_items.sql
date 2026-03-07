/*
Staging model for order items.
This model selects order item data from the raw order items table.
*/
WITH raw_order_items AS (

    SELECT *
    FROM BRAZIL_ECOMMERCE.RAW.RAW_ORDER_ITEMS

)

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value

FROM raw_order_items