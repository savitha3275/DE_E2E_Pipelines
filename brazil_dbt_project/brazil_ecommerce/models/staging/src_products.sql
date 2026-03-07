/*
Staging model for products.
This model selects product data from the raw products table.
*/
WITH raw_products AS (

    SELECT *
    FROM BRAZIL_ECOMMERCE.RAW.RAW_PRODUCTS

)

SELECT
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm

FROM raw_products