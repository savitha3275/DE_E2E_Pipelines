/*
Dimension model for products.
This model provides product dimension data, including translated category names in English.
*/
WITH products AS (

    SELECT * FROM {{ ref('src_products') }}

),

category AS (

    SELECT * FROM {{ ref('src_category_translation') }}

)

SELECT
    p.product_id,
    c.product_category_name_english,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm

FROM products p
LEFT JOIN category c
ON p.product_category_name = c.product_category_name