SELECT
product_category_name AS category_name_pt,
product_category_name_english AS category_name_en
FROM {{ ref('SEED_product_category_translation') }}