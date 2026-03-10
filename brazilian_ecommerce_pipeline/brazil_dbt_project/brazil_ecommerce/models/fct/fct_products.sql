SELECT
p.product_id,
p.product_category_name,
c.product_category_name_english AS category_name_en
FROM {{ ref('src_products') }} p
LEFT JOIN {{ ref('SEED_product_category_translation') }} c
ON p.product_category_name = c.product_category_name