/*
Staging model for category translation.
This model selects category translation data from the raw category translation table.
*/
WITH raw_category AS (

    SELECT *
    FROM BRAZIL_ECOMMERCE.RAW.RAW_CATEGORY_TRANSLATION

)

SELECT
    product_category_name,
    product_category_name_english

FROM raw_category