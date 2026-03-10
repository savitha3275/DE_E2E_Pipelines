/*
Staging model for sellers.
This model selects seller data from the raw sellers table.
*/
WITH raw_sellers AS (

    SELECT *
    FROM BRAZIL_ECOMMERCE.RAW.RAW_SELLERS

)

SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state

FROM raw_sellers