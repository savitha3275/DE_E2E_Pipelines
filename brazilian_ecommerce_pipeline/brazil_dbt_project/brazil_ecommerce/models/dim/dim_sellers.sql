/*
Dimension model for sellers.
This model provides seller dimension data from the staging sellers table.
*/
WITH sellers AS (

    SELECT * FROM {{ ref('src_sellers') }}

)

SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state

FROM sellers