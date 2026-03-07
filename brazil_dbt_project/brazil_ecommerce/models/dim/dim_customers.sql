/*
Dimension model for customers.
This model provides customer dimension data from the staging customers table.
*/
WITH customers AS (

    SELECT * FROM {{ ref('src_customers') }}

)

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state

FROM customers