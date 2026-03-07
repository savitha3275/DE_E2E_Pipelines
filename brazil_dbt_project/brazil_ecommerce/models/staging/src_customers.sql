/*
Staging model for customers.
This model selects customer data from the raw customers table.
*/
WITH raw_customers AS (

    SELECT *
    FROM BRAZIL_ECOMMERCE.RAW.RAW_CUSTOMERS

)

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state

FROM raw_customers