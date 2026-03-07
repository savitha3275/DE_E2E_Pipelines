/*
Fact model for orders.
This model provides fact data for orders from the enriched orders intermediate table.
*/
WITH enriched_orders AS (

SELECT *
FROM {{ ref('int_orders_enriched') }}

)

SELECT *
FROM enriched_orders