{{ config(
    materialized='table'
) }}

SELECT DISTINCT
    top_product,
    top_category
FROM {{ ref('fct_analytics') }}

UNION

SELECT DISTINCT
    top_product,
    top_category
FROM {{ ref('fct_inventory') }}