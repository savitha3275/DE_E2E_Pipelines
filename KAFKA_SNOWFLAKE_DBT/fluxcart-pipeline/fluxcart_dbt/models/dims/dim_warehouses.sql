{{ config(
    materialized='table'
) }}

SELECT DISTINCT
    top_warehouse
FROM {{ ref('fct_inventory') }}