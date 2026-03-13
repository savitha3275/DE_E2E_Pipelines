{{ config(
    materialized='table'
) }}

SELECT DISTINCT
    USER_ID
FROM {{ ref('fct_fraud') }}
WHERE USER_ID IS NOT NULL