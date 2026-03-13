{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='alert_id'
) }}

WITH cleaned AS (
    SELECT
        alert_id,
        alert_time,
        rule,
        severity,
        user_id,
        payment_id,
        order_id,
        amount,
        detail
    FROM {{ ref('stg_fraud') }}
)

SELECT *
FROM cleaned

{% if is_incremental() %}
WHERE alert_time > (SELECT MAX(alert_time) FROM {{ this }})
{% endif %}