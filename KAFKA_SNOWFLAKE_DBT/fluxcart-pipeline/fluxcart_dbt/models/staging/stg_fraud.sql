-- models/staging/stg_fraud.sql
WITH dedup AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY ALERT_ID  -- your unique key for fct_fraud
                   ORDER BY TIMESTAMP DESC  -- latest record first
               ) AS rn
        FROM {{ source('raw', 'raw_fraud') }}
    )
    WHERE rn = 1
)

SELECT
    ALERT_ID,
    USER_ID,
    PAYMENT_ID,
    STATUS,
    AMOUNT,
    TIMESTAMP,
    ALERT_TYPE
FROM dedup
