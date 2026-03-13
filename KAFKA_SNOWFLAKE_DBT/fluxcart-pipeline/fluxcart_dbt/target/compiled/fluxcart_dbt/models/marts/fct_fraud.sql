

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
    FROM FLUXCART.DEV.stg_fraud
)

SELECT *
FROM cleaned


WHERE alert_time > (SELECT MAX(alert_time) FROM FLUXCART.DEV.fct_fraud)
