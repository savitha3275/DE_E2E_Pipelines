

-- staging views for fraud data
-- Source table: raw_fraud_alerts (RAW schema)
SELECT
    timestamp AS alert_time,           
    alert_id,
    rule,
    severity,
    user_id,
    payment_id,
    order_id,
    amount,
    detail
FROM FLUXCART.RAW.raw_fraud

--Only keeps rows with valid alert_id.
--Converts amount to float.