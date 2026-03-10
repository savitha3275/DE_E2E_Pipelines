/*
Staging model for payments.
This model selects payment data from the raw order payments table.
*/
WITH raw_payments AS (

    SELECT *
    FROM BRAZIL_ECOMMERCE.RAW.RAW_ORDER_PAYMENTS

)

SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value

FROM raw_payments