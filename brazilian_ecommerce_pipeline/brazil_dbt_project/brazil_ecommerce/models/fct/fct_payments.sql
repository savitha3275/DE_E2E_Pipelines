/*
Fact model for payments.
This model provides fact data for payments from the staging payments table.
*/
WITH payments AS (

    SELECT * FROM {{ ref('src_payments') }}

)

SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value

FROM payments