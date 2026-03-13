
  
    

create or replace transient table FLUXCART.DEV.fct_fraud
    
    
    
    as (

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


    )
;


  