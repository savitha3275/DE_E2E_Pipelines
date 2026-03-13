
  
    

create or replace transient table FLUXCART.DEV.dim_users
    
    
    
    as (

SELECT DISTINCT
    USER_ID
FROM FLUXCART.DEV.fct_fraud
WHERE USER_ID IS NOT NULL
    )
;


  