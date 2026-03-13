
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ALERT_ID
from FLUXCART.DEV.fct_fraud
where ALERT_ID is null



  
  
      
    ) dbt_internal_test