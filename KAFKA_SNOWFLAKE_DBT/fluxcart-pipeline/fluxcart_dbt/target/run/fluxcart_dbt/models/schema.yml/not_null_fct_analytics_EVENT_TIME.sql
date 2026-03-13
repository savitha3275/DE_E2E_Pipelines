
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select EVENT_TIME
from FLUXCART.DEV.fct_analytics
where EVENT_TIME is null



  
  
      
    ) dbt_internal_test