
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select INVENTORY_TIME
from FLUXCART.DEV.fct_inventory
where INVENTORY_TIME is null



  
  
      
    ) dbt_internal_test