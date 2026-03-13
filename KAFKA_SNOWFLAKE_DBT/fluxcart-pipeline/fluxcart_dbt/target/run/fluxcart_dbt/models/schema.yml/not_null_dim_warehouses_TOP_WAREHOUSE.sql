
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select TOP_WAREHOUSE
from FLUXCART.DEV.dim_warehouses
where TOP_WAREHOUSE is null



  
  
      
    ) dbt_internal_test