
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select USER_ID
from FLUXCART.DEV.dim_users
where USER_ID is null



  
  
      
    ) dbt_internal_test