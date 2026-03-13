
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select TOP_PRODUCT
from FLUXCART.DEV.dim_products
where TOP_PRODUCT is null



  
  
      
    ) dbt_internal_test