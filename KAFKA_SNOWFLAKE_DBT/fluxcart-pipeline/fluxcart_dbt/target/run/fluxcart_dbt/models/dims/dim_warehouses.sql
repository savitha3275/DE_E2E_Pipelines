
  
    

create or replace transient table FLUXCART.DEV.dim_warehouses
    
    
    
    as (

SELECT DISTINCT
    top_warehouse
FROM FLUXCART.DEV.fct_inventory
    )
;


  