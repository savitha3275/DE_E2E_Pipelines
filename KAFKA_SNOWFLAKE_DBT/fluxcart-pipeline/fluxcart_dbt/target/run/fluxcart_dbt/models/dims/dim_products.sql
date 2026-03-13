
  
    

create or replace transient table FLUXCART.DEV.dim_products
    
    
    
    as (

SELECT DISTINCT
    top_product,
    top_category
FROM FLUXCART.DEV.fct_analytics

UNION

SELECT DISTINCT
    top_product,
    top_category
FROM FLUXCART.DEV.fct_inventory
    )
;


  