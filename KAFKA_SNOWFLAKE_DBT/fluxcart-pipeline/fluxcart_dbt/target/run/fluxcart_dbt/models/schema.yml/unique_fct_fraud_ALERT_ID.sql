
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    ALERT_ID as unique_field,
    count(*) as n_records

from FLUXCART.DEV.fct_fraud
where ALERT_ID is not null
group by ALERT_ID
having count(*) > 1



  
  
      
    ) dbt_internal_test