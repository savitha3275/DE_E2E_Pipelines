
    
    

select
    ALERT_ID as unique_field,
    count(*) as n_records

from FLUXCART.DEV.fct_fraud
where ALERT_ID is not null
group by ALERT_ID
having count(*) > 1


