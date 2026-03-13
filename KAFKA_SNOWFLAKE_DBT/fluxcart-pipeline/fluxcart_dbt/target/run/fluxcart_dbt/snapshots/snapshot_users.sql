
      
  
    

create or replace transient table FLUXCART.SNAPSHOTS.snapshot_users
    
    
    
    as (
    

    select *,
        md5(coalesce(cast(USER_ID as varchar ), '')
         || '|' || coalesce(cast(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as varchar ), '')
        ) as dbt_scd_id,
        to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_updated_at,
        to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_valid_from,
        
  
  coalesce(nullif(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())), to_timestamp_ntz(convert_timezone('UTC', current_timestamp()))), null)
  as dbt_valid_to
from (
        



SELECT *
FROM FLUXCART.DEV.dim_users

    ) sbq



    )
;


  
  