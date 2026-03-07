/*
Staging model for geolocation.
This model selects geolocation data from the raw geolocation table.
*/
WITH raw_geo AS (

    SELECT *
    FROM BRAZIL_ECOMMERCE.RAW.RAW_GEOLOCATION

)

SELECT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state

FROM raw_geo