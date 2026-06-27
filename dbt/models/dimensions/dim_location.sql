{{ config(materialized='table') }}

SELECT
    location_id,
    location_name,
    kecamatan,
    city,
    province,
    latitude,
    longitude,
    timezone,
    is_coastal
FROM gold.dim_location