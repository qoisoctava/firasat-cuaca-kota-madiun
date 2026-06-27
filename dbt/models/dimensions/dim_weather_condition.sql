{{ config(materialized='table') }}

WITH unique_conditions AS (
    SELECT DISTINCT
        weather_code,
        weather_desc,
        weather_desc_en
    FROM {{ ref('int_weather_forecast') }}
    WHERE weather_code IS NOT NULL
)

SELECT
    ROW_NUMBER() OVER (ORDER BY weather_code)   AS weather_key,
    weather_code,
    weather_desc,
    weather_desc_en
FROM unique_conditions
ORDER BY weather_code