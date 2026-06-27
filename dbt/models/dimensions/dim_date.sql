{{ config(materialized='table') }}

WITH date_spine AS (
    SELECT DISTINCT
        CAST(utc_datetime AS DATE) AS date
    FROM {{ ref('int_weather_forecast') }}
)

SELECT
    CAST(strftime(date, '%Y%m%d') AS INTEGER)   AS date_key,
    date,
    EXTRACT(year    FROM date)::INTEGER          AS year,
    EXTRACT(month   FROM date)::INTEGER          AS month,
    EXTRACT(day     FROM date)::INTEGER          AS day,
    EXTRACT(isodow  FROM date)::INTEGER - 1      AS day_of_week,
    strftime(date, '%A')    AS day_name,     -- contoh: 'Monday'
    strftime(date, '%B')    AS month_name,  -- contoh: 'January'
    EXTRACT(quarter FROM date)::INTEGER          AS quarter,
    EXTRACT(isodow FROM date)::INTEGER >= 6      AS is_weekend

FROM date_spine