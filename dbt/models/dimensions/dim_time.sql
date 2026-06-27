{{ config(materialized='table') }}

WITH hours AS (
    -- Generate 24 jam menggunakan range DuckDB
    SELECT unnest(range(0, 24)) AS hour
)

SELECT
    hour                                AS time_key,
    hour,
    CASE
        WHEN hour >= 5  AND hour < 12 THEN 'Pagi'
        WHEN hour >= 12 AND hour < 15 THEN 'Siang'
        WHEN hour >= 15 AND hour < 18 THEN 'Sore'
        ELSE                               'Malam'
    END                                 AS time_of_day

FROM hours
ORDER BY hour