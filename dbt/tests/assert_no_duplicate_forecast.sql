SELECT
    _batch_date,
    _run_hour,
    location_id,
    utc_datetime,
    COUNT(*) AS row_count
FROM staging.stg_weather_forecast
GROUP BY
    _batch_date,
    _run_hour,
    location_id,
    utc_datetime
HAVING COUNT(*) > 1