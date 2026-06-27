{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY date_key, time_key, location_id)  AS fact_id,
    date_key,
    time_key,
    location_id,
    weather_key,
    temperature,
    humidity,
    precipitation,
    wind_speed,

    -- Rain flag: TRUE jika ada curah hujan
    precipitation > 0                            AS rain_flag,

    -- Irrigation flag: TRUE jika tidak hujan DAN kelembapan rendah
    -- Kondisi ini mengindikasikan tanaman butuh irigasi tambahan
    (precipitation = 0 AND humidity < 60)        AS irrigation_flag

FROM (
    SELECT
        CAST(strftime(CAST(utc_datetime AS DATE), '%Y%m%d') AS INTEGER) AS date_key,
        EXTRACT(hour FROM utc_datetime)::INTEGER                         AS time_key,
        location_id,
        weather_code    AS weather_key,
        temperature,
        humidity,
        precipitation,
        wind_speed
    FROM {{ ref('int_weather_forecast') }}
)