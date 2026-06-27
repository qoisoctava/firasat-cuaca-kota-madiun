{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY date_key, time_key, location_id)  AS fact_id,
    date_key,
    time_key,
    location_id,
    weather_key,
    temperature,
    cloud_cover,
    wind_speed,
    ROUND(visibility_m / 1000.0, 3)                               AS visibility_km,

    -- Proxy potensi solar berdasarkan tutupan awan (tcc)
    -- Semakin sedikit awan → potensi solar semakin tinggi
    CASE
        WHEN cloud_cover < 25               THEN 'Tinggi'
        WHEN cloud_cover >= 25 AND cloud_cover < 60 THEN 'Sedang'
        ELSE                                     'Rendah'
    END                                          AS solar_proxy,

    -- Proxy potensi angin berdasarkan kecepatan angin (ws)
    -- Turbin angin umumnya efektif di atas 15 km/jam
    CASE
        WHEN wind_speed >= 30               THEN 'Kencang'
        WHEN wind_speed >= 15 AND wind_speed < 30 THEN 'Sedang'
        ELSE                                     'Lemah'
    END                                          AS wind_proxy

FROM (
    SELECT
        CAST(strftime(CAST(utc_datetime AS DATE), '%Y%m%d') AS INTEGER) AS date_key,
        EXTRACT(hour FROM utc_datetime)::INTEGER                         AS time_key,
        location_id,
        weather_code    AS weather_key,
        temperature,
        cloud_cover,
        wind_speed,
        visibility_m
    FROM {{ ref('int_weather_forecast') }}
)