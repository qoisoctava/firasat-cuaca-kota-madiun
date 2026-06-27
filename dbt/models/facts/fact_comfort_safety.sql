{{ config(materialized='table') }}

WITH base AS (
    SELECT
        CAST(strftime(CAST(utc_datetime AS DATE), '%Y%m%d') AS INTEGER) AS date_key,
        EXTRACT(hour FROM utc_datetime)::INTEGER                         AS time_key,
        location_id,
        weather_code,
        temperature,
        humidity,
        wind_speed,
        wind_dir        AS wind_direction

    FROM {{ ref('int_weather_forecast') }}
),

with_heat_index AS (
    SELECT
        *,
        -- Rumus Heat Index (Steadman, versi sederhana)
        -- Menggabungkan suhu dan kelembapan untuk menggambarkan
        -- "feels like temperature"
        ROUND(
            -8.78469475556
            + 1.61139411    * temperature
            + 2.33854883889 * humidity
            - 0.14611605    * temperature * humidity
            - 0.01230809    * temperature * temperature
            - 0.01642482    * humidity    * humidity
            + 0.00221732    * temperature * temperature * humidity
            + 0.00072546    * temperature * humidity    * humidity
            - 0.00000358    * temperature * temperature * humidity * humidity
        , 1)                                AS heat_index

    FROM base
)

SELECT
    ROW_NUMBER() OVER (ORDER BY date_key, time_key, location_id)  AS fact_id,
    date_key,
    time_key,
    location_id,
    weather_code                                                   AS weather_key,
    temperature,
    humidity,
    wind_speed,
    wind_direction,
    heat_index,

    CASE
        WHEN heat_index < 27                THEN 'Nyaman'
        WHEN heat_index >= 27 AND heat_index < 32 THEN 'Panas'
        WHEN heat_index >= 32 AND heat_index < 41 THEN 'Sangat Panas'
        WHEN heat_index >= 41 AND heat_index < 54 THEN 'Berbahaya'
        ELSE                                     'Ekstrem'
    END                                          AS comfort_label

FROM with_heat_index