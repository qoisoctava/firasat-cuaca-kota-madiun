{{ config(materialized='view') }}

SELECT
    stg_id,
    location_id,
    utc_datetime,
    local_datetime,
    t               AS temperature,
    hu              AS humidity,
    weather_code,
    weather_desc,
    weather_desc_en,
    ws              AS wind_speed,
    wd              AS wind_dir,
    wd_deg          AS wind_dir_deg,
    wd_to           AS wind_dir_to,
    tcc             AS cloud_cover,
    vs              AS visibility_m,
    vs_text         AS visibility_text,
    tp              AS precipitation,
    analysis_date,
    time_index,
    image_url,
    _run_hour,
    _batch_date,
    _ingested_at

FROM staging.stg_weather_forecast