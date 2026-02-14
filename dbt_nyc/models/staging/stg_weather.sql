{{ config(materialized='table') }}

WITH raw_data AS (
    SELECT * FROM {{ source('raw', 'weather_hourly') }}
)

SELECT
    timestamp::timestamp AS timestamp,
    date(timestamp::timestamp) as date,
    extract(hour from timestamp::timestamp) as hour,
    temp_c::float AS temp_celsius,
    precipitation_mm::float AS precipitation_mm,
    snowfall_cm::float AS snowfall_cm
FROM raw_data
WHERE timestamp IS NOT NULL
