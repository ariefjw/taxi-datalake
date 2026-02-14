{{ config(materialized='table') }}

WITH trips AS (
    SELECT 
        DATE_TRUNC('hour', pickup_datetime) AS timestamp,
        COUNT(*) AS total_trips,
        SUM(total_amount) AS total_revenue,
        AVG(trip_distance) AS avg_distance
    FROM {{ ref('stg_nyc_tripdata') }}
    GROUP BY 1
),

weather AS (
    SELECT * FROM {{ ref('stg_weather') }}
)

SELECT
    t.timestamp,
    t.total_trips,
    t.total_revenue,
    t.avg_distance,
    w.temp_celsius,
    w.precipitation_mm,
    w.snowfall_cm,
    CASE 
        WHEN w.precipitation_mm > 0.5 THEN 'Rainy'
        WHEN w.snowfall_cm > 0 THEN 'Snowy'
        ELSE 'Clear'
    END AS weather_condition
FROM trips t
JOIN weather w ON t.timestamp = w.timestamp
