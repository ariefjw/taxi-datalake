{{ config(materialized='table') }}

SELECT
    pickup_location_id,
    date_trunc('hour', pickup_datetime) AS pickup_hour,
    COUNT(*) AS trip_count
FROM {{ ref('stg_nyc_tripdata') }}
GROUP BY 1, 2
ORDER BY pickup_hour DESC, trip_count DESC
