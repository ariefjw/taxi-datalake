{{ config(materialized='table') }}

SELECT
    pickup_datetime::date AS trip_date,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS daily_revenue
FROM {{ ref('stg_nyc_tripdata') }} 
GROUP BY 1
ORDER BY 1 DESC
