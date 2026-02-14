{{ config(materialized='table') }}

SELECT
    pickup_datetime::date AS trip_date,
    SUM(total_amount) / NULLIF(SUM(trip_distance), 0) AS avg_revenue_per_mile,
    AVG(tip_amount / NULLIF(fare_amount, 0)) * 100 AS avg_tip_percentage
FROM {{ ref('stg_nyc_tripdata') }}
GROUP BY 1
ORDER BY 1 DESC
