{{ config(materialized='table') }}

WITH raw_data AS (
    SELECT * FROM {{ source('raw', 'taxi_trips') }}
)

SELECT
    -- 1. Casting Data Types
    vendorid::int AS vendor_id,
    tpep_pickup_datetime::timestamp AS pickup_datetime,
    tpep_dropoff_datetime::timestamp AS dropoff_datetime,
    
    -- 2. Business Logic / Calculations
    passenger_count::int AS passenger_count,
    trip_distance::float AS trip_distance,
    fare_amount::float AS fare_amount,
    tip_amount::float AS tip_amount,
    total_amount::float AS total_amount,
    pulocationid::int AS pickup_location_id

FROM raw_data
WHERE 
    -- Filter invalid data based on business rules
    fare_amount > 0 
    AND trip_distance > 0 
    AND passenger_count IS NOT NULL
    AND passenger_count > 0
    AND tpep_pickup_datetime IS NOT NULL
