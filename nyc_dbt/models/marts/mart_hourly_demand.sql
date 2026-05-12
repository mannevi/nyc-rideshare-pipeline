WITH trips AS (
    SELECT * FROM {{ ref('stg_trips') }}
)

SELECT
    pickup_hour,
    is_weekend,
    company,
    COUNT(*)                        AS total_trips,
    ROUND(AVG(passenger_fare), 2)   AS avg_fare,
    ROUND(AVG(trip_miles), 2)       AS avg_miles
FROM trips
GROUP BY pickup_hour, is_weekend, company
ORDER BY pickup_hour, total_trips DESC
