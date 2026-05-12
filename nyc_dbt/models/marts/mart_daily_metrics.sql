WITH trips AS (
    SELECT * FROM {{ ref('stg_trips') }}
)

SELECT
    pickup_date,
    company,
    weather_label,
    COUNT(*)                            AS total_trips,
    ROUND(SUM(passenger_fare), 2)       AS total_revenue,
    ROUND(AVG(passenger_fare), 2)       AS avg_fare,
    ROUND(AVG(trip_miles), 2)           AS avg_miles,
    ROUND(AVG(trip_minutes), 2)         AS avg_duration_mins,
    ROUND(SUM(tips), 2)                 AS total_tips,
    ROUND(AVG(temperature_f), 1)        AS avg_temp_f
FROM trips
GROUP BY pickup_date, company, weather_label
ORDER BY pickup_date, total_trips DESC
