WITH trips AS (
    SELECT * FROM {{ ref('stg_trips') }}
)

SELECT
    weather_label,
    is_bad_weather,
    temp_category,
    COUNT(*)                            AS total_trips,
    ROUND(AVG(passenger_fare), 2)       AS avg_fare,
    ROUND(AVG(tips), 2)                 AS avg_tips,
    ROUND(AVG(trip_miles), 2)           AS avg_miles,
    ROUND(
        SUM(tips) / NULLIF(SUM(passenger_fare), 0) * 100,
        2
    )                                   AS tip_percentage
FROM trips
GROUP BY weather_label, is_bad_weather, temp_category
ORDER BY total_trips DESC
