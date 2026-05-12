WITH source AS (
    SELECT * FROM {{ source('raw', 'trips_with_weather') }}
),

cleaned AS (
    SELECT
        pickup_date,
        pickup_hour,
        day_of_week,
        is_weekend,
        company,
        pulocationid                            AS pickup_zone_id,
        dolocationid                            AS dropoff_zone_id,
        ROUND(trip_miles, 2)                    AS trip_miles,
        ROUND(trip_minutes, 2)                  AS trip_minutes,
        ROUND(base_passenger_fare, 2)           AS passenger_fare,
        ROUND(driver_pay, 2)                    AS driver_pay,
        ROUND(tips, 2)                          AS tips,
        ROUND(congestion_surcharge, 2)          AS congestion_surcharge,
        ROUND(cbd_congestion_fee, 2)            AS cbd_congestion_fee,
        ROUND(temperature_c, 1)                 AS temperature_c,
        ROUND((temperature_c * 9/5) + 32, 1)   AS temperature_f,
        precipitation,
        windspeed_kmh,
        weather_label,
        CASE
            WHEN temperature_c <= 0  THEN 'Freezing'
            WHEN temperature_c <= 10 THEN 'Cold'
            WHEN temperature_c <= 20 THEN 'Mild'
            ELSE 'Warm'
        END AS temp_category,
        ROUND(base_passenger_fare + tips, 2) AS total_passenger_payment,
        CASE
            WHEN weather_label IN ('Rainy', 'Snowy', 'Thunderstorm')
            THEN TRUE ELSE FALSE
        END AS is_bad_weather
    FROM source
    WHERE
        trip_miles > 0
        AND trip_minutes > 0
        AND base_passenger_fare > 0
)

SELECT * FROM cleaned
