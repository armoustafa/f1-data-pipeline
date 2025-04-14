SELECT
    raceId AS race_id,
    driverId AS driver_id,
    constructorId AS constructor_id,
    stop,
    lap,
    milliseconds AS pit_stop_duration_ms,
    date AS race_date
FROM {{ source('f1_optimized', 'opt_pit_stops') }}
WHERE raceId IS NOT NULL
  AND driverId IS NOT NULL