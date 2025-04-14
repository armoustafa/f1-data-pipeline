SELECT
    resultId AS result_id,
    raceId AS race_id,
    driverId AS driver_id,
    constructorId AS constructor_id,
    number,
    grid,
    position,
    positionText AS position_text,
    positionOrder AS position_order,
    points,
    laps,
    time,
    milliseconds,
    fastestLap AS fastest_lap,
    rank,
    fastestLapTime AS fastest_lap_time,
    fastestLapSpeed AS fastest_lap_speed,
    statusId AS status_id,
    date AS race_date
FROM {{ source('f1_optimized', 'opt_results') }}
WHERE resultId IS NOT NULL