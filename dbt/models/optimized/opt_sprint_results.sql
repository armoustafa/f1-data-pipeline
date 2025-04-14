{{
    config(
        materialized='table',
        partition_by={
            "field": "date",
            "data_type": "date",
            "granularity": "year"
        },
        cluster_by=["driverId", "constructorId"]
    )
}}
SELECT
    sr.raceId,
    sr.driverId,
    sr.constructorId,
    sr.number,
    sr.grid,
    sr.position,
    sr.positionText,
    sr.positionOrder,
    sr.points,
    sr.laps,
    sr.time,
    sr.milliseconds,
    sr.fastestLap,
    sr.fastestLapTime,
    CAST(r.date AS DATE) AS date
FROM {{ source('f1_data', 'sprint_results') }} sr
JOIN {{ source('f1_data', 'races') }} r ON sr.raceId = r.raceId
WHERE sr.raceId IS NOT NULL
  AND sr.driverId IS NOT NULL
  AND sr.constructorId IS NOT NULL
  AND r.date IS NOT NULL