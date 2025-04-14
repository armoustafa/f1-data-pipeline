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
    r.resultId,
    r.raceId,
    r.driverId,
    r.constructorId,
    r.number,
    r.grid,
    r.position,
    r.positionText,
    r.positionOrder,
    r.points,
    r.laps,
    r.time,
    r.milliseconds,
    r.fastestLap,
    r.rank,
    r.fastestLapTime,
    r.fastestLapSpeed,
    r.statusId,
    CAST(races.date AS DATE) AS date
FROM {{ source('f1_data', 'results') }} r
JOIN {{ source('f1_data', 'races') }} races ON r.raceId = races.raceId
WHERE r.raceId IS NOT NULL
  AND r.driverId IS NOT NULL
  AND r.constructorId IS NOT NULL