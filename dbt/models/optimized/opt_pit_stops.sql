{{
    config(
        materialized='table',
        partition_by={
            "field": "date",
            "data_type": "date",
            "granularity": "year"
        },
        cluster_by=["constructorId"]
    )
}}
SELECT
    ps.raceId,
    ps.driverId,
    ps.stop,
    ps.lap,
    ps.time,
    ps.duration,
    ps.milliseconds,
    CAST(r.date AS DATE) AS date,
    res.constructorId
FROM {{ source('f1_data', 'pit_stops') }} ps
JOIN {{ source('f1_data', 'races') }} r ON ps.raceId = r.raceId
JOIN {{ source('f1_data', 'results') }} res ON ps.raceId = res.raceId AND ps.driverId = res.driverId
WHERE ps.raceId IS NOT NULL
  AND ps.driverId IS NOT NULL
  AND res.constructorId IS NOT NULL