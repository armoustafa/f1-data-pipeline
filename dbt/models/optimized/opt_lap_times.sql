{{
    config(
        materialized='table',
        partition_by={
            "field": "date",
            "data_type": "date",
            "granularity": "year"
        },
        cluster_by=["driverId", "raceId"]
    )
}}
SELECT
    lt.raceId,
    lt.driverId,
    lt.lap,
    lt.position,
    lt.time,
    lt.milliseconds,
    CAST(r.date AS DATE) AS date
FROM {{ source('f1_data', 'lap_times') }} lt
JOIN {{ source('f1_data', 'races') }} r ON lt.raceId = r.raceId
WHERE lt.raceId IS NOT NULL
  AND lt.driverId IS NOT NULL