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
    q.qualifyId,
    q.raceId,
    q.driverId,
    q.constructorId,
    q.number,
    q.position,
    q.q1,
    q.q2,
    q.q3,
    CAST(r.date AS DATE) AS date
FROM {{ source('f1_data', 'qualifying') }} q
JOIN {{ source('f1_data', 'races') }} r ON q.raceId = r.raceId
WHERE q.raceId IS NOT NULL
  AND q.driverId IS NOT NULL
  AND q.constructorId IS NOT NULL