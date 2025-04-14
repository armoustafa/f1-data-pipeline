{{
    config(
        materialized='table',
        partition_by={
            "field": "date",
            "data_type": "date",
            "granularity": "year"
        },
        cluster_by=["driverId"]
    )
}}
SELECT
    ds.driverStandingsId,
    ds.raceId,
    ds.driverId,
    ds.points,
    ds.position,
    ds.positionText,
    ds.wins,
    CAST(r.date AS DATE) AS date
FROM {{ source('f1_data', 'driver_standings') }} ds
JOIN {{ source('f1_data', 'races') }} r ON ds.raceId = r.raceId
WHERE ds.raceId IS NOT NULL
  AND ds.driverId IS NOT NULL