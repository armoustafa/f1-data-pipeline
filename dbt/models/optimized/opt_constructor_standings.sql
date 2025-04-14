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
    cs.constructorStandingsId,
    cs.raceId,
    cs.constructorId,
    cs.points,
    cs.position,
    cs.positionText,
    cs.wins,
    CAST(r.date AS DATE) AS date
FROM {{ source('f1_data', 'constructor_standings') }} cs
JOIN {{ source('f1_data', 'races') }} r ON cs.raceId = r.raceId
WHERE cs.raceId IS NOT NULL
  AND cs.constructorId IS NOT NULL