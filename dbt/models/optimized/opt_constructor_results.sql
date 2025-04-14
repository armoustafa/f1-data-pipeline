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
    cr.constructorResultsId,
    cr.raceId,
    cr.constructorId,
    cr.points,
    cr.status,
    CAST(r.date AS DATE) AS date
FROM {{ source('f1_data', 'constructor_results') }} cr
JOIN {{ source('f1_data', 'races') }} r ON cr.raceId = r.raceId
WHERE cr.raceId IS NOT NULL
  AND cr.constructorId IS NOT NULL