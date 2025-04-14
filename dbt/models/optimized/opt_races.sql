{{
    config(
        materialized='table',
        partition_by={
            "field": "date",
            "data_type": "date",
            "granularity": "year"
        },
        cluster_by=["circuitId"]
    )
}}
SELECT
    raceId,
    year,
    round,
    circuitId,
    name,
    CAST(date AS DATE) AS date,
    time
FROM {{ source('f1_data', 'races') }}
WHERE raceId IS NOT NULL
  AND circuitId IS NOT NULL