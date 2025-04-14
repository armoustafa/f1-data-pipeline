{{
    config(
        materialized='table',
        cluster_by=["circuitId"]
    )
}}
SELECT
    circuitId,
    circuitRef,
    name,
    location,
    country,
    lat,
    lng,
    alt
FROM {{ source('f1_data', 'circuits') }}
WHERE circuitId IS NOT NULL