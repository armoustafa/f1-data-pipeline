{{
    config(
        materialized='table',
        cluster_by=["constructorId"]
    )
}}
SELECT
    constructorId,
    constructorRef,
    name,
    nationality
FROM {{ source('f1_data', 'constructors') }}
WHERE constructorId IS NOT NULL