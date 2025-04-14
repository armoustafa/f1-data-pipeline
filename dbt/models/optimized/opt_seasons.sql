{{
    config(
        materialized='table',
        cluster_by=["year"]
    )
}}
SELECT
    year,
    url
FROM {{ source('f1_data', 'seasons') }}
WHERE year IS NOT NULL