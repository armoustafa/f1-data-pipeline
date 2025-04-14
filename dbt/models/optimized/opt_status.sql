{{
    config(
        materialized='table',
        cluster_by=["statusId"]
    )
}}
SELECT
    statusId,
    status
FROM {{ source('f1_data', 'status') }}
WHERE statusId IS NOT NULL