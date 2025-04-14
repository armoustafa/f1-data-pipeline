{{
    config(
        materialized='table',
        cluster_by=["driverId"]
    )
}}
SELECT
    driverId,
    driverRef,
    number,
    code,
    forename,
    surname,
    dob,
    nationality
FROM {{ source('f1_data', 'drivers') }}
WHERE driverId IS NOT NULL