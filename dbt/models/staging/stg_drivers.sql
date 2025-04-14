SELECT
    driverId AS driver_id,
    driverRef AS driver_ref,
    number,
    code,
    forename,
    surname,
    dob,
    nationality,
    CONCAT(forename, ' ', surname) AS driver_name
FROM {{ source('f1_optimized', 'opt_drivers') }}
WHERE driverId IS NOT NULL