SELECT
    raceId AS race_id,
    year,
    round,
    circuitId AS circuit_id,
    name AS race_name,
    date AS race_date
FROM {{ source('f1_optimized', 'opt_races') }}
WHERE raceId IS NOT NULL