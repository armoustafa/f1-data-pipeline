SELECT
    rs.race_id,
    rs.race_name,
    rs.race_date,
    rs.circuit_id,
    res.driver_id,
    d.driver_name,
    res.fastest_lap_time,
    res.fastest_lap_speed
FROM {{ ref('stg_results') }} res
JOIN {{ ref('stg_races') }} rs ON res.race_id = rs.race_id
JOIN {{ ref('stg_drivers') }} d ON res.driver_id = d.driver_id
WHERE res.fastest_lap_time IS NOT NULL
  AND res.fastest_lap_speed IS NOT NULL
ORDER BY rs.race_date, res.fastest_lap_time