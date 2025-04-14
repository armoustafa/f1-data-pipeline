SELECT
    ps.race_id,
    ps.race_date,
    rs.race_name,
    rs.year AS season,
    ps.constructor_id,
    c.constructor_name,
    AVG(ps.pit_stop_duration_ms) / 1000 AS avg_pit_stop_seconds,
    COUNT(*) AS pit_stop_count
FROM {{ ref('stg_pit_stops') }} ps
JOIN {{ ref('stg_races') }} rs ON ps.race_id = rs.race_id
JOIN {{ ref('stg_constructors') }} c ON ps.constructor_id = c.constructor_id
GROUP BY ps.race_id, ps.race_date, rs.race_name, rs.year, ps.constructor_id, c.constructor_name
ORDER BY season, race_date