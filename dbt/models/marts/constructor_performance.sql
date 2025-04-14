SELECT
    rs.year AS season,
    c.constructor_id,
    c.constructor_name,
    SUM(res.points) AS total_points,
    COUNT(CASE WHEN SAFE_CAST(res.position AS INT64) = 1 THEN 1 END) AS wins,
    COUNT(CASE WHEN SAFE_CAST(res.position AS INT64) <= 3 THEN 1 END) AS podiums
FROM {{ ref('stg_results') }} res
JOIN {{ ref('stg_races') }} rs ON res.race_id = rs.race_id
JOIN {{ ref('stg_constructors') }} c ON res.constructor_id = c.constructor_id
GROUP BY rs.year, c.constructor_id, c.constructor_name
ORDER BY season, total_points DESC