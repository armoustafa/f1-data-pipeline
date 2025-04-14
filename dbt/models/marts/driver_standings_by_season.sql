WITH race_results AS (
    SELECT
        r.driver_id,
        d.driver_name,
        rs.year AS season,
        SUM(r.points) AS total_points,
        COUNT(CASE WHEN SAFE_CAST(r.position AS INT64) = 1 THEN 1 END) AS wins,
        COUNT(CASE WHEN SAFE_CAST(r.position AS INT64) <= 3 THEN 1 END) AS podiums
    FROM {{ ref('stg_results') }} r
    JOIN {{ ref('stg_drivers') }} d ON r.driver_id = d.driver_id
    JOIN {{ ref('stg_races') }} rs ON r.race_id = rs.race_id
    GROUP BY r.driver_id, d.driver_name, rs.year
)
SELECT
    driver_id,
    driver_name,
    season,
    total_points,
    wins,
    podiums,
    RANK() OVER (PARTITION BY season ORDER BY total_points DESC) AS championship_rank
FROM race_results
WHERE total_points > 0
ORDER BY season, championship_rank