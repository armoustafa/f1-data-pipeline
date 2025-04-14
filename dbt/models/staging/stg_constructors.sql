SELECT
    constructorId AS constructor_id,
    constructorRef AS constructor_ref,
    name AS constructor_name,
    nationality
FROM {{ source('f1_optimized', 'opt_constructors') }}
WHERE constructorId IS NOT NULL