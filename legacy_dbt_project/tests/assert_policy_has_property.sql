-- assert_policy_has_property.sql
-- Custom test: Every active policy must reference a valid property

SELECT
    p.policy_id,
    p.property_id
FROM {{ ref('dim_policy') }} p
LEFT JOIN {{ ref('dim_property') }} prop
    ON p.property_id = prop.property_id
WHERE p.is_current = TRUE
    AND p.status = 'ACTIVE'
    AND prop.property_id IS NULL
