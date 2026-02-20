-- dim_coverage.sql
-- Dimension: Coverage type reference data
-- Migration target: lakehouse_pipelines/gold/dim_coverage.py

WITH coverage_types AS (
    SELECT DISTINCT
        coverage_type_code
    FROM {{ ref('stg_policies') }}
),

final AS (
    SELECT
        {{ generate_surrogate_key(['coverage_type_code']) }} AS coverage_sk,
        coverage_type_code,
        CASE coverage_type_code
            WHEN 'HO3' THEN 'Special Form Homeowners'
            WHEN 'HO5' THEN 'Comprehensive Form Homeowners'
            WHEN 'HO6' THEN 'Condo Unit Owners'
            WHEN 'DP1' THEN 'Basic Dwelling Fire'
            WHEN 'DP3' THEN 'Special Dwelling Fire'
            WHEN 'HO4' THEN 'Renters Insurance'
            WHEN 'FLOOD' THEN 'Flood Insurance'
            WHEN 'WIND' THEN 'Wind/Hurricane Coverage'
            ELSE 'Other'
        END AS coverage_type_name,
        CASE coverage_type_code
            WHEN 'HO3' THEN 'Homeowners'
            WHEN 'HO5' THEN 'Homeowners'
            WHEN 'HO6' THEN 'Homeowners'
            WHEN 'DP1' THEN 'Dwelling'
            WHEN 'DP3' THEN 'Dwelling'
            WHEN 'HO4' THEN 'Renters'
            WHEN 'FLOOD' THEN 'Specialty'
            WHEN 'WIND' THEN 'Specialty'
            ELSE 'Other'
        END AS coverage_category,
        TRUE AS is_current
    FROM coverage_types
)

SELECT * FROM final
