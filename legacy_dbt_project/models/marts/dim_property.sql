-- dim_property.sql
-- Dimension: Property details with risk attributes for climate modeling
-- Migration target: lakehouse_pipelines/gold/dim_property.py

WITH properties AS (
    SELECT * FROM {{ ref('stg_properties') }}
),

final AS (
    SELECT
        {{ generate_surrogate_key(['property_id', 'updated_at']) }} AS property_sk,
        property_id,
        street_address,
        city,
        state,
        zip_code,
        county,
        latitude,
        longitude,
        year_built,
        square_footage,
        construction_type,
        roof_type,
        stories,
        occupancy_type,
        flood_zone,
        wind_zone,
        property_value,
        -- Derived risk attributes
        EXTRACT(YEAR FROM CURRENT_DATE) - year_built AS property_age_years,
        CASE
            WHEN construction_type IN ('MASONRY', 'CONCRETE') THEN 'LOW'
            WHEN construction_type IN ('FRAME', 'WOOD') THEN 'HIGH'
            ELSE 'MEDIUM'
        END AS construction_risk_tier,
        CASE
            WHEN flood_zone IN ('A', 'AE', 'V', 'VE') THEN 'HIGH'
            WHEN flood_zone IN ('B', 'X500') THEN 'MODERATE'
            ELSE 'LOW'
        END AS flood_risk_tier,
        CASE
            WHEN wind_zone IN ('4', '5') THEN 'HIGH'
            WHEN wind_zone IN ('3') THEN 'MODERATE'
            ELSE 'LOW'
        END AS wind_risk_tier,
        -- SCD fields
        updated_at AS effective_start_date,
        CAST(NULL AS TIMESTAMP) AS effective_end_date,
        TRUE AS is_current,
        created_at,
        updated_at
    FROM properties
)

SELECT * FROM final
