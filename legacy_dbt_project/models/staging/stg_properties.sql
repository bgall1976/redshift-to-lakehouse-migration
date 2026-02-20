-- stg_properties.sql
-- Staging model: clean and standardize property data
-- Migration target: lakehouse_pipelines/silver/clean_properties.py

WITH source AS (
    SELECT * FROM {{ source('raw', 'properties') }}
),

cleaned AS (
    SELECT
        property_id,
        TRIM(street_address) AS street_address,
        TRIM(city) AS city,
        UPPER(TRIM(state)) AS state,
        TRIM(zip_code) AS zip_code,
        TRIM(county) AS county,
        CAST(latitude AS DECIMAL(10, 7)) AS latitude,
        CAST(longitude AS DECIMAL(10, 7)) AS longitude,
        CAST(year_built AS INTEGER) AS year_built,
        CAST(square_footage AS INTEGER) AS square_footage,
        UPPER(TRIM(construction_type)) AS construction_type,
        UPPER(TRIM(roof_type)) AS roof_type,
        CAST(stories AS INTEGER) AS stories,
        UPPER(TRIM(occupancy_type)) AS occupancy_type,
        UPPER(TRIM(flood_zone)) AS flood_zone,
        UPPER(TRIM(wind_zone)) AS wind_zone,
        CAST(property_value AS DECIMAL(14, 2)) AS property_value,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at
    FROM source
    WHERE property_id IS NOT NULL
)

SELECT * FROM cleaned
