-- stg_policies.sql
-- Staging model: clean and standardize raw policy data from source system
-- Migration target: lakehouse_pipelines/silver/clean_policies.py

WITH source AS (
    SELECT * FROM {{ source('raw', 'policies') }}
),

cleaned AS (
    SELECT
        policy_id,
        TRIM(policyholder_first_name) AS policyholder_first_name,
        TRIM(policyholder_last_name) AS policyholder_last_name,
        TRIM(policyholder_first_name) || ' ' || TRIM(policyholder_last_name) AS policyholder_full_name,
        TRIM(policyholder_email) AS policyholder_email,
        property_id,
        coverage_type_code,
        CAST(effective_date AS DATE) AS effective_date,
        CAST(expiration_date AS DATE) AS expiration_date,
        UPPER(TRIM(status)) AS status,
        CAST(annual_premium AS DECIMAL(12, 2)) AS annual_premium,
        CAST(deductible AS DECIMAL(12, 2)) AS deductible,
        CAST(coverage_limit AS DECIMAL(14, 2)) AS coverage_limit,
        TRIM(agent_id) AS agent_id,
        TRIM(channel) AS channel,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at
    FROM source
    WHERE policy_id IS NOT NULL
        AND effective_date IS NOT NULL
)

SELECT * FROM cleaned
