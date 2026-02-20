-- stg_claims.sql
-- Staging model: clean and standardize raw claims data
-- Migration target: lakehouse_pipelines/silver/clean_claims.py

WITH source AS (
    SELECT * FROM {{ source('raw', 'claims') }}
),

cleaned AS (
    SELECT
        claim_id,
        policy_id,
        CAST(claim_date AS DATE) AS claim_date,
        CAST(reported_date AS DATE) AS reported_date,
        CAST(closed_date AS DATE) AS closed_date,
        UPPER(TRIM(claim_type)) AS claim_type,
        UPPER(TRIM(claim_status)) AS claim_status,
        CAST(claim_amount AS DECIMAL(12, 2)) AS claim_amount,
        CAST(approved_amount AS DECIMAL(12, 2)) AS approved_amount,
        CAST(deductible_applied AS DECIMAL(12, 2)) AS deductible_applied,
        TRIM(adjuster_id) AS adjuster_id,
        TRIM(cause_of_loss) AS cause_of_loss,
        TRIM(description) AS description,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at
    FROM source
    WHERE claim_id IS NOT NULL
        AND policy_id IS NOT NULL
        AND claim_amount >= 0
)

SELECT * FROM cleaned
