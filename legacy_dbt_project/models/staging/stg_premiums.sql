-- stg_premiums.sql
-- Staging model: clean and standardize premium payment data
-- Migration target: lakehouse_pipelines/silver/clean_premiums.py

WITH source AS (
    SELECT * FROM {{ source('raw', 'premiums') }}
),

cleaned AS (
    SELECT
        premium_id,
        policy_id,
        CAST(payment_date AS DATE) AS payment_date,
        CAST(due_date AS DATE) AS due_date,
        CAST(amount AS DECIMAL(12, 2)) AS amount,
        UPPER(TRIM(payment_method)) AS payment_method,
        UPPER(TRIM(payment_status)) AS payment_status,
        TRIM(billing_period) AS billing_period,
        CAST(period_start_date AS DATE) AS period_start_date,
        CAST(period_end_date AS DATE) AS period_end_date,
        CAST(created_at AS TIMESTAMP) AS created_at
    FROM source
    WHERE premium_id IS NOT NULL
        AND policy_id IS NOT NULL
        AND amount > 0
)

SELECT * FROM cleaned
