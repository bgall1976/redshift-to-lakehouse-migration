-- int_policy_claims.sql
-- Intermediate model: join policies with their claims for enriched analysis
-- Migration target: lakehouse_pipelines/silver/ (combined in silver transforms)

WITH policies AS (
    SELECT * FROM {{ ref('stg_policies') }}
),

claims AS (
    SELECT * FROM {{ ref('stg_claims') }}
),

policy_claims AS (
    SELECT
        c.claim_id,
        c.policy_id,
        p.policyholder_full_name,
        p.property_id,
        p.coverage_type_code,
        p.annual_premium,
        p.deductible,
        p.coverage_limit,
        c.claim_date,
        c.reported_date,
        c.closed_date,
        c.claim_type,
        c.claim_status,
        c.claim_amount,
        c.approved_amount,
        c.deductible_applied,
        c.cause_of_loss,
        c.adjuster_id,
        -- Derived fields
        DATEDIFF(DAY, c.claim_date, c.reported_date) AS days_to_report,
        DATEDIFF(DAY, c.reported_date, c.closed_date) AS days_to_close,
        CASE
            WHEN c.claim_amount > p.coverage_limit THEN p.coverage_limit
            ELSE c.claim_amount
        END AS capped_claim_amount,
        c.claim_amount / NULLIF(p.annual_premium, 0) AS claim_to_premium_ratio
    FROM claims c
    INNER JOIN policies p
        ON c.policy_id = p.policy_id
)

SELECT * FROM policy_claims
