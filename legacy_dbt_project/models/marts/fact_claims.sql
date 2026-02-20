-- fact_claims.sql
-- Fact table: Insurance claims at the grain of one row per claim
-- Migration target: lakehouse_pipelines/gold/fact_claims.py

WITH policy_claims AS (
    SELECT * FROM {{ ref('int_policy_claims') }}
),

policies AS (
    SELECT * FROM {{ ref('stg_policies') }}
),

properties AS (
    SELECT * FROM {{ ref('stg_properties') }}
),

final AS (
    SELECT
        -- Keys
        pc.claim_id,
        pc.policy_id,
        pc.property_id,
        pc.coverage_type_code,
        pc.claim_date AS claim_date_key,
        pc.reported_date AS reported_date_key,
        pc.closed_date AS closed_date_key,

        -- Claim attributes
        pc.claim_type,
        pc.claim_status,
        pc.cause_of_loss,
        pc.adjuster_id,

        -- Measures
        pc.claim_amount,
        pc.approved_amount,
        pc.deductible_applied,
        pc.capped_claim_amount,
        pc.approved_amount - pc.deductible_applied AS net_claim_payout,

        -- Policy context at time of claim
        pc.annual_premium,
        pc.deductible AS policy_deductible,
        pc.coverage_limit,
        pc.claim_to_premium_ratio,

        -- Timing measures
        pc.days_to_report,
        pc.days_to_close,

        -- Property risk context
        prop.state AS property_state,
        prop.flood_zone,
        prop.wind_zone,

        -- Derived flags
        CASE WHEN pc.claim_amount > pc.coverage_limit THEN TRUE ELSE FALSE END AS exceeds_coverage_limit,
        CASE WHEN pc.days_to_report > 30 THEN TRUE ELSE FALSE END AS late_reported,
        CASE WHEN pc.claim_status = 'CLOSED' THEN TRUE ELSE FALSE END AS is_closed,
        CASE WHEN pc.approved_amount > 0 THEN TRUE ELSE FALSE END AS is_paid

    FROM policy_claims pc
    LEFT JOIN properties prop
        ON pc.property_id = prop.property_id
)

SELECT * FROM final
