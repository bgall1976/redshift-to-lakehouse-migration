-- dim_policy.sql
-- Dimension: Policy master with SCD Type 2 support
-- Migration target: lakehouse_pipelines/gold/dim_policy.py

WITH policies AS (
    SELECT * FROM {{ ref('stg_policies') }}
),

premium_summary AS (
    SELECT * FROM {{ ref('int_premium_summary') }}
),

final AS (
    SELECT
        {{ generate_surrogate_key(['p.policy_id', 'p.updated_at']) }} AS policy_sk,
        p.policy_id,
        p.policyholder_first_name,
        p.policyholder_last_name,
        p.policyholder_full_name,
        p.policyholder_email,
        p.property_id,
        p.coverage_type_code,
        p.effective_date,
        p.expiration_date,
        p.status,
        p.annual_premium,
        p.deductible,
        p.coverage_limit,
        p.agent_id,
        p.channel,
        -- Premium enrichment
        COALESCE(ps.total_payments, 0) AS total_premium_payments,
        COALESCE(ps.total_collected, 0) AS total_premium_collected,
        COALESCE(ps.late_payments, 0) AS late_premium_payments,
        -- Derived attributes
        DATEDIFF(DAY, p.effective_date, p.expiration_date) AS policy_term_days,
        CASE
            WHEN p.status = 'ACTIVE' AND p.expiration_date >= CURRENT_DATE THEN 'IN FORCE'
            WHEN p.status = 'ACTIVE' AND p.expiration_date < CURRENT_DATE THEN 'EXPIRED'
            WHEN p.status = 'CANCELLED' THEN 'CANCELLED'
            WHEN p.status = 'PENDING' THEN 'PENDING'
            ELSE 'UNKNOWN'
        END AS policy_status_category,
        -- SCD Type 2 fields
        p.updated_at AS effective_start_date,
        CAST(NULL AS TIMESTAMP) AS effective_end_date,
        TRUE AS is_current,
        p.created_at,
        p.updated_at
    FROM policies p
    LEFT JOIN premium_summary ps
        ON p.policy_id = ps.policy_id
)

SELECT * FROM final
