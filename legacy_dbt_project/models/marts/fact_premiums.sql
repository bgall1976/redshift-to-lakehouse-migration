-- fact_premiums.sql
-- Fact table: Premium payments at the grain of one row per payment
-- Migration target: lakehouse_pipelines/gold/fact_premiums.py

WITH premiums AS (
    SELECT * FROM {{ ref('stg_premiums') }}
),

policies AS (
    SELECT
        policy_id,
        property_id,
        coverage_type_code,
        agent_id,
        channel
    FROM {{ ref('stg_policies') }}
),

final AS (
    SELECT
        -- Keys
        pr.premium_id,
        pr.policy_id,
        p.property_id,
        p.coverage_type_code,
        pr.payment_date AS payment_date_key,
        pr.due_date AS due_date_key,
        pr.period_start_date,
        pr.period_end_date,

        -- Payment attributes
        pr.payment_method,
        pr.payment_status,
        pr.billing_period,

        -- Policy context
        p.agent_id,
        p.channel,

        -- Measures
        pr.amount AS premium_amount,
        CASE WHEN pr.payment_status = 'COMPLETED' THEN pr.amount ELSE 0 END AS collected_amount,
        CASE WHEN pr.payment_status = 'FAILED' THEN pr.amount ELSE 0 END AS failed_amount,

        -- Timing measures
        DATEDIFF(DAY, pr.due_date, pr.payment_date) AS days_from_due,

        -- Derived flags
        CASE WHEN pr.payment_date > pr.due_date THEN TRUE ELSE FALSE END AS is_late_payment,
        CASE WHEN pr.payment_status = 'COMPLETED' THEN TRUE ELSE FALSE END AS is_collected,
        CASE WHEN pr.payment_status = 'FAILED' THEN TRUE ELSE FALSE END AS is_failed

    FROM premiums pr
    LEFT JOIN policies p
        ON pr.policy_id = p.policy_id
)

SELECT * FROM final
