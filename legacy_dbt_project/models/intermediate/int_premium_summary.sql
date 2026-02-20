-- int_premium_summary.sql
-- Intermediate model: aggregate premium payments by policy
-- Migration target: lakehouse_pipelines/silver/ (combined in silver transforms)

WITH premiums AS (
    SELECT * FROM {{ ref('stg_premiums') }}
),

premium_summary AS (
    SELECT
        policy_id,
        COUNT(*) AS total_payments,
        SUM(amount) AS total_paid,
        SUM(CASE WHEN payment_status = 'COMPLETED' THEN amount ELSE 0 END) AS total_collected,
        SUM(CASE WHEN payment_status = 'FAILED' THEN amount ELSE 0 END) AS total_failed,
        SUM(CASE WHEN payment_status = 'PENDING' THEN amount ELSE 0 END) AS total_pending,
        MIN(payment_date) AS first_payment_date,
        MAX(payment_date) AS last_payment_date,
        COUNT(CASE WHEN payment_date > due_date THEN 1 END) AS late_payments,
        AVG(DATEDIFF(DAY, due_date, payment_date)) AS avg_days_from_due
    FROM premiums
    GROUP BY policy_id
)

SELECT * FROM premium_summary
