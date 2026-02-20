-- ============================================================
-- Audit Queries for GLBA/CCPA Compliance
-- ============================================================
-- These queries run against Unity Catalog's audit log to
-- monitor data access patterns and detect anomalies.
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- 1. Who accessed PII tables in the last 7 days?
-- ────────────────────────────────────────────────────────────

SELECT
    event_time,
    user_identity.email AS user_email,
    request_params.full_name_arg AS table_accessed,
    action_name,
    response.status_code
FROM system.access.audit
WHERE action_name IN ('getTable', 'commandSubmit')
    AND request_params.full_name_arg LIKE 'fintech_catalog.gold.dim_policy%'
    AND event_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
ORDER BY event_time DESC;

-- ────────────────────────────────────────────────────────────
-- 2. Detect anomalous bulk data access
-- ────────────────────────────────────────────────────────────

SELECT
    user_identity.email AS user_email,
    DATE(event_time) AS access_date,
    COUNT(*) AS query_count,
    COUNT(DISTINCT request_params.full_name_arg) AS distinct_tables
FROM system.access.audit
WHERE action_name = 'commandSubmit'
    AND event_time >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
GROUP BY user_identity.email, DATE(event_time)
HAVING COUNT(*) > 100
ORDER BY query_count DESC;

-- ────────────────────────────────────────────────────────────
-- 3. CCPA: Find all tables containing data for a specific user
--    (Supports data subject access requests)
-- ────────────────────────────────────────────────────────────

-- This would be parameterized in production with the requesting user's ID
-- DECLARE @subject_policy_id = 'POL-0001234';

-- SELECT * FROM fintech_catalog.gold.dim_policy
-- WHERE policy_id = @subject_policy_id;

-- SELECT * FROM fintech_catalog.gold.fact_claims
-- WHERE policy_id = @subject_policy_id;

-- SELECT * FROM fintech_catalog.gold.fact_premiums
-- WHERE policy_id = @subject_policy_id;

-- ────────────────────────────────────────────────────────────
-- 4. Data lineage verification
-- ────────────────────────────────────────────────────────────

SELECT
    table_catalog,
    table_schema,
    table_name,
    column_name,
    data_type,
    comment
FROM information_schema.columns
WHERE table_catalog = 'fintech_catalog'
ORDER BY table_schema, table_name, ordinal_position;
