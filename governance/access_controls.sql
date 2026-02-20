-- ============================================================
-- Unity Catalog Access Controls for Fintech Lakehouse
-- ============================================================
-- Implements RBAC aligned with GLBA compliance requirements.
-- Replaces Redshift GRANT statements with Unity Catalog equivalents.
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- 1. Catalog and Schema Setup
-- ────────────────────────────────────────────────────────────

CREATE CATALOG IF NOT EXISTS fintech_catalog_dev;
USE CATALOG fintech_catalog_dev;

CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw ingestion layer - append only';
CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Cleaned and validated data';
CREATE SCHEMA IF NOT EXISTS gold COMMENT 'Business-ready dimensional models';
CREATE SCHEMA IF NOT EXISTS data_quality COMMENT 'DQ results and reconciliation logs';

-- ────────────────────────────────────────────────────────────
-- 2. Role-Based Access Control
-- ────────────────────────────────────────────────────────────

-- Data Engineers: Full access to all layers
GRANT USE CATALOG ON CATALOG fintech_catalog_dev TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA fintech_catalog_dev.bronze TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA fintech_catalog_dev.silver TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA fintech_catalog_dev.gold TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA fintech_catalog_dev.bronze TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA fintech_catalog_dev.silver TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA fintech_catalog_dev.gold TO `data_engineers`;

-- BI Analysts: Read access to Gold layer only
GRANT USE CATALOG ON CATALOG fintech_catalog_dev TO `bi_analysts`;
GRANT USE SCHEMA ON SCHEMA fintech_catalog_dev.gold TO `bi_analysts`;
GRANT SELECT ON SCHEMA fintech_catalog_dev.gold TO `bi_analysts`;

-- Data Scientists: Read access to Silver and Gold
GRANT USE CATALOG ON CATALOG fintech_catalog_dev TO `data_scientists`;
GRANT USE SCHEMA ON SCHEMA fintech_catalog_dev.silver TO `data_scientists`;
GRANT USE SCHEMA ON SCHEMA fintech_catalog_dev.gold TO `data_scientists`;
GRANT SELECT ON SCHEMA fintech_catalog_dev.silver TO `data_scientists`;
GRANT SELECT ON SCHEMA fintech_catalog_dev.gold TO `data_scientists`;

-- Application Services: Read access to Gold (for API serving)
GRANT USE CATALOG ON CATALOG fintech_catalog_dev TO `app_services`;
GRANT USE SCHEMA ON SCHEMA fintech_catalog_dev.gold TO `app_services`;
GRANT SELECT ON SCHEMA fintech_catalog_dev.gold TO `app_services`;

-- ────────────────────────────────────────────────────────────
-- 3. Column-Level Security (PII Masking)
-- ────────────────────────────────────────────────────────────
-- GLBA requires restricting access to customer financial data.
-- Only authorized roles can see unmasked PII.

-- Create a function to mask email addresses
CREATE OR REPLACE FUNCTION fintech_catalog_dev.gold.mask_email(email STRING)
RETURNS STRING
RETURN CASE
    WHEN IS_MEMBER('pii_authorized') THEN email
    ELSE CONCAT(LEFT(email, 2), '***@***.***')
END;

-- Create a function to mask names
CREATE OR REPLACE FUNCTION fintech_catalog_dev.gold.mask_name(name STRING)
RETURNS STRING
RETURN CASE
    WHEN IS_MEMBER('pii_authorized') THEN name
    ELSE CONCAT(LEFT(name, 1), REPEAT('*', LENGTH(name) - 1))
END;

-- ────────────────────────────────────────────────────────────
-- 4. Secure Views for PII-Masked Access
-- ────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW fintech_catalog_dev.gold.v_dim_policy_masked AS
SELECT
    policy_sk,
    policy_id,
    fintech_catalog_dev.gold.mask_name(policyholder_first_name) AS policyholder_first_name,
    fintech_catalog_dev.gold.mask_name(policyholder_last_name) AS policyholder_last_name,
    fintech_catalog_dev.gold.mask_name(policyholder_full_name) AS policyholder_full_name,
    fintech_catalog_dev.gold.mask_email(policyholder_email) AS policyholder_email,
    -- Non-PII fields pass through unchanged
    property_id,
    coverage_type_code,
    effective_date,
    expiration_date,
    status,
    annual_premium,
    deductible,
    coverage_limit,
    agent_id,
    channel,
    total_premium_payments,
    total_premium_collected,
    late_premium_payments,
    policy_term_days,
    policy_status_category,
    is_current
FROM fintech_catalog_dev.gold.dim_policy;

-- Grant BI analysts access to masked view instead of base table
REVOKE SELECT ON TABLE fintech_catalog_dev.gold.dim_policy FROM `bi_analysts`;
GRANT SELECT ON VIEW fintech_catalog_dev.gold.v_dim_policy_masked TO `bi_analysts`;
