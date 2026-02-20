-- ============================================================
-- Unity Catalog Setup for Fintech Lakehouse
-- ============================================================
-- Run this in Databricks SQL after Terraform provisions the workspace.
-- Creates the catalog, schemas, and managed storage locations.
-- ============================================================

-- Create the catalog
CREATE CATALOG IF NOT EXISTS fintech_catalog_dev
COMMENT 'Insurance data lakehouse (dev) - migrated from Redshift/dbt';

USE CATALOG fintech_catalog_dev;

-- Create schemas for each medallion layer
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw ingestion layer. Append-only. Source data in original format.'
MANAGED LOCATION 's3://fintechco-datalake-dev-bronze/unity-catalog/';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Cleaned, validated, and conformed data. Schema-enforced.'
MANAGED LOCATION 's3://fintechco-datalake-dev-silver/unity-catalog/';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Business-ready dimensional models. Star schema for reporting.'
MANAGED LOCATION 's3://fintechco-datalake-dev-gold/unity-catalog/';

CREATE SCHEMA IF NOT EXISTS data_quality
COMMENT 'Data quality check results, reconciliation logs, and monitoring.'
MANAGED LOCATION 's3://fintechco-datalake-dev-gold/data-quality/';

-- Create a reconciliation results table for migration tracking
CREATE TABLE IF NOT EXISTS data_quality.reconciliation_log (
    run_id STRING NOT NULL,
    run_timestamp TIMESTAMP NOT NULL,
    table_name STRING NOT NULL,
    check_type STRING NOT NULL,
    passed BOOLEAN NOT NULL,
    legacy_value STRING,
    lakehouse_value STRING,
    difference DOUBLE,
    tolerance DOUBLE,
    details STRING
)
USING DELTA
COMMENT 'Migration reconciliation results between Redshift and Lakehouse'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Tag tables with data classification for GLBA compliance
ALTER TABLE gold.dim_policy SET TAGS ('data_classification' = 'confidential', 'contains_pii' = 'true');
ALTER TABLE gold.dim_property SET TAGS ('data_classification' = 'confidential', 'contains_pii' = 'true');
ALTER TABLE gold.fact_claims SET TAGS ('data_classification' = 'internal', 'contains_pii' = 'false');
ALTER TABLE gold.fact_premiums SET TAGS ('data_classification' = 'internal', 'contains_pii' = 'false');
