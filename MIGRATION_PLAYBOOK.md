# Migration Playbook: Redshift/dbt to Databricks Lakehouse

## Overview

This playbook documents the phased migration strategy from a Redshift + dbt data warehouse to a Databricks Lakehouse architecture on S3. It is designed as a reference for teams executing similar migrations in regulated industries.

## Phase 0: Discovery & Inventory (Weeks 1-2)

### Objectives
- Catalog all dbt models, their dependencies, and downstream consumers
- Map Redshift schemas, tables, views, and stored procedures
- Identify data volumes, refresh frequencies, and SLAs
- Document regulatory and compliance requirements

### Deliverables
- Complete dbt model inventory with DAG visualization
- Redshift table catalog with row counts, data types, and storage
- Stakeholder map: who consumes what data and how
- Compliance requirements matrix (GLBA, CCPA, GDPR applicability)
- Risk assessment for migration ordering

### Key Activities

**dbt Model Audit**
```bash
# Generate dbt docs and DAG
dbt docs generate
dbt docs serve

# Export model manifest for analysis
cat target/manifest.json | python -m json.tool > model_inventory.json
```

**Redshift Schema Export**
```sql
-- Export all table definitions
SELECT schemaname, tablename, column, type, encoding, distkey, sortkey
FROM pg_table_def
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY schemaname, tablename, column;

-- Row counts and storage
SELECT schema, "table", size AS size_mb, tbl_rows
FROM svv_table_info
ORDER BY size DESC;
```

## Phase 1: Foundation (Weeks 3-4)

### Objectives
- Provision target infrastructure on AWS + Databricks
- Establish Unity Catalog structure with security policies
- Set up CI/CD pipelines and development workflows
- Create Bronze layer ingestion framework

### Infrastructure Setup

**S3 Bucket Architecture**
```
s3://company-datalake-{env}-bronze/    # Raw ingestion
s3://company-datalake-{env}-silver/    # Cleaned & validated
s3://company-datalake-{env}-gold/      # Business-ready models
s3://company-datalake-{env}-sandbox/   # Development & testing
```

**Unity Catalog Hierarchy**
```
fintech_catalog (catalog)
├── bronze (schema)
│   ├── raw_policies
│   ├── raw_claims
│   ├── raw_premiums
│   └── raw_properties
├── silver (schema)
│   ├── cleaned_policies
│   ├── cleaned_claims
│   ├── cleaned_premiums
│   └── cleaned_properties
├── gold (schema)
│   ├── dim_policy
│   ├── dim_property
│   ├── dim_coverage
│   ├── dim_date
│   ├── fact_claims
│   └── fact_premiums
└── data_quality (schema)
    ├── validation_results
    └── reconciliation_log
```

## Phase 2: Bronze Layer (Weeks 5-6)

### Objectives
- Implement raw data ingestion from source systems to S3/Delta
- Establish metadata tracking and source lineage
- Configure Auto Loader for incremental ingestion

### dbt Staging to Bronze Mapping

| dbt Staging Model | Bronze Table | Ingestion Method |
|---|---|---|
| stg_policies | bronze.raw_policies | Auto Loader (CSV/Parquet) |
| stg_claims | bronze.raw_claims | Auto Loader (CSV/Parquet) |
| stg_premiums | bronze.raw_premiums | Auto Loader (CSV/Parquet) |
| stg_properties | bronze.raw_properties | Auto Loader (CSV/Parquet) |

### Design Principles
- Append-only: never modify Bronze data after landing
- Add metadata columns: _ingestion_timestamp, _source_file, _batch_id
- Preserve original data types and formats
- Schema-on-read with permissive mode for new columns

## Phase 3: Silver Layer (Weeks 7-9)

### Objectives
- Translate dbt staging and intermediate models to PySpark transforms
- Implement data validation and quality checks
- Establish schema contracts between layers

### dbt Model Translation Guide

**Example: dbt staging model to Silver PySpark**

dbt original (stg_policies.sql):
```sql
SELECT
    policy_id,
    TRIM(policyholder_name) AS policyholder_name,
    CAST(effective_date AS DATE) AS effective_date,
    CAST(expiration_date AS DATE) AS expiration_date,
    UPPER(status) AS status,
    CAST(annual_premium AS DECIMAL(12,2)) AS annual_premium
FROM {{ source('raw', 'policies') }}
WHERE policy_id IS NOT NULL
```

PySpark equivalent (clean_policies.py):
```python
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType

df_bronze = spark.read.table("fintech_catalog.bronze.raw_policies")

df_silver = (
    df_bronze
    .filter(F.col("policy_id").isNotNull())
    .withColumn("policyholder_name", F.trim(F.col("policyholder_name")))
    .withColumn("effective_date", F.col("effective_date").cast(DateType()))
    .withColumn("expiration_date", F.col("expiration_date").cast(DateType()))
    .withColumn("status", F.upper(F.col("status")))
    .withColumn("annual_premium", F.col("annual_premium").cast(DecimalType(12, 2)))
    .withColumn("_cleaned_timestamp", F.current_timestamp())
)

df_silver.write.mode("overwrite").saveAsTable("fintech_catalog.silver.cleaned_policies")
```

### dbt Test Translation

| dbt Test | Lakehouse Equivalent |
|---|---|
| unique | df.groupBy(key).count().filter("count > 1") |
| not_null | df.filter(col(field).isNull()).count() == 0 |
| accepted_values | df.filter(~col(field).isin(values)).count() == 0 |
| relationships | df.join(ref_df, key, "left_anti").count() == 0 |
| custom SQL test | Python function returning pass/fail with metrics |

## Phase 4: Gold Layer (Weeks 10-12)

### Objectives
- Translate dbt marts models to Gold layer star schema
- Implement slowly changing dimensions (SCD Type 2)
- Optimize for downstream BI query patterns

### Dimensional Model

```
              ┌──────────────┐
              │   dim_date   │
              └──────┬───────┘
                     │
┌──────────────┐     │     ┌──────────────┐
│  dim_policy  │─────┼─────│ fact_claims  │
└──────────────┘     │     └──────┬───────┘
                     │            │
┌──────────────┐     │     ┌──────┴───────┐
│ dim_property │─────┼─────│fact_premiums │
└──────────────┘     │     └──────────────┘
                     │
┌──────────────┐     │
│ dim_coverage │─────┘
└──────────────┘
```

### SCD Type 2 Implementation Pattern
```python
from delta.tables import DeltaTable

def merge_scd2(spark, target_table, source_df, key_cols, tracked_cols):
    """Merge with SCD Type 2 logic for tracked attribute changes."""
    target = DeltaTable.forName(spark, target_table)

    # Identify changed records
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in key_cols])
    change_condition = " OR ".join([f"target.{c} != source.{c}" for c in tracked_cols])

    # Close existing records that have changes
    target.alias("target").merge(
        source_df.alias("source"),
        merge_condition
    ).whenMatchedUpdate(
        condition=f"target.is_current = TRUE AND ({change_condition})",
        set={
            "is_current": "FALSE",
            "effective_end_date": "source._cleaned_timestamp"
        }
    ).whenNotMatchedInsertAll().execute()

    # Insert new version of changed records
    # (simplified — production would handle this atomically)
```

## Phase 5: Validation & Parallel Run (Weeks 13-15)

### Objectives
- Run Redshift and lakehouse pipelines in parallel
- Compare outputs systematically
- Establish acceptance criteria with stakeholders

### Reconciliation Checks

1. **Row Count Comparison**: Every table, every refresh
2. **Aggregate Validation**: SUM, AVG, MIN, MAX on numeric columns
3. **Distribution Comparison**: Value frequency counts for categorical columns
4. **Sample Record Comparison**: Random sample of 1000 records per table
5. **Schema Comparison**: Column names, types, and nullability
6. **Freshness Comparison**: Data latency between source event and availability

### Go/No-Go Criteria

| Metric | Threshold |
|---|---|
| Row count match | 100% (zero tolerance) |
| Aggregate value match | Within 0.01% (floating point tolerance) |
| Schema compatibility | 100% column mapping |
| Query performance | Within 120% of Redshift baseline |
| Data freshness | Equal or better than current SLA |
| All DQ checks passing | 100% |

## Phase 6: Cutover & Decommission (Weeks 16-18)

### Objectives
- Switch BI consumers to lakehouse Gold layer
- Decommission Redshift pipelines
- Document lessons learned

### Cutover Sequence
1. Freeze dbt model changes (code freeze)
2. Final parallel validation run
3. Update BI connection strings to Databricks SQL Warehouse
4. Validate all dashboards and reports
5. Monitor for 1 week in production
6. Decommission Redshift staging and transformation schemas
7. Archive Redshift warehouse data to S3 Glacier
8. Terminate Redshift cluster

### Rollback Plan
- Maintain Redshift cluster in standby for 30 days post-cutover
- Keep dbt project repository intact and deployable
- Document rollback procedure with estimated recovery time

## Cost Comparison Framework

| Component | Redshift (Legacy) | Lakehouse (Target) |
|---|---|---|
| Compute | RA3 node-hours | Databricks DBU (autoscaling) |
| Storage | Redshift managed storage | S3 (tiered: Standard/IA/Glacier) |
| ETL | dbt Cloud + Redshift compute | Databricks Jobs (spot instances) |
| BI Serving | Redshift concurrency scaling | SQL Warehouse (serverless) |
| Governance | Manual + Redshift GRANT | Unity Catalog (included) |

## Lessons Learned Template

After completing the migration, document:
- What dbt patterns translated cleanly vs. required redesign
- Performance differences (better and worse) between Redshift and Spark
- Unity Catalog capabilities that replaced custom governance tooling
- Cost actuals vs. projections
- Stakeholder feedback on data quality and availability
- Recommendations for future migrations
