# dbt to Lakehouse Mapping Reference

This document maps every dbt model in the legacy project to its equivalent in the Databricks lakehouse implementation, documenting what changed and why.

## Staging Models → Silver Layer

| dbt Model | Lakehouse Module | Changes |
|---|---|---|
| `stg_policies.sql` | `silver/clean_policies.py` | Identical logic. Added `_cleaned_timestamp` metadata. Schema enforcement via StructType. |
| `stg_claims.sql` | `silver/clean_claims.py` | Identical logic. Empty string `closed_date` now explicitly converted to NULL (Redshift handled implicitly). |
| `stg_premiums.sql` | `silver/clean_premiums.py` | Identical logic. Added metadata column. |
| `stg_properties.sql` | `silver/clean_properties.py` | Identical logic. Added metadata column. |

## Intermediate Models → Silver Layer (Absorbed)

| dbt Model | Lakehouse Module | Changes |
|---|---|---|
| `int_policy_claims.sql` | Absorbed into `gold/fact_claims.py` | Join logic moved into the Gold layer build to reduce intermediate materialization. |
| `int_premium_summary.sql` | Absorbed into `gold/dim_policy.py` | Aggregation logic inlined as `build_premium_summary()` function. |

**Rationale**: dbt's `ephemeral` materialization for intermediate models means they were never physically stored — they were inlined into downstream queries. In PySpark, we achieve the same by composing functions without writing intermediate tables.

## Mart Models → Gold Layer

| dbt Model | Lakehouse Module | Changes |
|---|---|---|
| `dim_policy.sql` | `gold/dim_policy.py` | Added full SCD Type 2 MERGE support via Delta Lake. dbt version only set initial SCD fields. |
| `dim_property.sql` | `gold/dim_property.py` | Identical logic with SCD Type 2 support. |
| `dim_coverage.sql` | `gold/dim_coverage.py` | Static reference table, minimal changes. |
| `dim_date.sql` | `gold/dim_date.py` | Replaced `dbt_utils.date_spine` with PySpark date generation. Added insurance-specific season flags. |
| `fact_claims.sql` | `gold/fact_claims.py` | Merged `int_policy_claims` join into build. Added `partitionBy("property_state")` for query optimization. |
| `fact_premiums.sql` | `gold/fact_premiums.py` | Identical logic. |

## dbt Macros → Python Functions

| dbt Macro | Python Equivalent | Location |
|---|---|---|
| `generate_surrogate_key()` | `generate_surrogate_key()` | `gold/dim_policy.py` |

## dbt Tests → Data Quality Framework

| dbt Test Type | Lakehouse Equivalent | Location |
|---|---|---|
| `unique` | `check_unique()` | `silver/utils/data_quality_checks.py` |
| `not_null` | `check_not_null()` | `silver/utils/data_quality_checks.py` |
| `accepted_values` | `check_accepted_values()` | `silver/utils/data_quality_checks.py` |
| `relationships` | `check_relationships()` | `silver/utils/data_quality_checks.py` |
| Custom SQL tests | pytest test functions | `tests/test_*.py` |

## dbt Documentation → Unity Catalog + Markdown

| dbt Feature | Lakehouse Equivalent |
|---|---|
| `description` in schema.yml | Unity Catalog `COMMENT` on tables/columns |
| `dbt docs generate` | Unity Catalog Explorer + `docs/data_dictionary.md` |
| dbt DAG visualization | Unity Catalog Lineage graph |

## Key Architectural Differences

**Materialization Strategy**
- dbt: `view` (staging), `ephemeral` (intermediate), `table` (marts)
- Lakehouse: Delta tables at every layer with explicit write modes

**Schema Enforcement**
- dbt/Redshift: Schema-on-write enforced by Redshift DDL
- Lakehouse: Schema defined via StructType, enforced at Silver layer, permissive at Bronze

**Incremental Processing**
- dbt: `incremental` materialization with `is_incremental()` macro
- Lakehouse: Delta Lake MERGE for upserts, Auto Loader for streaming ingestion

**Testing**
- dbt: YAML-based schema tests + custom SQL tests
- Lakehouse: Python-based DQ framework + pytest with PySpark fixtures

**Access Control**
- Redshift: SQL GRANT statements per schema/table
- Lakehouse: Unity Catalog RBAC with column-level masking and audit logging
