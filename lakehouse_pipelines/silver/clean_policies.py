"""
Silver Layer: Clean and validate policy data.

Migrated from: legacy_dbt_project/models/staging/stg_policies.sql
Source: fintech_catalog.bronze.raw_policies
Target: fintech_catalog.silver.cleaned_policies

This module translates the dbt staging model into PySpark, preserving
identical business logic while adding enhanced validation and metadata.
"""

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType, TimestampType

from lakehouse_pipelines.silver.utils.data_quality_checks import (
    check_accepted_values,
    check_not_null,
    check_unique,
    log_quality_results,
)

# ──────────────────────────────────────────────────────────────────────
# Transformation logic (mirrors dbt stg_policies.sql)
# ──────────────────────────────────────────────────────────────────────

VALID_STATUSES = ["ACTIVE", "CANCELLED", "EXPIRED", "PENDING", "SUSPENDED"]
VALID_CHANNELS = ["ONLINE", "AGENT", "REFERRAL", "PARTNER", "DIRECT_MAIL"]


def transform_policies(df_bronze: DataFrame) -> DataFrame:
    """Apply cleaning and standardization rules from the legacy dbt model.

    Each transformation maps 1:1 to a line in stg_policies.sql:
      - TRIM() on string fields
      - CAST() on date and numeric fields
      - UPPER() on status/code fields
      - WHERE policy_id IS NOT NULL AND effective_date IS NOT NULL
    """
    df_cleaned = (
        df_bronze
        # Filter: equivalent to WHERE clause in dbt
        .filter(F.col("policy_id").isNotNull())
        .filter(F.col("effective_date").isNotNull())
        # String cleaning: TRIM
        .withColumn("policyholder_first_name", F.trim(F.col("policyholder_first_name")))
        .withColumn("policyholder_last_name", F.trim(F.col("policyholder_last_name")))
        .withColumn(
            "policyholder_full_name",
            F.concat_ws(
                " ",
                F.trim(F.col("policyholder_first_name")),
                F.trim(F.col("policyholder_last_name")),
            ),
        )
        .withColumn("policyholder_email", F.trim(F.col("policyholder_email")))
        .withColumn("agent_id", F.trim(F.col("agent_id")))
        .withColumn("channel", F.trim(F.col("channel")))
        # Type casting: CAST
        .withColumn("effective_date", F.col("effective_date").cast(DateType()))
        .withColumn("expiration_date", F.col("expiration_date").cast(DateType()))
        .withColumn("annual_premium", F.col("annual_premium").cast(DecimalType(12, 2)))
        .withColumn("deductible", F.col("deductible").cast(DecimalType(12, 2)))
        .withColumn("coverage_limit", F.col("coverage_limit").cast(DecimalType(14, 2)))
        .withColumn("created_at", F.col("created_at").cast(TimestampType()))
        .withColumn("updated_at", F.col("updated_at").cast(TimestampType()))
        # Standardization: UPPER
        .withColumn("status", F.upper(F.trim(F.col("status"))))
        .withColumn("coverage_type_code", F.upper(F.trim(F.col("coverage_type_code"))))
        # Silver metadata
        .withColumn("_cleaned_timestamp", F.current_timestamp())
    )

    return df_cleaned


# ──────────────────────────────────────────────────────────────────────
# Data quality checks (replaces dbt tests)
# ──────────────────────────────────────────────────────────────────────


def validate_policies(df: DataFrame) -> dict:
    """Run data quality checks equivalent to dbt schema tests.

    Replaces:
      - tests/schema.yml unique and not_null tests
      - tests/accepted_values tests
    """
    results = {}
    results["not_null_policy_id"] = check_not_null(df, "policy_id")
    results["not_null_effective_date"] = check_not_null(df, "effective_date")
    results["unique_policy_id"] = check_unique(df, "policy_id")
    results["valid_status"] = check_accepted_values(df, "status", VALID_STATUSES)
    results["valid_channel"] = check_accepted_values(df, "channel", VALID_CHANNELS)

    log_quality_results("silver.cleaned_policies", results)
    return results


# ──────────────────────────────────────────────────────────────────────
# Pipeline execution
# ──────────────────────────────────────────────────────────────────────


def run(
    spark: SparkSession,
    source_table: str = "fintech_catalog.bronze.raw_policies",
    target_table: str = "fintech_catalog.silver.cleaned_policies",
) -> int:
    """Execute the Silver layer policy cleaning pipeline."""

    logger.info(f"Reading from Bronze: {source_table}")
    df_bronze = spark.read.table(source_table)
    bronze_count = df_bronze.count()
    logger.info(f"Bronze row count: {bronze_count:,}")

    logger.info("Applying transformations...")
    df_silver = transform_policies(df_bronze)

    logger.info("Running data quality checks...")
    quality_results = validate_policies(df_silver)

    # Check for critical failures
    failures = {k: v for k, v in quality_results.items() if not v["passed"]}
    if failures:
        logger.warning(f"Data quality issues detected: {list(failures.keys())}")
        # In production, route failures to quarantine rather than failing
        # For now, log and continue

    silver_count = df_silver.count()
    logger.info(
        f"Silver row count: {silver_count:,} (dropped {bronze_count - silver_count:,} invalid rows)"
    )

    logger.info(f"Writing to Silver: {target_table}")
    (
        df_silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    logger.info(f"Silver layer complete: {silver_count:,} rows written")
    return silver_count


if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver_clean_policies").getOrCreate()
    run(spark)
    spark.stop()
