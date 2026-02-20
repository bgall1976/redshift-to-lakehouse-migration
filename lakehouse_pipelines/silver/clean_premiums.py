"""
Silver Layer: Clean and validate premium payment data.

Migrated from: legacy_dbt_project/models/staging/stg_premiums.sql
Source: fintech_catalog.bronze.raw_premiums
Target: fintech_catalog.silver.cleaned_premiums
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType, TimestampType
from loguru import logger


def transform_premiums(df_bronze: DataFrame) -> DataFrame:
    """Mirrors stg_premiums.sql transformation logic."""
    return (
        df_bronze
        .filter(F.col("premium_id").isNotNull())
        .filter(F.col("policy_id").isNotNull())
        .filter(F.col("amount") > 0)

        .withColumn("payment_date", F.col("payment_date").cast(DateType()))
        .withColumn("due_date", F.col("due_date").cast(DateType()))
        .withColumn("amount", F.col("amount").cast(DecimalType(12, 2)))
        .withColumn("payment_method", F.upper(F.trim(F.col("payment_method"))))
        .withColumn("payment_status", F.upper(F.trim(F.col("payment_status"))))
        .withColumn("billing_period", F.trim(F.col("billing_period")))
        .withColumn("period_start_date", F.col("period_start_date").cast(DateType()))
        .withColumn("period_end_date", F.col("period_end_date").cast(DateType()))
        .withColumn("created_at", F.col("created_at").cast(TimestampType()))
        .withColumn("_cleaned_timestamp", F.current_timestamp())
    )


def run(spark: SparkSession,
        source_table: str = "fintech_catalog.bronze.raw_premiums",
        target_table: str = "fintech_catalog.silver.cleaned_premiums") -> int:

    logger.info(f"Reading from Bronze: {source_table}")
    df_bronze = spark.read.table(source_table)

    df_silver = transform_premiums(df_bronze)
    silver_count = df_silver.count()

    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    logger.info(f"Silver premiums complete: {silver_count:,} rows")
    return silver_count


if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver_clean_premiums").getOrCreate()
    run(spark)
    spark.stop()
