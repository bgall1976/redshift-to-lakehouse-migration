"""
Silver Layer: Clean and validate claims data.

Migrated from: legacy_dbt_project/models/staging/stg_claims.sql
Source: fintech_catalog.bronze.raw_claims
Target: fintech_catalog.silver.cleaned_claims
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType, TimestampType
from loguru import logger


VALID_CLAIM_STATUSES = ["OPEN", "UNDER_REVIEW", "APPROVED", "DENIED", "CLOSED", "REOPENED"]
VALID_CLAIM_TYPES = [
    "PROPERTY_DAMAGE", "LIABILITY", "THEFT", "WATER_DAMAGE", "WIND_DAMAGE",
    "FIRE", "HAIL", "FLOOD", "MOLD", "OTHER"
]


def transform_claims(df_bronze: DataFrame) -> DataFrame:
    """Mirrors stg_claims.sql transformation logic."""
    return (
        df_bronze
        .filter(F.col("claim_id").isNotNull())
        .filter(F.col("policy_id").isNotNull())
        .filter(F.col("claim_amount") >= 0)

        .withColumn("claim_date", F.col("claim_date").cast(DateType()))
        .withColumn("reported_date", F.col("reported_date").cast(DateType()))
        .withColumn("closed_date",
                    F.when(F.col("closed_date") == "", None)
                     .otherwise(F.col("closed_date").cast(DateType())))
        .withColumn("claim_type", F.upper(F.trim(F.col("claim_type"))))
        .withColumn("claim_status", F.upper(F.trim(F.col("claim_status"))))
        .withColumn("claim_amount", F.col("claim_amount").cast(DecimalType(12, 2)))
        .withColumn("approved_amount", F.col("approved_amount").cast(DecimalType(12, 2)))
        .withColumn("deductible_applied", F.col("deductible_applied").cast(DecimalType(12, 2)))
        .withColumn("adjuster_id", F.trim(F.col("adjuster_id")))
        .withColumn("cause_of_loss", F.trim(F.col("cause_of_loss")))
        .withColumn("description", F.trim(F.col("description")))
        .withColumn("created_at", F.col("created_at").cast(TimestampType()))
        .withColumn("updated_at", F.col("updated_at").cast(TimestampType()))
        .withColumn("_cleaned_timestamp", F.current_timestamp())
    )


def run(spark: SparkSession,
        source_table: str = "fintech_catalog.bronze.raw_claims",
        target_table: str = "fintech_catalog.silver.cleaned_claims") -> int:

    logger.info(f"Reading from Bronze: {source_table}")
    df_bronze = spark.read.table(source_table)

    df_silver = transform_claims(df_bronze)
    silver_count = df_silver.count()

    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    logger.info(f"Silver claims complete: {silver_count:,} rows")
    return silver_count


if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver_clean_claims").getOrCreate()
    run(spark)
    spark.stop()
