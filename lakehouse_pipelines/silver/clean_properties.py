"""
Silver Layer: Clean and validate property data.

Migrated from: legacy_dbt_project/models/staging/stg_properties.sql
Source: fintech_catalog.bronze.raw_properties
Target: fintech_catalog.silver.cleaned_properties
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType, IntegerType, TimestampType
from loguru import logger


def transform_properties(df_bronze: DataFrame) -> DataFrame:
    """Mirrors stg_properties.sql transformation logic."""
    return (
        df_bronze
        .filter(F.col("property_id").isNotNull())

        .withColumn("street_address", F.trim(F.col("street_address")))
        .withColumn("city", F.trim(F.col("city")))
        .withColumn("state", F.upper(F.trim(F.col("state"))))
        .withColumn("zip_code", F.trim(F.col("zip_code")))
        .withColumn("county", F.trim(F.col("county")))
        .withColumn("latitude", F.col("latitude").cast(DecimalType(10, 7)))
        .withColumn("longitude", F.col("longitude").cast(DecimalType(10, 7)))
        .withColumn("year_built", F.col("year_built").cast(IntegerType()))
        .withColumn("square_footage", F.col("square_footage").cast(IntegerType()))
        .withColumn("construction_type", F.upper(F.trim(F.col("construction_type"))))
        .withColumn("roof_type", F.upper(F.trim(F.col("roof_type"))))
        .withColumn("stories", F.col("stories").cast(IntegerType()))
        .withColumn("occupancy_type", F.upper(F.trim(F.col("occupancy_type"))))
        .withColumn("flood_zone", F.upper(F.trim(F.col("flood_zone"))))
        .withColumn("wind_zone", F.upper(F.trim(F.col("wind_zone"))))
        .withColumn("property_value", F.col("property_value").cast(DecimalType(14, 2)))
        .withColumn("created_at", F.col("created_at").cast(TimestampType()))
        .withColumn("updated_at", F.col("updated_at").cast(TimestampType()))
        .withColumn("_cleaned_timestamp", F.current_timestamp())
    )


def run(spark: SparkSession,
        source_table: str = "fintech_catalog.bronze.raw_properties",
        target_table: str = "fintech_catalog.silver.cleaned_properties") -> int:

    logger.info(f"Reading from Bronze: {source_table}")
    df_bronze = spark.read.table(source_table)

    df_silver = transform_properties(df_bronze)
    silver_count = df_silver.count()

    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    logger.info(f"Silver properties complete: {silver_count:,} rows")
    return silver_count


if __name__ == "__main__":
    spark = SparkSession.builder.appName("silver_clean_properties").getOrCreate()
    run(spark)
    spark.stop()
