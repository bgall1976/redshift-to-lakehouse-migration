"""
Bronze Layer: Ingest raw policy data into Delta Lake.

Migrates the first step of the legacy pipeline where raw data was loaded
into Redshift staging tables via COPY command. In the lakehouse, we use
Auto Loader (cloudFiles) for production or batch reads for development.

Legacy equivalent: COPY raw.policies FROM 's3://...' CSV
Target table: fintech_catalog.bronze.raw_policies
"""

import argparse

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# ──────────────────────────────────────────────────────────────────────
# Schema definition (enforce on read for Bronze)
# ──────────────────────────────────────────────────────────────────────

RAW_POLICIES_SCHEMA = StructType(
    [
        StructField("policy_id", StringType(), False),
        StructField("policyholder_first_name", StringType(), True),
        StructField("policyholder_last_name", StringType(), True),
        StructField("policyholder_email", StringType(), True),
        StructField("property_id", StringType(), True),
        StructField("coverage_type_code", StringType(), True),
        StructField("effective_date", StringType(), True),
        StructField("expiration_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("annual_premium", DoubleType(), True),
        StructField("deductible", DoubleType(), True),
        StructField("coverage_limit", DoubleType(), True),
        StructField("agent_id", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]
)


# ──────────────────────────────────────────────────────────────────────
# Ingestion functions
# ──────────────────────────────────────────────────────────────────────


def add_metadata_columns(df: DataFrame, source_path: str) -> DataFrame:
    """Add Bronze layer metadata columns for lineage tracking.

    These columns replace the implicit metadata that Redshift COPY provided
    (load timestamps, source file tracking) with explicit, queryable fields.
    """
    return (
        df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_path))
        .withColumn("_batch_id", F.lit(F.current_timestamp().cast("long")))
    )


def ingest_batch(spark: SparkSession, source_path: str, target_table: str) -> int:
    """Batch ingest from CSV file to Bronze Delta table.

    Used for initial migration and development. Production would use
    Auto Loader (cloudFiles) for incremental ingestion.
    """
    logger.info(f"Reading source file: {source_path}")

    df_raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .schema(RAW_POLICIES_SCHEMA)
        .csv(source_path)
    )

    row_count = df_raw.count()
    logger.info(f"Read {row_count:,} rows from source")

    df_bronze = add_metadata_columns(df_raw, source_path)

    logger.info(f"Writing to Bronze table: {target_table}")
    (
        df_bronze.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

    logger.info(f"Successfully ingested {row_count:,} rows to {target_table}")
    return row_count


def ingest_autoloader(
    spark: SparkSession, source_path: str, target_table: str, checkpoint_path: str
) -> None:
    """Production incremental ingestion using Databricks Auto Loader.

    Auto Loader automatically detects new files in the source directory
    and processes them exactly once. This replaces scheduled Redshift COPY
    jobs with a continuous, incremental approach.
    """
    logger.info(f"Starting Auto Loader ingestion from: {source_path}")

    (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", checkpoint_path + "/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .schema(RAW_POLICIES_SCHEMA)
        .load(source_path)
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.lit(F.current_timestamp().cast("long")))
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    logger.info(f"Auto Loader ingestion complete for {target_table}")


# ──────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Bronze layer: Ingest raw policies")
    parser.add_argument("--source", required=True, help="Path to source CSV file or S3 directory")
    parser.add_argument(
        "--target", default="fintech_catalog.bronze.raw_policies", help="Target Delta table name"
    )
    parser.add_argument(
        "--mode", choices=["batch", "autoloader"], default="batch", help="Ingestion mode"
    )
    parser.add_argument(
        "--checkpoint",
        default="/tmp/checkpoints/bronze_policies",
        help="Checkpoint path for Auto Loader",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("bronze_ingest_policies")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalogExtension",
        )
        .getOrCreate()
    )

    if args.mode == "batch":
        rows = ingest_batch(spark, args.source, args.target)
        logger.info(f"Batch ingestion complete: {rows:,} rows")
    else:
        ingest_autoloader(spark, args.source, args.target, args.checkpoint)

    spark.stop()


if __name__ == "__main__":
    main()
