"""
Bronze Layer: Ingest raw premium payment data into Delta Lake.

Legacy equivalent: COPY raw.premiums FROM 's3://...' CSV
Target table: fintech_catalog.bronze.raw_premiums
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

RAW_PREMIUMS_SCHEMA = StructType(
    [
        StructField("premium_id", StringType(), False),
        StructField("policy_id", StringType(), False),
        StructField("payment_date", StringType(), True),
        StructField("due_date", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("billing_period", StringType(), True),
        StructField("period_start_date", StringType(), True),
        StructField("period_end_date", StringType(), True),
        StructField("created_at", StringType(), True),
    ]
)


def add_metadata_columns(df: DataFrame, source_path: str) -> DataFrame:
    return (
        df.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_path))
        .withColumn("_batch_id", F.lit(F.current_timestamp().cast("long")))
    )


def ingest_batch(spark: SparkSession, source_path: str, target_table: str) -> int:
    df_raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .schema(RAW_PREMIUMS_SCHEMA)
        .csv(source_path)
    )

    row_count = df_raw.count()
    df_bronze = add_metadata_columns(df_raw, source_path)

    (
        df_bronze.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )
    return row_count


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--target", default="fintech_catalog.bronze.raw_premiums")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("bronze_ingest_premiums").getOrCreate()
    rows = ingest_batch(spark, args.source, args.target)
    print(f"Ingested {rows:,} premiums to Bronze")
    spark.stop()
