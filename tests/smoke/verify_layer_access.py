"""
Smoke Test: Verify layer access after deployment.

Confirms that the deployed pipelines can read from and write to
all lakehouse layers. Triggered automatically by the CD pipeline
after each deployment.

Usage (via Databricks job):
    spark-submit verify_layer_access.py bronze
    spark-submit verify_layer_access.py silver
    spark-submit verify_layer_access.py gold
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def verify_layer(spark: SparkSession, layer: str, catalog: str = "fintech_catalog_dev"):
    """Verify read/write access to a lakehouse layer."""

    schema_name = f"{catalog}.{layer}"
    test_table = f"{schema_name}._smoke_test"

    print(f"=== Smoke Test: {layer} layer ===")

    # 1. Verify schema exists and is accessible
    print(f"  Checking schema access: {schema_name}")
    try:
        spark.sql(f"USE SCHEMA {schema_name}")
        print(f"  PASS: Schema {schema_name} is accessible")
    except Exception as e:
        print(f"  FAIL: Cannot access schema {schema_name}: {e}")
        sys.exit(1)

    # 2. Verify write access
    print(f"  Testing write access: {test_table}")
    try:
        test_schema = StructType(
            [
                StructField("test_id", StringType(), False),
                StructField("test_value", IntegerType(), True),
                StructField("test_timestamp", StringType(), True),
            ]
        )
        [("smoke_test_1", 42, F.current_timestamp())]

        df = spark.createDataFrame([("smoke_test_1", 42, "2024-01-01T00:00:00")], test_schema)
        df.write.format("delta").mode("overwrite").saveAsTable(test_table)
        print("  PASS: Write access verified")
    except Exception as e:
        print(f"  FAIL: Cannot write to {test_table}: {e}")
        sys.exit(1)

    # 3. Verify read access
    print(f"  Testing read access: {test_table}")
    try:
        df_read = spark.read.table(test_table)
        count = df_read.count()
        assert count == 1, f"Expected 1 row, got {count}"
        print(f"  PASS: Read access verified ({count} row)")
    except Exception as e:
        print(f"  FAIL: Cannot read from {test_table}: {e}")
        sys.exit(1)

    # 4. Clean up
    try:
        spark.sql(f"DROP TABLE IF EXISTS {test_table}")
        print(f"  Cleanup: Dropped {test_table}")
    except Exception:
        pass  # Non-critical

    # 5. List existing tables in the layer
    print(f"  Existing tables in {schema_name}:")
    try:
        tables = spark.sql(f"SHOW TABLES IN {schema_name}").collect()
        for table in tables:
            if not table.tableName.startswith("_"):
                print(f"    - {table.tableName}")
        if not tables:
            print("    (no tables yet)")
    except Exception:
        print("    (unable to list tables)")

    print(f"=== {layer} layer: ALL CHECKS PASSED ===\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: verify_layer_access.py <layer> [catalog]")
        print("  layer: bronze | silver | gold")
        sys.exit(1)

    layer = sys.argv[1].lower()
    catalog = sys.argv[2] if len(sys.argv) > 2 else "fintech_catalog_dev"

    if layer not in ("bronze", "silver", "gold"):
        print(f"Invalid layer: {layer}. Must be bronze, silver, or gold.")
        sys.exit(1)

    spark = SparkSession.builder.appName(f"smoke_test_{layer}").getOrCreate()
    verify_layer(spark, layer, catalog)
    spark.stop()
