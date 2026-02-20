# Databricks notebook source
# MAGIC %md
# MAGIC # Smoke Test: Verify Layer Access

# COMMAND ----------

dbutils.widgets.text("layer", "bronze")
dbutils.widgets.text("catalog", "fintech_catalog_dev")

layer = dbutils.widgets.get("layer")
catalog = dbutils.widgets.get("catalog")

print(f"Testing {layer} layer in {catalog}")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema_name = f"{catalog}.{layer}"
test_table = f"{schema_name}._smoke_test"

# 1. Verify schema access
print(f"Checking schema access: {schema_name}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {layer}")
print(f"  PASS: Schema accessible")

# COMMAND ----------

# 2. Verify write access
print(f"Testing write: {test_table}")
test_schema = StructType([
    StructField("test_id", StringType(), False),
    StructField("test_value", IntegerType(), True),
    StructField("test_timestamp", StringType(), True),
])
df = spark.createDataFrame([("smoke_test_1", 42, "2024-01-01T00:00:00")], test_schema)
df.write.format("delta").mode("overwrite").saveAsTable(test_table)
print(f"  PASS: Write succeeded")

# COMMAND ----------

# 3. Verify read access
print(f"Testing read: {test_table}")
df_read = spark.read.table(test_table)
count = df_read.count()
assert count == 1, f"Expected 1 row, got {count}"
print(f"  PASS: Read succeeded ({count} row)")

# COMMAND ----------

# 4. Cleanup
spark.sql(f"DROP TABLE IF EXISTS {test_table}")
print(f"  Cleanup done")

# 5. List existing tables
print(f"\nExisting tables in {schema_name}:")
tables = spark.sql(f"SHOW TABLES IN {schema_name}").collect()
for t in tables:
    if not t.tableName.startswith("_"):
        print(f"  - {t.tableName}")
if not tables:
    print("  (none)")

print(f"\n=== {layer} layer: ALL CHECKS PASSED ===")
