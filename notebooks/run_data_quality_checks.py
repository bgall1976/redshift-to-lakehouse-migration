# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Checks — Fintech Lakehouse
# MAGIC Runs DQ checks across Bronze, Silver, and Gold layers after pipeline execution.

# COMMAND ----------

# MAGIC %pip install loguru

# COMMAND ----------

dbutils.widgets.text("catalog", "fintech_catalog_dev")
dbutils.widgets.text("alert_on_failure", "true")

catalog = dbutils.widgets.get("catalog")
alert_on_failure = dbutils.widgets.get("alert_on_failure").lower() == "true"

print(f"Running DQ checks on catalog: {catalog}")

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import json

run_id = f"dq_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
results = []

def record_check(layer, table, check_name, passed, details=""):
    results.append({
        "run_id": run_id,
        "run_timestamp": datetime.now().isoformat(),
        "layer": layer,
        "table_name": table,
        "check_name": check_name,
        "passed": passed,
        "details": details
    })
    status = "✅ PASS" if passed else "❌ FAIL"
    print(f"  {status}: {layer}.{table} — {check_name} {details}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Checks

# COMMAND ----------

bronze_tables = ["policies", "claims", "premiums", "properties"]

for tbl in bronze_tables:
    full_name = f"{catalog}.bronze.raw_{tbl}"
    try:
        df = spark.table(full_name)
        count = df.count()
        record_check("bronze", f"raw_{tbl}", "table_exists", True, f"rows={count}")
        record_check("bronze", f"raw_{tbl}", "not_empty", count > 0, f"rows={count}")
    except Exception as e:
        record_check("bronze", f"raw_{tbl}", "table_exists", False, str(e)[:200])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Checks

# COMMAND ----------

silver_tables = ["clean_policies", "clean_claims", "clean_premiums", "clean_properties"]

for tbl in silver_tables:
    full_name = f"{catalog}.silver.{tbl}"
    try:
        df = spark.table(full_name)
        count = df.count()
        record_check("silver", tbl, "table_exists", True, f"rows={count}")
        record_check("silver", tbl, "not_empty", count > 0, f"rows={count}")

        # Check for null primary keys
        pk_col = df.columns[0]
        null_pks = df.filter(F.col(pk_col).isNull()).count()
        record_check("silver", tbl, f"not_null_{pk_col}", null_pks == 0, f"nulls={null_pks}")
    except Exception as e:
        record_check("silver", tbl, "table_exists", False, str(e)[:200])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Checks

# COMMAND ----------

gold_tables = ["dim_policy", "fact_claims", "fact_premiums"]

for tbl in gold_tables:
    full_name = f"{catalog}.gold.{tbl}"
    try:
        df = spark.table(full_name)
        count = df.count()
        record_check("gold", tbl, "table_exists", True, f"rows={count}")
        record_check("gold", tbl, "not_empty", count > 0, f"rows={count}")

        # Check for null surrogate keys
        pk_col = df.columns[0]
        null_pks = df.filter(F.col(pk_col).isNull()).count()
        record_check("gold", tbl, f"not_null_{pk_col}", null_pks == 0, f"nulls={null_pks}")
    except Exception as e:
        record_check("gold", tbl, "table_exists", False, str(e)[:200])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total = len(results)
passed = sum(1 for r in results if r["passed"])
failed = total - passed

print(f"\n{'='*60}")
print(f"DQ Run: {run_id}")
print(f"Total Checks: {total} | Passed: {passed} | Failed: {failed}")
print(f"{'='*60}")

if failed > 0 and alert_on_failure:
    failing = [r for r in results if not r["passed"]]
    print("\nFailing checks:")
    for f_check in failing:
        print(f"  - {f_check['layer']}.{f_check['table_name']}: {f_check['check_name']} — {f_check['details']}")
    dbutils.notebook.exit(json.dumps({"status": "FAILED", "passed": passed, "failed": failed}))
else:
    dbutils.notebook.exit(json.dumps({"status": "PASSED", "passed": passed, "failed": failed}))
