"""
Data Quality Check Framework for Silver Layer.

Replaces dbt's built-in schema tests (unique, not_null, accepted_values,
relationships) with PySpark equivalents that produce structured results
for monitoring and alerting.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Any
from loguru import logger


def check_not_null(df: DataFrame, column: str) -> dict:
    """Equivalent to dbt not_null test."""
    null_count = df.filter(F.col(column).isNull()).count()
    total_count = df.count()
    return {
        "check": "not_null",
        "column": column,
        "passed": null_count == 0,
        "null_count": null_count,
        "total_count": total_count,
        "null_percentage": round(null_count / max(total_count, 1) * 100, 2),
    }


def check_unique(df: DataFrame, column: str) -> dict:
    """Equivalent to dbt unique test."""
    total_count = df.count()
    distinct_count = df.select(column).distinct().count()
    duplicate_count = total_count - distinct_count
    return {
        "check": "unique",
        "column": column,
        "passed": duplicate_count == 0,
        "duplicate_count": duplicate_count,
        "total_count": total_count,
        "distinct_count": distinct_count,
    }


def check_accepted_values(df: DataFrame, column: str, accepted: list[str]) -> dict:
    """Equivalent to dbt accepted_values test."""
    invalid = df.filter(~F.col(column).isin(accepted))
    invalid_count = invalid.count()
    total_count = df.count()

    # Capture sample of invalid values for debugging
    sample_values = []
    if invalid_count > 0:
        sample_values = [
            row[column] for row in invalid.select(column).distinct().limit(10).collect()
        ]

    return {
        "check": "accepted_values",
        "column": column,
        "passed": invalid_count == 0,
        "invalid_count": invalid_count,
        "total_count": total_count,
        "accepted_values": accepted,
        "sample_invalid_values": sample_values,
    }


def check_relationships(df: DataFrame, column: str,
                        ref_df: DataFrame, ref_column: str) -> dict:
    """Equivalent to dbt relationships test (referential integrity)."""
    orphans = df.join(ref_df, df[column] == ref_df[ref_column], "left_anti")
    orphan_count = orphans.count()
    total_count = df.count()
    return {
        "check": "relationships",
        "column": column,
        "ref_column": ref_column,
        "passed": orphan_count == 0,
        "orphan_count": orphan_count,
        "total_count": total_count,
    }


def check_row_count_range(df: DataFrame, min_rows: int, max_rows: int) -> dict:
    """Check that row count falls within an expected range."""
    count = df.count()
    return {
        "check": "row_count_range",
        "passed": min_rows <= count <= max_rows,
        "actual_count": count,
        "expected_min": min_rows,
        "expected_max": max_rows,
    }


def check_no_duplicates_on_composite_key(df: DataFrame, key_columns: list[str]) -> dict:
    """Check uniqueness on a composite key (multiple columns)."""
    total = df.count()
    distinct = df.select(key_columns).distinct().count()
    duplicates = total - distinct
    return {
        "check": "composite_key_unique",
        "columns": key_columns,
        "passed": duplicates == 0,
        "duplicate_count": duplicates,
        "total_count": total,
    }


def log_quality_results(table_name: str, results: dict) -> None:
    """Log data quality check results with structured output."""
    passed = sum(1 for r in results.values() if r["passed"])
    failed = sum(1 for r in results.values() if not r["passed"])
    total = len(results)

    logger.info(f"Data Quality Results for {table_name}: {passed}/{total} passed, {failed}/{total} failed")

    for check_name, result in results.items():
        if result["passed"]:
            logger.info(f"  PASS: {check_name}")
        else:
            logger.warning(f"  FAIL: {check_name} — {result}")
