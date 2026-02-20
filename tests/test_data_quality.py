"""
Unit tests for the data quality check framework.

Validates that DQ checks correctly identify issues, replacing
dbt's built-in test framework.
"""

import pytest
from pyspark.sql import functions as F
from lakehouse_pipelines.silver.utils.data_quality_checks import (
    check_not_null,
    check_unique,
    check_accepted_values,
    check_relationships,
)


class TestNotNullCheck:

    def test_passes_when_no_nulls(self, spark):
        df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
        result = check_not_null(df, "id")
        assert result["passed"] is True
        assert result["null_count"] == 0

    def test_fails_when_nulls_present(self, spark):
        df = spark.createDataFrame([(1,), (None,), (3,)], ["id"])
        result = check_not_null(df, "id")
        assert result["passed"] is False
        assert result["null_count"] == 1


class TestUniqueCheck:

    def test_passes_when_all_unique(self, spark):
        df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
        result = check_unique(df, "id")
        assert result["passed"] is True

    def test_fails_when_duplicates_exist(self, spark):
        df = spark.createDataFrame([(1,), (1,), (3,)], ["id"])
        result = check_unique(df, "id")
        assert result["passed"] is False
        assert result["duplicate_count"] == 1


class TestAcceptedValuesCheck:

    def test_passes_when_all_valid(self, spark):
        df = spark.createDataFrame([("A",), ("B",), ("C",)], ["status"])
        result = check_accepted_values(df, "status", ["A", "B", "C"])
        assert result["passed"] is True

    def test_fails_when_invalid_values_present(self, spark):
        df = spark.createDataFrame([("A",), ("B",), ("X",)], ["status"])
        result = check_accepted_values(df, "status", ["A", "B", "C"])
        assert result["passed"] is False
        assert result["invalid_count"] == 1
        assert "X" in result["sample_invalid_values"]


class TestRelationshipsCheck:

    def test_passes_with_valid_references(self, spark):
        df = spark.createDataFrame([("POL-1",), ("POL-2",)], ["policy_id"])
        ref = spark.createDataFrame([("POL-1",), ("POL-2",), ("POL-3",)], ["policy_id"])
        result = check_relationships(df, "policy_id", ref, "policy_id")
        assert result["passed"] is True

    def test_fails_with_orphan_records(self, spark):
        df = spark.createDataFrame([("POL-1",), ("POL-999",)], ["policy_id"])
        ref = spark.createDataFrame([("POL-1",), ("POL-2",)], ["policy_id"])
        result = check_relationships(df, "policy_id", ref, "policy_id")
        assert result["passed"] is False
        assert result["orphan_count"] == 1
