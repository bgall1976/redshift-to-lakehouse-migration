"""
Unit tests for Silver layer transformations.

Tests that PySpark transformations produce identical results to the
legacy dbt staging models they replace.
"""

from pyspark.sql import functions as F

from lakehouse_pipelines.silver.clean_claims import transform_claims
from lakehouse_pipelines.silver.clean_policies import transform_policies


class TestCleanPolicies:
    """Tests for Silver policy cleaning (replaces stg_policies.sql)."""

    def test_null_policy_ids_are_filtered(self, sample_policies):
        """Records with NULL policy_id should be removed (WHERE clause)."""
        result = transform_policies(sample_policies)
        null_count = result.filter(F.col("policy_id").isNull()).count()
        assert null_count == 0, "Null policy_id records should be filtered out"

    def test_row_count_after_filter(self, sample_policies):
        """Should have 2 rows after filtering out the null policy_id record."""
        result = transform_policies(sample_policies)
        assert result.count() == 2

    def test_names_are_trimmed(self, sample_policies):
        """TRIM() should remove leading/trailing whitespace from names."""
        result = transform_policies(sample_policies)
        john_row = result.filter(F.col("policy_id") == "POL-001").collect()[0]
        assert john_row["policyholder_first_name"] == "John"
        assert john_row["policyholder_last_name"] == "Smith"

    def test_full_name_is_constructed(self, sample_policies):
        """Full name should be first + last, trimmed."""
        result = transform_policies(sample_policies)
        john_row = result.filter(F.col("policy_id") == "POL-001").collect()[0]
        assert john_row["policyholder_full_name"] == "John Smith"

    def test_status_is_uppercased(self, sample_policies):
        """UPPER(TRIM(status)) should standardize status values."""
        result = transform_policies(sample_policies)
        john_row = result.filter(F.col("policy_id") == "POL-001").collect()[0]
        assert john_row["status"] == "ACTIVE"

    def test_coverage_code_is_uppercased(self, sample_policies):
        """Coverage type codes should be uppercased."""
        result = transform_policies(sample_policies)
        jane_row = result.filter(F.col("policy_id") == "POL-002").collect()[0]
        assert jane_row["coverage_type_code"] == "HO5"

    def test_dates_are_cast_to_date_type(self, sample_policies):
        """Date fields should be cast from string to DateType."""
        from pyspark.sql.types import DateType

        result = transform_policies(sample_policies)
        assert result.schema["effective_date"].dataType == DateType()
        assert result.schema["expiration_date"].dataType == DateType()

    def test_premium_is_cast_to_decimal(self, sample_policies):
        """Annual premium should be cast to DecimalType(12,2)."""
        from pyspark.sql.types import DecimalType

        result = transform_policies(sample_policies)
        assert result.schema["annual_premium"].dataType == DecimalType(12, 2)

    def test_metadata_column_added(self, sample_policies):
        """Silver layer should add _cleaned_timestamp metadata."""
        result = transform_policies(sample_policies)
        assert "_cleaned_timestamp" in result.columns


class TestCleanClaims:
    """Tests for Silver claims cleaning (replaces stg_claims.sql)."""

    def test_claim_types_are_uppercased(self, sample_claims):
        """Claim types should be standardized to uppercase."""
        result = transform_claims(sample_claims)
        types = [row["claim_type"] for row in result.select("claim_type").collect()]
        assert all(t == t.upper() for t in types)

    def test_empty_closed_date_becomes_null(self, sample_claims):
        """Empty string closed_date should be converted to NULL."""
        result = transform_claims(sample_claims)
        open_claim = result.filter(F.col("claim_id") == "CLM-002").collect()[0]
        assert open_claim["closed_date"] is None
