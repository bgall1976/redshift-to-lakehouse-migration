"""
Pytest configuration and shared fixtures for pipeline tests.
"""

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing with Delta Lake support."""
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("lakehouse_pipeline_tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_policies(spark):
    """Sample policy data for testing."""
    data = [
        (
            "POL-001",
            " John ",
            " Smith ",
            " john@test.com ",
            "PROP-001",
            "HO3",
            "2024-01-01",
            "2025-01-01",
            " active ",
            1200.50,
            1000.0,
            300000.0,
            "AGT-001",
            "ONLINE",
            "2023-12-15T00:00:00",
            "2024-01-01T00:00:00",
        ),
        (
            "POL-002",
            "Jane",
            "Doe",
            "jane@test.com",
            "PROP-002",
            "ho5",
            "2024-03-15",
            "2025-03-15",
            "CANCELLED",
            2500.00,
            2000.0,
            500000.0,
            "AGT-002",
            "AGENT",
            "2024-03-01T00:00:00",
            "2024-06-01T00:00:00",
        ),
        (
            None,
            "Bad",
            "Record",
            "bad@test.com",
            "PROP-003",
            "DP1",
            "2024-05-01",
            "2025-05-01",
            "ACTIVE",
            800.0,
            500.0,
            100000.0,
            "AGT-003",
            "ONLINE",
            "2024-04-20T00:00:00",
            "2024-05-01T00:00:00",
        ),
    ]
    schema = StructType(
        [
            StructField("policy_id", StringType(), True),
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
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_claims(spark):
    """Sample claims data for testing."""
    data = [
        (
            "CLM-001",
            "POL-001",
            "2024-06-15",
            "2024-06-16",
            "2024-08-01",
            "WIND_DAMAGE",
            "CLOSED",
            15000.0,
            12000.0,
            1000.0,
            "ADJ-001",
            "HURRICANE",
            "Wind damage claim",
            "2024-06-16T00:00:00",
            "2024-08-01T00:00:00",
        ),
        (
            "CLM-002",
            "POL-001",
            "2024-09-01",
            "2024-09-03",
            "",
            "water_damage",
            "OPEN",
            5000.0,
            0.0,
            0.0,
            "ADJ-002",
            "BURST_PIPE",
            "Pipe burst",
            "2024-09-03T00:00:00",
            "2024-09-10T00:00:00",
        ),
    ]
    schema = StructType(
        [
            StructField("claim_id", StringType(), True),
            StructField("policy_id", StringType(), True),
            StructField("claim_date", StringType(), True),
            StructField("reported_date", StringType(), True),
            StructField("closed_date", StringType(), True),
            StructField("claim_type", StringType(), True),
            StructField("claim_status", StringType(), True),
            StructField("claim_amount", DoubleType(), True),
            StructField("approved_amount", DoubleType(), True),
            StructField("deductible_applied", DoubleType(), True),
            StructField("adjuster_id", StringType(), True),
            StructField("cause_of_loss", StringType(), True),
            StructField("description", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)
