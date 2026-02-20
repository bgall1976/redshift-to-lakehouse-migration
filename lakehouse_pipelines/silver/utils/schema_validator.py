"""
Schema Validation Utilities for Silver Layer.

Provides schema contract enforcement between Bronze and Silver layers,
replacing the implicit schema guarantees that Redshift provided.
"""

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from loguru import logger


def validate_schema(df: DataFrame, expected_schema: StructType,
                    strict: bool = False) -> dict:
    """Validate a DataFrame's schema against an expected schema.

    Args:
        df: DataFrame to validate
        expected_schema: Expected StructType schema
        strict: If True, no extra columns allowed. If False, extra columns are okay.

    Returns:
        Dict with validation results
    """
    actual_fields = {f.name: f for f in df.schema.fields}
    expected_fields = {f.name: f for f in expected_schema.fields}

    missing = set(expected_fields.keys()) - set(actual_fields.keys())
    extra = set(actual_fields.keys()) - set(expected_fields.keys())

    type_mismatches = {}
    for name, expected_field in expected_fields.items():
        if name in actual_fields:
            actual_type = str(actual_fields[name].dataType)
            expected_type = str(expected_field.dataType)
            if actual_type != expected_type:
                type_mismatches[name] = {
                    "expected": expected_type,
                    "actual": actual_type,
                }

    passed = len(missing) == 0 and len(type_mismatches) == 0
    if strict:
        passed = passed and len(extra) == 0

    result = {
        "passed": passed,
        "missing_columns": list(missing),
        "extra_columns": list(extra),
        "type_mismatches": type_mismatches,
    }

    if passed:
        logger.info("Schema validation passed")
    else:
        logger.warning(f"Schema validation failed: {result}")

    return result


def compare_schemas(schema_a: StructType, schema_b: StructType,
                    label_a: str = "Source", label_b: str = "Target") -> dict:
    """Compare two schemas and report differences.

    Useful during migration to compare Redshift schema vs. lakehouse schema.
    """
    fields_a = {f.name: f for f in schema_a.fields}
    fields_b = {f.name: f for f in schema_b.fields}

    only_in_a = set(fields_a.keys()) - set(fields_b.keys())
    only_in_b = set(fields_b.keys()) - set(fields_a.keys())
    common = set(fields_a.keys()) & set(fields_b.keys())

    type_diffs = {}
    nullable_diffs = {}
    for name in common:
        if str(fields_a[name].dataType) != str(fields_b[name].dataType):
            type_diffs[name] = {
                label_a: str(fields_a[name].dataType),
                label_b: str(fields_b[name].dataType),
            }
        if fields_a[name].nullable != fields_b[name].nullable:
            nullable_diffs[name] = {
                label_a: fields_a[name].nullable,
                label_b: fields_b[name].nullable,
            }

    return {
        f"only_in_{label_a}": list(only_in_a),
        f"only_in_{label_b}": list(only_in_b),
        "type_differences": type_diffs,
        "nullable_differences": nullable_diffs,
        "compatible": len(only_in_a) == 0 and len(type_diffs) == 0,
    }
