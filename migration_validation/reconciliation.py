"""
Migration Validation: Reconciliation between Redshift (legacy) and Lakehouse (target).

Compares row counts, aggregate values, and distributions between the
legacy dbt/Redshift output and the new Databricks lakehouse output
to ensure data integrity during migration.

Usage:
    python reconciliation.py --legacy-config config/redshift.yml --lakehouse-config config/databricks.yml
"""

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass
class ReconciliationResult:
    """Result of a single reconciliation check."""

    table_name: str
    check_type: str
    passed: bool
    legacy_value: any
    lakehouse_value: any
    difference: float | None = None
    tolerance: float = 0.0
    details: str = ""


class MigrationReconciler:
    """Compare legacy Redshift output with lakehouse output."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.results: list[ReconciliationResult] = []

    def compare_row_counts(
        self, legacy_df: DataFrame, lakehouse_df: DataFrame, table_name: str
    ) -> ReconciliationResult:
        """Check 1: Row counts must match exactly."""
        legacy_count = legacy_df.count()
        lakehouse_count = lakehouse_df.count()

        result = ReconciliationResult(
            table_name=table_name,
            check_type="row_count",
            passed=legacy_count == lakehouse_count,
            legacy_value=legacy_count,
            lakehouse_value=lakehouse_count,
            difference=abs(legacy_count - lakehouse_count),
            details=f"Legacy: {legacy_count:,} | Lakehouse: {lakehouse_count:,}",
        )
        self.results.append(result)
        return result

    def compare_aggregates(
        self,
        legacy_df: DataFrame,
        lakehouse_df: DataFrame,
        table_name: str,
        numeric_columns: list[str],
        tolerance: float = 0.0001,
    ) -> list[ReconciliationResult]:
        """Check 2: SUM of numeric columns must match within tolerance."""
        results = []
        for col_name in numeric_columns:
            legacy_sum = legacy_df.agg(F.sum(col_name)).collect()[0][0] or 0
            lakehouse_sum = lakehouse_df.agg(F.sum(col_name)).collect()[0][0] or 0

            if legacy_sum == 0 and lakehouse_sum == 0:
                pct_diff = 0
            elif legacy_sum == 0:
                pct_diff = float("inf")
            else:
                pct_diff = abs(legacy_sum - lakehouse_sum) / abs(legacy_sum)

            result = ReconciliationResult(
                table_name=table_name,
                check_type=f"aggregate_sum_{col_name}",
                passed=pct_diff <= tolerance,
                legacy_value=float(legacy_sum),
                lakehouse_value=float(lakehouse_sum),
                difference=pct_diff,
                tolerance=tolerance,
                details=f"SUM({col_name}): Legacy={legacy_sum:.2f} | Lakehouse={lakehouse_sum:.2f} | Diff={pct_diff:.6%}",
            )
            results.append(result)
            self.results.append(result)

        return results

    def compare_distributions(
        self,
        legacy_df: DataFrame,
        lakehouse_df: DataFrame,
        table_name: str,
        categorical_columns: list[str],
    ) -> list[ReconciliationResult]:
        """Check 3: Value distributions for categorical columns must match."""
        results = []
        for col_name in categorical_columns:
            legacy_dist = legacy_df.groupBy(col_name).count().orderBy(col_name).collect()
            lakehouse_dist = lakehouse_df.groupBy(col_name).count().orderBy(col_name).collect()

            legacy_dict = {str(row[col_name]): row["count"] for row in legacy_dist}
            lakehouse_dict = {str(row[col_name]): row["count"] for row in lakehouse_dist}

            passed = legacy_dict == lakehouse_dict
            mismatches = {}
            all_keys = set(legacy_dict.keys()) | set(lakehouse_dict.keys())
            for key in all_keys:
                l_val = legacy_dict.get(key, 0)
                lh_val = lakehouse_dict.get(key, 0)
                if l_val != lh_val:
                    mismatches[key] = {"legacy": l_val, "lakehouse": lh_val}

            result = ReconciliationResult(
                table_name=table_name,
                check_type=f"distribution_{col_name}",
                passed=passed,
                legacy_value=legacy_dict,
                lakehouse_value=lakehouse_dict,
                details=f"Mismatches: {mismatches}" if mismatches else "Distributions match",
            )
            results.append(result)
            self.results.append(result)

        return results

    def compare_schemas(
        self, legacy_df: DataFrame, lakehouse_df: DataFrame, table_name: str
    ) -> ReconciliationResult:
        """Check 4: Schema compatibility."""
        legacy_cols = set(legacy_df.columns)
        lakehouse_cols = set(lakehouse_df.columns)

        # Ignore metadata columns added by lakehouse
        metadata_cols = {"_ingestion_timestamp", "_source_file", "_batch_id", "_cleaned_timestamp"}
        lakehouse_cols_filtered = lakehouse_cols - metadata_cols

        missing = legacy_cols - lakehouse_cols_filtered
        extra = lakehouse_cols_filtered - legacy_cols

        result = ReconciliationResult(
            table_name=table_name,
            check_type="schema_compatibility",
            passed=len(missing) == 0,
            legacy_value=sorted(legacy_cols),
            lakehouse_value=sorted(lakehouse_cols_filtered),
            details=f"Missing in lakehouse: {missing} | Extra in lakehouse: {extra}",
        )
        self.results.append(result)
        return result

    def generate_report(self) -> str:
        """Generate a human-readable reconciliation report."""
        lines = [
            "=" * 80,
            "MIGRATION RECONCILIATION REPORT",
            "=" * 80,
            "",
        ]

        passed = sum(1 for r in self.results if r.passed)
        failed = sum(1 for r in self.results if not r.passed)
        total = len(self.results)

        lines.append(f"Summary: {passed}/{total} checks passed, {failed}/{total} failed")
        lines.append(f"Overall status: {'PASS' if failed == 0 else 'FAIL'}")
        lines.append("")

        for result in self.results:
            status = "PASS" if result.passed else "FAIL"
            lines.append(f"  [{status}] {result.table_name}.{result.check_type}")
            lines.append(f"         {result.details}")
            lines.append("")

        return "\n".join(lines)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("migration_reconciliation").getOrCreate()

    reconciler = MigrationReconciler(spark)

    # Example usage (would be parameterized in production)
    # legacy_df = spark.read.jdbc(redshift_url, "marts.fact_claims", properties=redshift_props)
    # lakehouse_df = spark.read.table("fintech_catalog.gold.fact_claims")
    # reconciler.compare_row_counts(legacy_df, lakehouse_df, "fact_claims")
    # reconciler.compare_aggregates(legacy_df, lakehouse_df, "fact_claims", ["claim_amount", "approved_amount"])
    # print(reconciler.generate_report())

    print("Reconciliation module ready. Import and use MigrationReconciler class.")
    spark.stop()
