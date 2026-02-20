"""
Gold Layer: Policy dimension with SCD Type 2 support.

Migrated from: legacy_dbt_project/models/marts/dim_policy.sql
Sources: fintech_catalog.silver.cleaned_policies, cleaned_premiums
Target: fintech_catalog.gold.dim_policy
"""

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def generate_surrogate_key(
    df: DataFrame, key_columns: list[str], output_col: str = "surrogate_key"
) -> DataFrame:
    """Replaces dbt generate_surrogate_key macro."""
    concat_expr = F.concat_ws(
        "|", *[F.coalesce(F.col(c).cast("string"), F.lit("_null_")) for c in key_columns]
    )
    return df.withColumn(output_col, F.md5(concat_expr))


def build_premium_summary(df_premiums: DataFrame) -> DataFrame:
    """Migrated from: int_premium_summary.sql"""
    return df_premiums.groupBy("policy_id").agg(
        F.count("*").alias("total_premium_payments"),
        F.sum(F.when(F.col("payment_status") == "COMPLETED", F.col("amount")).otherwise(0))
        .cast(DecimalType(12, 2))
        .alias("total_premium_collected"),
        F.sum(F.when(F.col("payment_date") > F.col("due_date"), F.lit(1)).otherwise(0))
        .cast("int")
        .alias("late_premium_payments"),
    )


def build_dim_policy(df_policies: DataFrame, df_premium_summary: DataFrame) -> DataFrame:
    """Build policy dimension. Maps 1:1 to dbt dim_policy.sql mart."""
    df = (
        df_policies.alias("p")
        .join(df_premium_summary.alias("ps"), F.col("p.policy_id") == F.col("ps.policy_id"), "left")
        .select(
            F.col("p.policy_id"),
            F.col("p.policyholder_first_name"),
            F.col("p.policyholder_last_name"),
            F.col("p.policyholder_full_name"),
            F.col("p.policyholder_email"),
            F.col("p.property_id"),
            F.col("p.coverage_type_code"),
            F.col("p.effective_date"),
            F.col("p.expiration_date"),
            F.col("p.status"),
            F.col("p.annual_premium"),
            F.col("p.deductible"),
            F.col("p.coverage_limit"),
            F.col("p.agent_id"),
            F.col("p.channel"),
            F.coalesce(F.col("ps.total_premium_payments"), F.lit(0)).alias(
                "total_premium_payments"
            ),
            F.coalesce(F.col("ps.total_premium_collected"), F.lit(0)).alias(
                "total_premium_collected"
            ),
            F.coalesce(F.col("ps.late_premium_payments"), F.lit(0)).alias("late_premium_payments"),
            # Derived: policy_term_days
            F.datediff(F.col("p.expiration_date"), F.col("p.effective_date")).alias(
                "policy_term_days"
            ),
            # Derived: policy_status_category
            F.when(
                (F.col("p.status") == "ACTIVE") & (F.col("p.expiration_date") >= F.current_date()),
                F.lit("IN FORCE"),
            )
            .when(
                (F.col("p.status") == "ACTIVE") & (F.col("p.expiration_date") < F.current_date()),
                F.lit("EXPIRED"),
            )
            .when(F.col("p.status") == "CANCELLED", F.lit("CANCELLED"))
            .when(F.col("p.status") == "PENDING", F.lit("PENDING"))
            .otherwise(F.lit("UNKNOWN"))
            .alias("policy_status_category"),
            # SCD Type 2 fields
            F.col("p.updated_at").alias("effective_start_date"),
            F.lit(None).cast("timestamp").alias("effective_end_date"),
            F.lit(True).alias("is_current"),
            F.col("p.created_at"),
            F.col("p.updated_at"),
        )
    )

    # Add surrogate key
    df = generate_surrogate_key(df, ["policy_id", "updated_at"], "policy_sk")

    return df


def merge_scd2(spark: SparkSession, target_table: str, source_df: DataFrame) -> None:
    """Perform SCD Type 2 merge using Delta Lake.

    For records where tracked attributes have changed:
      1. Close the existing current record (set is_current=False, effective_end_date)
      2. Insert new version with updated attributes

    This is the lakehouse equivalent of the dbt snapshot strategy.
    """
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, target_table):
        logger.info(f"Target table {target_table} does not exist. Creating with initial load.")
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return

    target = DeltaTable.forName(spark, target_table)

    # Tracked columns that trigger a new version
    tracked_cols = [
        "status",
        "annual_premium",
        "deductible",
        "coverage_limit",
        "agent_id",
        "channel",
        "coverage_type_code",
    ]

    change_condition = " OR ".join([f"target.{c} != source.{c}" for c in tracked_cols])

    (
        target.alias("target")
        .merge(
            source_df.alias("source"),
            "target.policy_id = source.policy_id AND target.is_current = TRUE",
        )
        # Close existing record if attributes changed
        .whenMatchedUpdate(
            condition=change_condition,
            set={
                "is_current": F.lit(False),
                "effective_end_date": F.col("source.effective_start_date"),
            },
        )
        # Insert new records
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info(f"SCD Type 2 merge complete for {target_table}")


def run(
    spark: SparkSession,
    policies_table: str = "fintech_catalog.silver.cleaned_policies",
    premiums_table: str = "fintech_catalog.silver.cleaned_premiums",
    target_table: str = "fintech_catalog.gold.dim_policy",
) -> int:
    """Execute Gold layer dim_policy pipeline."""

    logger.info("Building dim_policy...")

    df_policies = spark.read.table(policies_table)
    df_premiums = spark.read.table(premiums_table)

    df_premium_summary = build_premium_summary(df_premiums)
    df_dim = build_dim_policy(df_policies, df_premium_summary)

    row_count = df_dim.count()
    logger.info(f"dim_policy row count: {row_count:,}")

    # For initial load, write directly. For incremental, use merge_scd2.
    df_dim.write.format("delta").mode("overwrite").saveAsTable(target_table)

    logger.info(f"Gold dim_policy complete: {row_count:,} rows")
    return row_count


if __name__ == "__main__":
    spark = SparkSession.builder.appName("gold_dim_policy").getOrCreate()
    run(spark)
    spark.stop()
