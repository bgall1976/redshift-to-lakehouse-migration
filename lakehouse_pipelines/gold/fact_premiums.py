"""
Gold Layer: Premium payments fact table.

Migrated from: legacy_dbt_project/models/marts/fact_premiums.sql
Sources: silver.cleaned_premiums, silver.cleaned_policies
Target: fintech_catalog.gold.fact_premiums
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from loguru import logger


def build_fact_premiums(df_premiums: DataFrame, df_policies: DataFrame) -> DataFrame:
    """Build premium fact table. Maps 1:1 to dbt fact_premiums.sql."""

    return (
        df_premiums.alias("pr")
        .join(
            df_policies.select(
                "policy_id", "property_id", "coverage_type_code", "agent_id", "channel"
            ).alias("p"),
            F.col("pr.policy_id") == F.col("p.policy_id"),
            "left"
        )
        .select(
            F.col("pr.premium_id"),
            F.col("pr.policy_id"),
            F.col("p.property_id"),
            F.col("p.coverage_type_code"),
            F.col("pr.payment_date").alias("payment_date_key"),
            F.col("pr.due_date").alias("due_date_key"),
            F.col("pr.period_start_date"),
            F.col("pr.period_end_date"),
            F.col("pr.payment_method"),
            F.col("pr.payment_status"),
            F.col("pr.billing_period"),
            F.col("p.agent_id"),
            F.col("p.channel"),
            F.col("pr.amount").alias("premium_amount"),
            F.when(F.col("pr.payment_status") == "COMPLETED", F.col("pr.amount"))
             .otherwise(0).alias("collected_amount"),
            F.when(F.col("pr.payment_status") == "FAILED", F.col("pr.amount"))
             .otherwise(0).alias("failed_amount"),
            F.datediff(F.col("pr.payment_date"), F.col("pr.due_date")).alias("days_from_due"),
            (F.col("pr.payment_date") > F.col("pr.due_date")).alias("is_late_payment"),
            (F.col("pr.payment_status") == "COMPLETED").alias("is_collected"),
            (F.col("pr.payment_status") == "FAILED").alias("is_failed"),
        )
    )


def run(spark: SparkSession,
        premiums_table: str = "fintech_catalog.silver.cleaned_premiums",
        policies_table: str = "fintech_catalog.silver.cleaned_policies",
        target_table: str = "fintech_catalog.gold.fact_premiums") -> int:

    logger.info("Building fact_premiums...")
    df_premiums = spark.read.table(premiums_table)
    df_policies = spark.read.table(policies_table)

    df_fact = build_fact_premiums(df_premiums, df_policies)
    row_count = df_fact.count()

    df_fact.write.format("delta").mode("overwrite").saveAsTable(target_table)

    logger.info(f"Gold fact_premiums complete: {row_count:,} rows")
    return row_count


if __name__ == "__main__":
    spark = SparkSession.builder.appName("gold_fact_premiums").getOrCreate()
    run(spark)
    spark.stop()
