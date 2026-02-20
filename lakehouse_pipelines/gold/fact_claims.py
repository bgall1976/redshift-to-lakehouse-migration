"""
Gold Layer: Claims fact table.

Migrated from: legacy_dbt_project/models/marts/fact_claims.sql
Sources: silver.cleaned_claims, silver.cleaned_policies, silver.cleaned_properties
Target: fintech_catalog.gold.fact_claims

Combines the logic from:
  - int_policy_claims.sql (intermediate join)
  - fact_claims.sql (final mart)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from loguru import logger


def build_fact_claims(df_claims: DataFrame, df_policies: DataFrame,
                      df_properties: DataFrame) -> DataFrame:
    """Build the claims fact table.

    Merges the intermediate int_policy_claims join with the final
    fact_claims mart into a single transformation.
    """

    # Step 1: Join claims to policies (replaces int_policy_claims.sql)
    df_enriched = (
        df_claims.alias("c")
        .join(
            df_policies.alias("p"),
            F.col("c.policy_id") == F.col("p.policy_id"),
            "inner"
        )
        .join(
            df_properties.alias("prop"),
            F.col("p.property_id") == F.col("prop.property_id"),
            "left"
        )
    )

    # Step 2: Select and compute final columns (replaces fact_claims.sql)
    df_fact = df_enriched.select(
        # Keys
        F.col("c.claim_id"),
        F.col("c.policy_id"),
        F.col("p.property_id"),
        F.col("p.coverage_type_code"),
        F.col("c.claim_date").alias("claim_date_key"),
        F.col("c.reported_date").alias("reported_date_key"),
        F.col("c.closed_date").alias("closed_date_key"),

        # Claim attributes
        F.col("c.claim_type"),
        F.col("c.claim_status"),
        F.col("c.cause_of_loss"),
        F.col("c.adjuster_id"),

        # Measures
        F.col("c.claim_amount"),
        F.col("c.approved_amount"),
        F.col("c.deductible_applied"),
        F.least(F.col("c.claim_amount"), F.col("p.coverage_limit")).alias("capped_claim_amount"),
        (F.col("c.approved_amount") - F.col("c.deductible_applied")).alias("net_claim_payout"),

        # Policy context
        F.col("p.annual_premium"),
        F.col("p.deductible").alias("policy_deductible"),
        F.col("p.coverage_limit"),
        (F.col("c.claim_amount") / F.when(F.col("p.annual_premium") == 0, None)
         .otherwise(F.col("p.annual_premium"))).alias("claim_to_premium_ratio"),

        # Timing measures
        F.datediff(F.col("c.reported_date"), F.col("c.claim_date")).alias("days_to_report"),
        F.datediff(F.col("c.closed_date"), F.col("c.reported_date")).alias("days_to_close"),

        # Property risk context
        F.col("prop.state").alias("property_state"),
        F.col("prop.flood_zone"),
        F.col("prop.wind_zone"),

        # Derived flags
        (F.col("c.claim_amount") > F.col("p.coverage_limit")).alias("exceeds_coverage_limit"),
        (F.datediff(F.col("c.reported_date"), F.col("c.claim_date")) > 30).alias("late_reported"),
        (F.col("c.claim_status") == "CLOSED").alias("is_closed"),
        (F.col("c.approved_amount") > 0).alias("is_paid"),
    )

    return df_fact


def run(spark: SparkSession,
        claims_table: str = "fintech_catalog.silver.cleaned_claims",
        policies_table: str = "fintech_catalog.silver.cleaned_policies",
        properties_table: str = "fintech_catalog.silver.cleaned_properties",
        target_table: str = "fintech_catalog.gold.fact_claims") -> int:

    logger.info("Building fact_claims...")

    df_claims = spark.read.table(claims_table)
    df_policies = spark.read.table(policies_table)
    df_properties = spark.read.table(properties_table)

    df_fact = build_fact_claims(df_claims, df_policies, df_properties)
    row_count = df_fact.count()

    (
        df_fact.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("property_state")
        .saveAsTable(target_table)
    )

    logger.info(f"Gold fact_claims complete: {row_count:,} rows")
    return row_count


if __name__ == "__main__":
    spark = SparkSession.builder.appName("gold_fact_claims").getOrCreate()
    run(spark)
    spark.stop()
