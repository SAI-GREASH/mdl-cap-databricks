
# ============================================================
# MDL-CAP: Gold Marketing Analytics
# Purpose: Calculate daily advertising performance KPIs
# Technology: Lakeflow Spark Declarative Pipelines
# ============================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Clean Silver source table
SILVER_TABLE = (
    "workspace.mdl_cap_silver.silver_campaign_performance"
)

# ============================================================
# Gold Materialized View: Daily Channel Performance
# ============================================================

@dp.materialized_view(
    name="workspace.mdl_cap_gold.gold_daily_channel_performance",
    comment="Daily marketing performance metrics by source",
    table_properties={"quality": "gold"}
)
def gold_daily_channel_performance():

    # Step 1: Read clean Silver data
    silver_df = spark.read.table(SILVER_TABLE)

    # Step 2: Aggregate campaign performance
    daily_df = (
        silver_df
        .groupBy("campaign_date", "source")
        .agg(
            F.count("*").alias("total_records"),
            F.countDistinct("campaign_id").alias("active_campaigns"),
            F.sum("impressions").alias("total_impressions"),
            F.sum("clicks").alias("total_clicks"),
            F.sum("conversions").alias("total_conversions"),
            F.sum("spend").alias("total_spend")
        )
    )

    # Step 3: Calculate marketing KPIs
    return (
        daily_df
        .withColumn(
            "ctr_pct",
            F.when(
                F.col("total_impressions") > 0,
                F.round(
                    F.col("total_clicks") * 100.0 /
                    F.col("total_impressions"), 2
                )
            )
        )
        .withColumn(
            "conversion_rate_pct",
            F.when(
                F.col("total_clicks") > 0,
                F.round(
                    F.col("total_conversions") * 100.0 /
                    F.col("total_clicks"), 2
                )
            )
        )
        .withColumn(
            "cpc",
            F.when(
                F.col("total_clicks") > 0,
                F.round(
                    F.col("total_spend") /
                    F.col("total_clicks"), 2
                )
            )
        )
        .withColumn(
            "cpa",
            F.when(
                F.col("total_conversions") > 0,
                F.round(
                    F.col("total_spend") /
                    F.col("total_conversions"), 2
                )
            )
        )
    )
