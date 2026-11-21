
# ============================================================
# MDL-CAP: Silver Transformation Layer
# Purpose: Clean, validate, and standardize marketing data
# Technology: Lakeflow Spark Declarative Pipelines
# ============================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, expr, trim, lower, when
)

BRONZE_TABLE = (
    "workspace.mdl_cap_bronze.bronze_campaign_performance"
)

# ============================================================
# Step 1: Standardize Bronze data and validate business rules
# ============================================================

@dp.temporary_view(name="campaigns_standardized")
def campaigns_standardized():

    df = (
        spark.readStream.table(BRONZE_TABLE)
        .select(
            expr("try_cast(campaign_date AS DATE)").alias("campaign_date"),
            lower(trim(col("source"))).alias("source"),
            trim(col("campaign_id")).alias("campaign_id"),
            trim(col("campaign_name")).alias("campaign_name"),
            expr("try_cast(impressions AS BIGINT)").alias("impressions"),
            expr("try_cast(clicks AS BIGINT)").alias("clicks"),
            expr("try_cast(conversions AS BIGINT)").alias("conversions"),
            expr("try_cast(spend AS DECIMAL(18,2))").alias("spend"),
            col("source_file_path"),
            col("ingested_at")
        )
    )

    # Flag invalid records without discarding them
    return (
        df.withColumn(
            "quality_issue",
            when(
                col("campaign_date").isNull(),
                "invalid_campaign_date"
            )
            .when(
                col("source").isNull() |
                (~col("source").isin(
                    "google_ads",
                    "youtube_ads",
                    "facebook_ads",
                    "internal_marketing"
                )),
                "invalid_source"
            )
            .when(
                col("campaign_id").isNull() |
                (col("campaign_id") == "") |
                col("campaign_name").isNull() |
                (col("campaign_name") == ""),
                "missing_campaign_information"
            )
            .when(
                col("impressions").isNull() |
                col("clicks").isNull() |
                col("conversions").isNull() |
                col("spend").isNull(),
                "invalid_numeric_value"
            )
            .when(
                (col("impressions") < 0) |
                (col("clicks") < 0) |
                (col("conversions") < 0) |
                (col("spend") < 0),
                "negative_metric"
            )
            .when(
                col("clicks") > col("impressions"),
                "clicks_exceed_impressions"
            )
            .when(
                col("conversions") > col("clicks"),
                "conversions_exceed_clicks"
            )
            .when(
                col("source_file_path").isNull() |
                col("ingested_at").isNull(),
                "missing_ingestion_metadata"
            )
        )
    )


# ============================================================
# Step 2: Publish valid records to the Silver Delta table
# ============================================================

@dp.table(
    name="workspace.mdl_cap_silver.silver_campaign_performance",
    comment="Clean and validated marketing campaign data",
    table_properties={"quality": "silver"}
)
def silver_campaign_performance():

    return (
        spark.readStream.table("campaigns_standardized")
        .filter(col("quality_issue").isNull())
        .drop("quality_issue")
    )


# ============================================================
# Step 3: Preserve invalid records for investigation
# ============================================================

@dp.table(
    name="workspace.mdl_cap_silver.silver_campaign_quarantine",
    comment="Marketing campaign records failing quality checks",
    table_properties={"quality": "silver_quarantine"}
)
def silver_campaign_quarantine():

    return (
        spark.readStream.table("campaigns_standardized")
        .filter(col("quality_issue").isNotNull())
    )
