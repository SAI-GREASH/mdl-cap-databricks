
# ============================================================
# MDL-CAP: Bronze Ingestion Pipeline
# Purpose: Incrementally ingest raw marketing CSV files
# Technology: Lakeflow Spark Declarative Pipelines
# ============================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# Unity Catalog Volume containing our four data sources
LANDING_PATH = "/Volumes/workspace/mdl_cap_bronze/landing/*/"

# Define the raw CSV schema
# Keep source values as strings in the Bronze layer
raw_schema = StructType([
    StructField("campaign_date", StringType(), True),
    StructField("source", StringType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("campaign_name", StringType(), True),
    StructField("impressions", StringType(), True),
    StructField("clicks", StringType(), True),
    StructField("conversions", StringType(), True),
    StructField("spend", StringType(), True),
])

# Define our managed Bronze streaming table
@dp.table(
    name="bronze_campaign_performance",
    comment="Raw marketing campaign data from four sources",
    table_properties={"quality": "bronze"}
)
def bronze_campaign_performance():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("pathGlobFilter", "*.csv")
        .option("cloudFiles.useStrictGlobber", "true")
        .schema(raw_schema)
        .load(LANDING_PATH)
        .select(
            "*",
            col("_metadata.file_path").alias("source_file_path"),
            current_timestamp().alias("ingested_at")
        )
    )
