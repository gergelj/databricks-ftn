# =============================================================================
# BATCH PIPELINE - BRONZE: the nightly restaurant catalog.
#
# The batch pipeline runs on demand, on a daily schedule, or when a new restaurants
# file lands in the volume (file-arrival job). Auto Loader still makes sure each run
# only reads the files it has not seen before.
# =============================================================================
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING_PATH = spark.conf.get("landing_path")


# Nightly full snapshot of the restaurant catalog (CSV with a header).
# The file's modification time tells us which snapshot is the newest.
@dp.table(name="bronze.restaurants", comment="Raw nightly restaurant catalog snapshots (CSV).")
def bronze_restaurants():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints",
                "restaurant_id STRING, name STRING, cuisine STRING, district STRING, address STRING, "
                "latitude DOUBLE, longitude DOUBLE, avg_prep_minutes INT, is_active BOOLEAN")
        .load(f"{LANDING_PATH}/restaurants/")
        .select(
            "*",
            F.col("_metadata.file_modification_time").alias("_file_modified_at"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
