# =============================================================================
# BATCH PIPELINE - BRONZE: the nightly restaurant catalog.
#
# The batch pipeline runs on demand, on a daily schedule, or when a new restaurants
# file lands in the volume (file-arrival job). Auto Loader still makes sure each run
# only reads the files it has not seen before.
# =============================================================================
# dp = the Lakeflow pipelines Python API (module pyspark.pipelines, formerly called "dlt").
# Instead of writing CREATE TABLE / INSERT / checkpoints ourselves, we *declare* the tables we want
# with dp decorators, and the pipeline works out the order, runs them and tracks data quality.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING_PATH = spark.conf.get("landing_path")


# Nightly full snapshot of the restaurant catalog (CSV with a header).
# The file's modification time tells us which snapshot is the newest.
#
# @dp.table: "maintain a table called bronze.restaurants with whatever this function returns".
# The function returns a *streaming* DataFrame (readStream), so this becomes a streaming table:
# every row is processed exactly once, and each run only adds rows from new files.
# (For a query result that is recomputed on every run you would use @dp.materialized_view, see gold.sql.)
# We never call this function ourselves; the pipeline does, and draws it as a box in the graph.
@dp.table(name="bronze.restaurants", comment="Raw nightly restaurant catalog snapshots (CSV).")
def bronze_restaurants():
    return (
        # Auto Loader ("cloudFiles"): only reads files it has not seen before.
        # The pipeline keeps the checkpoint (which files are done) for us.
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")       # guess column types from the data...
        .option("cloudFiles.schemaHints",                     # ...but pin the important ones
                "restaurant_id STRING, name STRING, cuisine STRING, district STRING, address STRING, "
                "latitude DOUBLE, longitude DOUBLE, avg_prep_minutes INT, is_active BOOLEAN")
        .load(f"{LANDING_PATH}/restaurants/")
        .select(
            "*",                                                                     # all CSV columns, untouched (bronze = as received)
            F.col("_metadata.file_modification_time").alias("_file_modified_at"),   # when the file arrived: silver uses it to find the newest version
            F.col("_metadata.file_path").alias("_source_file"),                      # which file the row came from (audit trail)
            F.current_timestamp().alias("_ingested_at"),                             # when the pipeline read it
        )
    )
