# =============================================================================
# STREAMING PIPELINE - BRONZE: the continuously arriving sources, exactly as they arrived.
#
# Every table here reads files incrementally with Auto Loader (format "cloudFiles"):
# the pipeline runs continuously and picks up each new file within seconds.
# It manages the checkpoint and the schema location for us. "landing_path" comes
# from the pipeline configuration (resources/food_delivery_streaming.pipeline.yml).
# =============================================================================
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

LANDING_PATH = spark.conf.get("landing_path")


def auto_loader(folder: str, schema_hints: str) -> DataFrame:
    """Incrementally read new JSON files from a landing folder, with ingestion metadata.

    schemaHints pin the types of the important columns; everything else is inferred.
    A value that doesn't match its type (e.g. total_amount = "twelve hundred") is not lost:
    it lands in _rescued_data. New fields (e.g. tip_amount) are added to the schema automatically.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", schema_hints)
        .load(f"{LANDING_PATH}/{folder}/")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )


# Delivered orders from the app (JSON lines, a new file every few seconds).
@dp.table(name="bronze.orders", comment="Raw delivered-order events from the delivery app (JSON), ingested with Auto Loader.")
def bronze_orders():
    return auto_loader(
        "orders",
        "order_id BIGINT, customer_id BIGINT, restaurant_id STRING, courier_id STRING, delivery_district STRING, "
        "delivery_address STRING, order_ts TIMESTAMP, promised_delivery_ts TIMESTAMP, picked_up_ts TIMESTAMP, "
        "delivered_ts TIMESTAMP, delivery_fee DOUBLE, total_amount DOUBLE, payment_method STRING, rating INT, "
        "review_text STRING",
    )


# Courier GPS pings (pickup, en route, delivered).
@dp.table(name="bronze.gps_events", comment="Raw courier GPS pings (JSON), ingested with Auto Loader.")
def bronze_gps_events():
    return auto_loader(
        "gps",
        "courier_id STRING, order_id BIGINT, event_type STRING, event_ts TIMESTAMP, "
        "latitude DOUBLE, longitude DOUBLE, speed_kmh DOUBLE",
    )


# Customer profile changes (CDC feed: INSERT / UPDATE / DELETE).
@dp.table(name="bronze.customers_cdc", comment="Raw customer change events (CDC) from the customer service (JSON).")
def bronze_customers_cdc():
    return auto_loader(
        "customers_cdc",
        "customer_id BIGINT, first_name STRING, last_name STRING, email STRING, phone STRING, "
        "address STRING, district STRING, operation STRING, change_ts TIMESTAMP",
    )
