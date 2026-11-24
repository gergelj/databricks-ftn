# =============================================================================
# STREAMING PIPELINE - SILVER: cleaned and validated, still streaming.
#
# No joins with the restaurant catalog here: that is a batch source, refreshed by
# the batch pipeline. The batch gold tables join orders with restaurants.
# =============================================================================
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# -----------------------------------------------------------------------------
# Customers: full history of every customer (SCD Type 2) from the CDC feed.
# Moving to a new address creates a new row; the old row gets an __END_AT.
# A phone number change is applied in place (history is tracked on address/district only).
# A DELETE closes the customer's current row.
# Current rows: WHERE __END_AT IS NULL.
# -----------------------------------------------------------------------------
@dp.temporary_view()
def customer_changes():
    return (
        spark.readStream.table("bronze.customers_cdc")
        .where("customer_id IS NOT NULL")
        .select("customer_id", "first_name", "last_name", "email", "phone", "address", "district",
                "operation", "change_ts")
    )


dp.create_streaming_table(
    name="silver.customers",
    comment="Customer profiles with history (SCD Type 2). __START_AT/__END_AT show when each address was valid; "
            "current rows have __END_AT IS NULL.",
)

dp.create_auto_cdc_flow(
    name="customers_scd2",
    target="silver.customers",
    source="customer_changes",
    keys=["customer_id"],
    sequence_by="change_ts",
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation"],
    stored_as_scd_type=2,
    track_history_column_list=["address", "district"],
)

# -----------------------------------------------------------------------------
# Orders: validated orders with delivery time and lateness.
# The rules are defined once and used twice: expectations drop bad rows from
# silver.orders (and count them in the pipeline UI), and the quarantine table
# keeps exactly those rows. Nothing is silently lost.
# -----------------------------------------------------------------------------
ORDER_RULES = {
    "parsable_record": "_rescued_data IS NULL",          # a value didn't match its type (see _rescued_data)
    "valid_order_id": "order_id IS NOT NULL",
    "valid_restaurant": "restaurant_id IS NOT NULL",
    "positive_amount": "coalesce(total_amount, 0) > 0",
}


@dp.table(
    name="silver.orders",
    comment="Validated delivered orders with delivery time and lateness (one row per order).",
)
@dp.expect_all_or_drop(ORDER_RULES)
@dp.expect("delivered_after_order", "delivered_ts >= order_ts")     # warn only: keep, but count
def silver_orders():
    return spark.readStream.table("bronze.orders").select(
        "order_id",
        "order_ts",
        "promised_delivery_ts",
        "picked_up_ts",
        "delivered_ts",
        F.expr("timestampdiff(MINUTE, order_ts, delivered_ts)").alias("delivery_minutes"),
        F.expr("timestampdiff(MINUTE, promised_delivery_ts, delivered_ts)").alias("delay_minutes"),
        (F.col("delivered_ts") > F.col("promised_delivery_ts")).alias("is_late"),
        "restaurant_id",
        "customer_id",
        "delivery_district",
        "delivery_address",
        "courier_id",
        "items",
        F.size("items").alias("item_count"),
        "delivery_fee",
        "total_amount",
        "payment_method",
        "rating",
        "review_text",
        "_rescued_data",
        "_source_file",
        "_ingested_at",
    )


@dp.table(name="silver.orders_quarantine", comment="Order events that failed the silver data quality rules, kept for inspection.")
def silver_orders_quarantine():
    all_rules_pass = " AND ".join(f"({rule})" for rule in ORDER_RULES.values())
    # The name of the first rule the row breaks, e.g. "positive_amount".
    reason = F.lit(None)
    for name, rule in reversed(ORDER_RULES.items()):
        reason = F.when(~F.expr(rule), F.lit(name)).otherwise(reason)
    return (
        spark.readStream.table("bronze.orders")
        .where(f"NOT ({all_rules_pass})")
        .select(reason.alias("quarantine_reason"), "*")
    )


# -----------------------------------------------------------------------------
# GPS: drop pings outside Novi Sad (GPS glitches such as 0,0).
# -----------------------------------------------------------------------------
# Change Data Feed: the Lakebase synced table (resources/live_map.lakebase.yml) streams new pings from here
# into Postgres for the live courier map app.
@dp.table(
    name="silver.gps_events",
    comment="Courier GPS pings inside the Novi Sad area.",
    table_properties={"delta.enableChangeDataFeed": "true"},
)
@dp.expect_all_or_drop({
    "has_courier": "courier_id IS NOT NULL",
    "inside_novi_sad": "latitude BETWEEN 45.15 AND 45.35 AND longitude BETWEEN 19.65 AND 19.98",
})
def silver_gps_events():
    return (
        spark.readStream.table("bronze.gps_events")
        .select("courier_id", "order_id", "event_type", "event_ts", "latitude", "longitude", "speed_kmh", "_ingested_at")
    )
