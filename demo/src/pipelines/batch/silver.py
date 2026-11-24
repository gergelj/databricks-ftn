# =============================================================================
# BATCH PIPELINE - SILVER: the current restaurant catalog.
# =============================================================================
from pyspark import pipelines as dp


# Each nightly CSV is a full snapshot; AUTO CDC keeps only the newest row per key (SCD Type 1).
@dp.temporary_view()
def restaurant_updates():
    return (
        spark.readStream.table("bronze.restaurants")
        .where("restaurant_id IS NOT NULL")
        .select("restaurant_id", "name", "cuisine", "district", "address", "latitude", "longitude",
                "avg_prep_minutes", "is_active", "_file_modified_at")
    )


dp.create_streaming_table(
    name="silver.restaurants",
    comment="Current restaurant catalog (SCD Type 1): one row per restaurant, newest nightly snapshot wins.",
)

dp.create_auto_cdc_flow(
    name="restaurants_scd1",
    target="silver.restaurants",
    source="restaurant_updates",
    keys=["restaurant_id"],
    sequence_by="_file_modified_at",
    except_column_list=["_file_modified_at"],
)
