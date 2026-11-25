# =============================================================================
# BATCH PIPELINE - SILVER: the current restaurant catalog.
#
# Bronze keeps every nightly snapshot (the same restaurant appears once per file).
# Silver keeps ONE row per restaurant: the newest version. This is SCD Type 1
# ("slowly changing dimension, type 1"): overwrite, no history.
#
# Three dp building blocks, in the order the data flows:
#   1. @dp.temporary_view         a helper step (filter + pick columns), private to this pipeline
#   2. dp.create_streaming_table  the empty target table we want to end up with
#   3. dp.create_auto_cdc_flow    the rule that keeps the target up to date from the helper
# =============================================================================
from pyspark import pipelines as dp  # Lakeflow pipelines Python API (formerly "dlt"), see bronze.py


# 1. @dp.temporary_view: like @dp.table, but nothing is stored and it is not published to
#    Unity Catalog. It only exists inside the pipeline, as an intermediate step in the graph.
#    readStream.table(...) = read only the bronze rows that are new since the last run.
@dp.temporary_view()
def restaurant_updates():
    return (
        spark.readStream.table("bronze.restaurants")
        .where("restaurant_id IS NOT NULL")                 # a row without an ID can't be matched to a restaurant
        .select("restaurant_id", "name", "cuisine", "district", "address", "latitude", "longitude",
                "avg_prep_minutes", "is_active", "_file_modified_at")
    )


# 2. dp.create_streaming_table: declares the target table. There is no function body,
#    because the table is not filled by a query: it is filled by the AUTO CDC flow below.
dp.create_streaming_table(
    name="silver.restaurants",
    comment="Current restaurant catalog (SCD Type 1): one row per restaurant, newest nightly snapshot wins.",
)

# 3. dp.create_auto_cdc_flow: "apply these changes to that table". It does the
#    "update if the restaurant exists, insert if it is new" logic that you would otherwise
#    write by hand as a MERGE statement, and it copes with rows arriving out of order.
dp.create_auto_cdc_flow(
    name="restaurants_scd1",                  # name of this flow (shown in the pipeline UI)
    target="silver.restaurants",              # table to keep up to date (declared in step 2)
    source="restaurant_updates",              # where the changes come from (step 1)
    keys=["restaurant_id"],                   # what makes two rows "the same restaurant"
    sequence_by="_file_modified_at",          # which version is newer: the later file wins
    except_column_list=["_file_modified_at"], # helper column, not needed in the final table
    # stored_as_scd_type is not set, so the default applies: SCD Type 1 (keep the latest only).
    # The streaming pipeline uses stored_as_scd_type=2 for customers, to keep their address history.
)
