# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Streaming, live
# MAGIC
# MAGIC Start the data generator first (`databricks bundle run data_generator -t dev`), so new files keep arriving.
# MAGIC
# MAGIC Run this notebook on the **classic cluster** (`databricks-ftn-uc-cluster`): continuously updating
# MAGIC `display()` of a stream needs a cluster that stays up. Serverless notebooks run streams with
# MAGIC `availableNow` triggers (see notebook 01).
# MAGIC
# MAGIC Stop the streams with the last cell (or "Interrupt") when you're done.

# COMMAND ----------

dbutils.widgets.text("catalog", "databricks_ftn_catalog")
catalog = dbutils.widgets.get("catalog")
raw = f"/Volumes/{catalog}/landing/raw"
checkpoints = f"/Volumes/{catalog}/landing/checkpoints"

# COMMAND ----------

# Files that arrived in the last 30 minutes
import time
recent = [f for f in dbutils.fs.ls(f"{raw}/orders") if f.modificationTime > (time.time() - 30 * 60) * 1000]
print(f"{len(recent)} order files arrived in the last 30 minutes")

# COMMAND ----------

# The SAME Auto Loader code as in notebook 01, only without trigger(availableNow=True): it never stops.
# schemaEvolutionMode=rescue: new fields go to _rescued_data instead of restarting the stream.
orders = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("cloudFiles.schemaLocation", f"{checkpoints}/live_orders/schema")
    .load(f"{raw}/orders/")
)

# COMMAND ----------

# Live counter: watch it go up every few seconds.
display(
    orders.selectExpr("count(*) AS orders_so_far", "max(delivered_ts) AS latest_delivery"),
    checkpointLocation=f"{checkpoints}/live_orders/counter_{int(time.time())}",
)

# COMMAND ----------

# Live per district: orders and late deliveries in 1-minute windows, newest first.
from pyspark.sql import functions as F

per_district = (
    orders
    .withColumn("delivered_ts", F.col("delivered_ts").cast("timestamp"))
    .groupBy(F.window("delivered_ts", "1 minute"), "delivery_district")
    .agg(
        F.count("*").alias("orders"),
        F.sum((F.col("delivered_ts") > F.col("promised_delivery_ts").cast("timestamp")).cast("int")).alias("late"),
    )
)
display(per_district.orderBy(F.desc("window")), checkpointLocation=f"{checkpoints}/live_orders/district_{int(time.time())}")

# COMMAND ----------

# Stop all streams of this notebook
for q in spark.streams.active:
    print(f"stopping {q.name or q.id}")
    q.stop()
