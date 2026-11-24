# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Batch ingestion, Auto Loader and Delta time travel
# MAGIC
# MAGIC 1. Look at the landing zone (a Unity Catalog volume)
# MAGIC 2. Read the nightly `restaurants.csv` with schema inference
# MAGIC 3. Ingest the order history with **Auto Loader**: incremental file discovery and checkpoints
# MAGIC 4. Follow order **#1042** through bronze, silver and gold
# MAGIC 5. Delta Lake: history, time travel and "undo" an accidental `DELETE`
# MAGIC
# MAGIC Runs on serverless or on the classic cluster.

# COMMAND ----------

dbutils.widgets.text("catalog", "databricks_ftn_catalog")
catalog = dbutils.widgets.get("catalog")
raw = f"/Volumes/{catalog}/landing/raw"
checkpoints = f"/Volumes/{catalog}/landing/checkpoints"
spark.sql(f"USE CATALOG `{catalog}`")

# COMMAND ----------

# MAGIC %md ## 1. The landing zone
# MAGIC Files, not tables: a volume is governed storage for files, with the same permissions model as tables.

# COMMAND ----------

for folder in ["orders", "gps", "customers_cdc", "restaurants"]:
    files = dbutils.fs.ls(f"{raw}/{folder}")
    print(f"{folder:15s} {len(files):5d} files, e.g. {files[0].name if files else '-'}")

# COMMAND ----------

# MAGIC %md ## 2. Batch: read the restaurant catalog with schema inference

# COMMAND ----------

restaurants = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{raw}/restaurants/")
)
restaurants.printSchema()
display(restaurants)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Auto Loader: only new files, every time
# MAGIC
# MAGIC `cloudFiles` keeps track of the files it has already processed in the **checkpoint**.
# MAGIC With `trigger(availableNow=True)` the stream processes everything new and then stops: this is a batch job
# MAGIC written as a stream. Run the cell twice: the second run finds no new files and writes no rows.

# COMMAND ----------

def ingest_orders():
    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{checkpoints}/orders_autoloader/schema")
        .load(f"{raw}/orders/")
        .writeStream
        .option("checkpointLocation", f"{checkpoints}/orders_autoloader/checkpoint")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{catalog}.sandbox.orders_autoloader")
    )
    query.awaitTermination()
    progress = query.recentProgress
    print(f"rows ingested in this run: {sum(p['numInputRows'] for p in progress)}")
    print(f"rows in table: {spark.table(f'{catalog}.sandbox.orders_autoloader').count()}")

ingest_orders()

# COMMAND ----------

# Run it again: nothing new to do.
ingest_orders()

# COMMAND ----------

# What's in a checkpoint? Offsets (what was read) and commits (what was written).
display(dbutils.fs.ls(f"{checkpoints}/orders_autoloader/checkpoint"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Follow order #1042 through the medallion layers
# MAGIC bronze and silver come from the **streaming** pipeline, gold from the **batch** pipeline
# MAGIC (which joins the order with the restaurant catalog).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronze.orders (streaming)' AS layer, order_id, delivery_district, order_ts, delivered_ts, total_amount, _source_file AS detail
# MAGIC FROM bronze.orders WHERE order_id = 1042
# MAGIC UNION ALL
# MAGIC SELECT 'silver.orders (streaming)', order_id, delivery_district, order_ts, delivered_ts, total_amount,
# MAGIC        concat('restaurant ', restaurant_id, ', customer ', customer_id, ', late by ', delay_minutes, ' min')
# MAGIC FROM silver.orders WHERE order_id = 1042
# MAGIC UNION ALL
# MAGIC SELECT 'gold.deliveries (batch)', order_id, delivery_district, order_ts, delivered_ts, total_amount_rsd,
# MAGIC        concat(restaurant_name, ': ', review_text)
# MAGIC FROM gold.deliveries WHERE order_id = 1042

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD Type 2: Milica (customer 77) moved from Detelinara to Liman. Two rows, one is closed (__END_AT).
# MAGIC SELECT customer_id, first_name, last_name, district, address, phone, __START_AT, __END_AT
# MAGIC FROM silver.customers WHERE customer_id = 77 ORDER BY __START_AT

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Delta Lake: history, time travel, undo
# MAGIC A Delta table = Parquet files + a transaction log (`_delta_log`). Every write is a new version.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sandbox.orders_autoloader

# COMMAND ----------

# The current version, before anything goes wrong
version_before = spark.sql("DESCRIBE HISTORY sandbox.orders_autoloader").first()["version"]
print(f"current version: {version_before}")

# COMMAND ----------

# Oops: someone forgets the WHERE clause...
spark.sql("DELETE FROM sandbox.orders_autoloader")
display(spark.sql("SELECT count(*) AS rows_now FROM sandbox.orders_autoloader"))

# COMMAND ----------

# Time travel: the data is still there in the previous version.
display(spark.sql(f"SELECT count(*) AS rows_before_delete FROM sandbox.orders_autoloader VERSION AS OF {version_before}"))

# COMMAND ----------

# Undo
spark.sql(f"RESTORE TABLE sandbox.orders_autoloader TO VERSION AS OF {version_before}")
display(spark.sql("SELECT count(*) AS rows_after_restore FROM sandbox.orders_autoloader"))
