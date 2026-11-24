# Databricks notebook source
# MAGIC %md
# MAGIC # 99 Reset: start the demo from scratch
# MAGIC
# MAGIC **Deletes all generated data** in the landing volume (orders, GPS, customer changes, restaurant files,
# MAGIC generator state), the notebook checkpoints and the sandbox tables.
# MAGIC
# MAGIC The `food_delivery_reset` job runs this notebook, stops the streaming pipeline, regenerates the history,
# MAGIC then **fully refreshes** the streaming pipeline (it keeps running afterwards) and the batch pipeline.
# MAGIC That empties and rebuilds every bronze/silver/gold table from the new files.
# MAGIC
# MAGIC Set `confirm` to `yes` to run it. Any other value fails the job before anything is deleted.

# COMMAND ----------

dbutils.widgets.text("catalog", "databricks_ftn_catalog")
dbutils.widgets.text("confirm", "no")
catalog = dbutils.widgets.get("catalog")

if dbutils.widgets.get("confirm").strip().lower() != "yes":
    raise ValueError("Reset not confirmed. Run again with confirm = yes. Nothing was deleted.")

# COMMAND ----------

raw = f"/Volumes/{catalog}/landing/raw"
for folder in ["orders", "gps", "customers_cdc", "restaurants", "_state"]:
    dbutils.fs.rm(f"{raw}/{folder}", recurse=True)
    print(f"deleted {raw}/{folder}")
for folder in ["orders", "gps", "customers_cdc", "restaurants"]:
    dbutils.fs.mkdirs(f"{raw}/{folder}")

for entry in dbutils.fs.ls(f"/Volumes/{catalog}/landing/checkpoints"):
    dbutils.fs.rm(entry.path, recurse=True)
    print(f"deleted {entry.path}")

for row in spark.sql(f"SHOW TABLES IN `{catalog}`.sandbox").collect():
    spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.sandbox.`{row.tableName}`")
    print(f"dropped sandbox.{row.tableName}")

print("Reset done. The next tasks regenerate the history and fully refresh the pipeline.")
