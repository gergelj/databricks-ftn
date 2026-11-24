# Databricks notebook source
# MAGIC %md
# MAGIC # Start the streaming pipeline and wait for data
# MAGIC
# MAGIC Used by the setup and reset jobs. A continuous pipeline can't be a job's pipeline task (it never
# MAGIC finishes), so this notebook starts it through the API and waits until `silver.orders` contains the
# MAGIC freshly ingested data. The batch pipeline, which reads `silver.orders`, runs after this task.
# MAGIC
# MAGIC With `full_refresh = true` all streaming tables are emptied and rebuilt from the landing files.

# COMMAND ----------

import time
from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineState

dbutils.widgets.text("catalog", "databricks_ftn_catalog")
dbutils.widgets.text("streaming_pipeline_id", "")
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"])
dbutils.widgets.text("timeout_minutes", "20")

catalog = dbutils.widgets.get("catalog")
pipeline_id = dbutils.widgets.get("streaming_pipeline_id")
full_refresh = dbutils.widgets.get("full_refresh") == "true"
timeout_s = float(dbutils.widgets.get("timeout_minutes")) * 60
if not pipeline_id:
    raise ValueError("streaming_pipeline_id is empty")

w = WorkspaceClient()

# COMMAND ----------

pipeline = w.pipelines.get(pipeline_id)
active = pipeline.state not in (PipelineState.IDLE, PipelineState.FAILED, None)
started_at_ms = int(time.time() * 1000)
update_id = None

if active and full_refresh:
    print(f"{pipeline.name} is {pipeline.state.value}: stopping it for the full refresh")
    w.pipelines.stop(pipeline_id).result(timeout=timedelta(minutes=10))
    active = False

if active:
    print(f"{pipeline.name} is already {pipeline.state.value}: it will pick up the new files by itself")
else:
    update_id = w.pipelines.start_update(pipeline_id, full_refresh=full_refresh).update_id
    print(f"Started update {update_id} of {pipeline.name} (full_refresh={full_refresh})")

# COMMAND ----------

orders_table = f"`{catalog}`.silver.orders"
# If we started the update, wait for rows it ingested; if it was already running, any data will do.
ready_condition = f"_ingested_at >= timestamp_millis({started_at_ms})" if update_id else "true"
deadline = time.time() + timeout_s
while True:
    if update_id:
        state = w.pipelines.get_update(pipeline_id, update_id).update.state.value
        if state in ("FAILED", "CANCELED"):
            raise RuntimeError(f"Streaming pipeline update {update_id} ended with {state}. Check the pipeline event log.")
    else:
        state = "RUNNING"
    try:
        fresh = spark.sql(
            f"SELECT count(*) AS n FROM {orders_table} WHERE {ready_condition}"
        ).first()["n"]
    except Exception as e:  # table not created yet, or being reset by the full refresh
        fresh = 0
        print(f"silver.orders not readable yet ({type(e).__name__})")
    if fresh > 0 and spark.catalog.tableExists(f"{catalog}.silver.customers"):
        print(f"silver.orders has {fresh} newly ingested orders (update state: {state})")
        break
    if time.time() > deadline:
        raise TimeoutError(f"No new data in silver.orders after {timeout_s / 60:.0f} minutes (update state: {state})")
    print(f"waiting... (update state: {state})")
    time.sleep(20)

# Give the customer and quarantine flows a moment to finish their first micro-batch too.
time.sleep(30)
print("Streaming pipeline is up. The batch pipeline can run now.")
