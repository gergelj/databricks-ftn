# Databricks notebook source
# MAGIC %md
# MAGIC # Stop the streaming pipeline
# MAGIC
# MAGIC Used by the reset job before it deletes the landing files. Also handy after a dry run:
# MAGIC a continuous pipeline keeps running (and costs serverless compute) until it is stopped.

# COMMAND ----------

from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineState

dbutils.widgets.text("streaming_pipeline_id", "")
pipeline_id = dbutils.widgets.get("streaming_pipeline_id")
if not pipeline_id:
    raise ValueError("streaming_pipeline_id is empty")

w = WorkspaceClient()
pipeline = w.pipelines.get(pipeline_id)
if pipeline.state in (PipelineState.IDLE, PipelineState.FAILED, None):
    print(f"{pipeline.name} is not running ({pipeline.state.value if pipeline.state else 'no state'})")
else:
    print(f"Stopping {pipeline.name} ({pipeline.state.value})")
    w.pipelines.stop(pipeline_id).result(timeout=timedelta(minutes=10))
    print("Stopped.")
