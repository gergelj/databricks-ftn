# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Setup: schemas, landing volume and the PII mask function
# MAGIC
# MAGIC Run once per catalog (the `food_delivery_setup` job runs it for you). Idempotent.
# MAGIC
# MAGIC | Object | Purpose |
# MAGIC |---|---|
# MAGIC | `landing` schema, `landing.raw` volume | landing zone: the generator writes raw files here |
# MAGIC | `landing.checkpoints` volume | Auto Loader checkpoints for the notebook demos |
# MAGIC | `bronze`, `silver`, `gold` schemas | medallion layers, filled by the pipeline |
# MAGIC | `sandbox` schema | scratch tables for the notebook demos |
# MAGIC | `gold.mask_pii` function | column mask for email and phone in `gold.customers` |

# COMMAND ----------

dbutils.widgets.text("catalog", "databricks_ftn_catalog")
dbutils.widgets.text("admin_group", "databricks-ftn-admins")
catalog = dbutils.widgets.get("catalog")
admin_group = dbutils.widgets.get("admin_group")

# COMMAND ----------

schemas = {
    "landing": "Landing zone: raw files exactly as the source systems deliver them.",
    "bronze": "Bronze layer: raw data as tables, append-only, with ingestion metadata.",
    "silver": "Silver layer: cleaned, validated and joined data.",
    "gold": "Gold layer: business-ready tables for dashboards, Genie and analysts.",
    "sandbox": "Scratch space for the notebook demos.",
}
for schema, comment in schemas.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}` COMMENT '{comment}'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.landing.raw COMMENT 'Raw files from the delivery app: orders, gps, customers_cdc, restaurants.'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.landing.checkpoints COMMENT 'Auto Loader schema and checkpoint locations for the notebook demos.'")

# The file-arrival trigger and Auto Loader need the folders to exist.
for folder in ["orders", "gps", "customers_cdc", "restaurants"]:
    dbutils.fs.mkdirs(f"/Volumes/{catalog}/landing/raw/{folder}")

# COMMAND ----------

# Column mask: admins see the real value, everybody else a masked one.
# Masks are evaluated at query time, so the pipeline still stores the real values.
spark.sql(f"""
CREATE OR REPLACE FUNCTION `{catalog}`.gold.mask_pii(value STRING)
RETURNS STRING
COMMENT 'Column mask for personal data: shows the value only to members of {admin_group}.'
RETURN CASE
  WHEN is_account_group_member('{admin_group}') THEN value
  WHEN value IS NULL THEN NULL
  ELSE concat(left(value, 3), '*****')
END
""")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN `{catalog}`"))
