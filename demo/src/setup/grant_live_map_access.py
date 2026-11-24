# Databricks notebook source
# MAGIC %md
# MAGIC # Let the live map app read the synced GPS table
# MAGIC
# MAGIC The app's service principal gets a Postgres role when the app is deployed with its `postgres` resource
# MAGIC (`CAN_CONNECT_AND_CREATE`), but it can't read tables it doesn't own. This notebook runs as the
# MAGIC Lakebase project owner and grants it read access to the `live` schema, where the synced table
# MAGIC `live.courier_gps` lives. Run it once after the first app deployment (job `live_map_grants`).

# COMMAND ----------

# MAGIC %pip install "psycopg[binary]>=3.2" "databricks-sdk>=0.81.0" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import psycopg
from databricks.sdk import WorkspaceClient
from psycopg import sql

dbutils.widgets.text("app_name", "")
dbutils.widgets.text("lakebase_endpoint", "projects/food-delivery-live/branches/production/endpoints/primary")
dbutils.widgets.text("postgres_database", "databricks_postgres")
dbutils.widgets.text("schema", "live")
app_name = dbutils.widgets.get("app_name")
endpoint = dbutils.widgets.get("lakebase_endpoint")

w = WorkspaceClient()
app_sp = w.apps.get(app_name).service_principal_client_id
me = w.current_user.me().user_name
host = w.postgres.get_endpoint(name=endpoint).status.hosts.host
print(f"app {app_name}: service principal {app_sp}")

# COMMAND ----------

schema = sql.Identifier(dbutils.widgets.get("schema"))
role = sql.Identifier(app_sp)
statements = [
    sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(schema, role),
    sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(schema, role),
    sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT ON TABLES TO {}").format(schema, role),
]
token = w.postgres.generate_database_credential(endpoint=endpoint).token
with psycopg.connect(host=host, dbname=dbutils.widgets.get("postgres_database"), user=me, password=token,
                     sslmode="require", autocommit=True) as conn:
    for statement in statements:
        conn.execute(statement)
        print(statement.as_string(conn))
print("Done: the app can read the live schema.")
