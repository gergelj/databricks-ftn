# Food delivery demo (Declarative Automation Bundle)

An end-to-end Databricks project, first shown at a Databricks guest lecture at FTN Novi Sad. Fork it and
reuse it. You are the data team of a (fictional) food delivery
startup in Novi Sad. Orders and courier GPS events arrive all the time, restaurants send their menus every night,
and customers change their addresses. Management asks two questions:

1. **Why are deliveries late?**
2. **Which restaurants make money?**

Follow one order, **#1042** (Milica, Liman, delivered 21 minutes late, "Food arrived cold"), from a raw JSON
file through bronze, silver and gold, to the dashboard and a Genie answer.

Terraform in [`../infra-terraform`](../infra-terraform) builds the platform (workspace, network, Unity Catalog).
This bundle deploys the data project that runs on it.

## Two pipelines: streaming and batch

| | `food_delivery_streaming` | `food_delivery_batch` |
|---|---|---|
| Sources | orders + courier GPS (JSON), customer changes (CDC, JSON) | restaurant catalog (nightly CSV) |
| Runs | **continuously**: new files are processed within seconds | **triggered**: on demand, daily at 06:00 (`food_delivery_daily_batch`), and when a restaurants file arrives (`food_delivery_on_new_restaurant_file`) |
| Tables | `bronze.orders`, `bronze.gps_events`, `bronze.customers_cdc`, `silver.orders`, `silver.orders_quarantine`, `silver.gps_events`, `silver.customers` (SCD2), `gold.avg_delivery_time_per_district_5min` | `bronze.restaurants`, `silver.restaurants` (SCD1), `gold.daily_revenue_per_restaurant`, `gold.late_deliveries_per_district`, `gold.deliveries`, `gold.customers` (masked) |
| Language | bronze + silver in Python, gold in SQL | bronze + silver in Python, gold in SQL |

The streaming pipeline keeps the facts fresh (live delivery times). The batch pipeline joins the orders with the
restaurant catalog and builds the reports (daily revenue). The streaming pipeline never reads batch tables, so the two
can run independently; the batch pipeline reads `silver.orders` and `silver.customers` from the streaming one.

```
landing.raw (volume)           STREAMING PIPELINE (continuous)                         BATCH PIPELINE (triggered)
--------------------           -------------------------------                         --------------------------
orders/*.json        ──▶ bronze.orders ──▶ silver.orders ──┬──▶ gold.avg_delivery_time_   ┌─▶ gold.daily_revenue_per_restaurant
                                 └──────▶ silver.orders_   │    per_district_5min         ├─▶ gold.late_deliveries_per_district
                                          quarantine       └──────────────────────────────┼─▶ gold.deliveries
gps/*.json           ──▶ bronze.gps_events ──▶ silver.gps_events                          │
customers_cdc/*.json ──▶ bronze.customers_cdc ──▶ silver.customers (SCD2) ────────────────┼─▶ gold.customers (masked)
restaurants/*.csv    ──────────────────────────────▶ bronze.restaurants ─▶ silver.restaurants (SCD1) ─┘
```

## Layout

```
demo/
  databricks.yml                              bundle definition, dev + prod targets
  resources/
    food_delivery_streaming.pipeline.yml      continuous pipeline (serverless)
    food_delivery_batch.pipeline.yml          triggered pipeline (serverless)
    daily_batch.job.yml                       batch pipeline every morning at 06:00
    restaurant_refresh.job.yml                batch pipeline when a restaurants file arrives (file-arrival trigger)
    data_generator.job.yml                    the data generator
    demo_setup.job.yml                        one-time setup
    demo_reset.job.yml                        reset everything to a clean state
    stop_streaming.job.yml                    stop the continuous pipeline
    food_delivery.dashboard.yml               AI/BI dashboard
    food_delivery.genie.yml                   Genie space on the gold tables
  src/
    generator/generate_events.py              writes orders, GPS, customer CDC and restaurant files (+ planned failures)
    pipelines/streaming/bronze.py             Python: Auto Loader for orders, GPS, customer changes
    pipelines/streaming/silver.py             Python: expectations + quarantine, SCD2 customers (AUTO CDC), GPS
    pipelines/streaming/gold.sql              SQL: 5-minute windows with a watermark
    pipelines/batch/bronze.py                 Python: Auto Loader for the restaurants CSV
    pipelines/batch/silver.py                 Python: SCD1 restaurants (AUTO CDC)
    pipelines/batch/gold.sql                  SQL: revenue, lateness, deliveries, masked customers
    setup/00_setup.py                         schemas, volumes, PII mask function
    setup/start_streaming_pipeline.py         start the streaming pipeline and wait for data (setup/reset jobs)
    setup/stop_streaming_pipeline.py          stop it (reset and stop jobs)
    setup/99_reset.py                         delete generated data (reset job)
    notebooks/01_batch_ingestion.py           CSV schema inference, Auto Loader + checkpoints, time travel
    notebooks/02_streaming_live.py            live-updating stream in a notebook
    analyst/01_analyst_access.sql             grants for the analysts group, masked columns
    dashboards/food_delivery.lvdash.json      the dashboard definition (datasets, widgets, layout)
  data/restaurants.csv                        20 fictional Novi Sad restaurants
```

The generator's data has a story built in: Liman is late in about 40% of orders (roadworks), the districts across
the river (Petrovaradin, Sremska Kamenica) about 20%, the rest under 10%. The dinner peak (18:00-21:00) is slower,
and three restaurants (R001, R012, R014) have slow kitchens. Late orders mostly come with a complaint in
`review_text`.

## First-time setup

Authentication: run the commands from the `demo/` folder. First replace the two `host` values in
`databricks.yml` with your workspace URL (`terraform output workspace_url` in `../infra-terraform`). The CLI can
use your Azure CLI login (`az login`, the same one Terraform uses, with `export DATABRICKS_AUTH_TYPE=azure-cli`),
or create a profile with `databricks auth login --host <workspace-url> --profile my-workspace` and add
`--profile my-workspace` to the commands.

The bundle expects the catalog `databricks_ftn_catalog` and the groups `databricks-ftn-admins` and
`databricks-ftn-analysts` (all created by `../infra-terraform` with `resource_prefix = "databricks-ftn"`).
Using other names? Override them, e.g. `--var catalog=my_catalog`, or change the defaults in `databricks.yml`.

```bash
cd demo
# once per catalog: the landing zone must exist before the file-arrival job can be created
databricks schemas create landing databricks_ftn_catalog
databricks volumes create databricks_ftn_catalog landing raw MANAGED

databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run food_delivery_setup -t dev   # schemas, history, starts streaming, runs batch once
```

**Cost:** the streaming pipeline is continuous: once started it runs until you stop it
(`databricks bundle run stop_streaming -t dev`). Stop it whenever you are done.

Start the data generator a few minutes before you want to look at live data, so new files keep arriving.
After a fresh deploy, wait a couple of minutes before the first file drop: the file-arrival trigger's first check
only records which files already exist.

## Walkthrough: what to try

A suggested order, from platform to answers:

| Step | What to do |
|---|---|
| 1. Look around | Jobs & Pipelines: `[dev ...] food_delivery_streaming` (running) and `food_delivery_batch`, their graphs, the dashboard and the Genie space. |
| 2. Unity Catalog | Catalog Explorer: `databricks_ftn_catalog` → the `landing.raw` volume, `silver.orders` (history, lineage). |
| 3. Batch | Notebook `01_batch_ingestion`. Then the batch pipeline (`src/pipelines/batch/`, graph, `databricks bundle run food_delivery_batch -t dev`). |
| 4. Data quality | Run the generator with `mode=bad_records`. Before you look, guess what the pipeline will do. The running streaming pipeline handles the rows within seconds: see the data quality tab of `silver.orders` and `silver.orders_quarantine`. |
| 5. Orchestration | Run the generator with `mode=nightly_restaurants`. `food_delivery_on_new_restaurant_file` starts by itself after about 2 minutes (the trigger checks every minute and waits 60 s after the last change); the new restaurant (Poke Bowl Liman) appears in `silver.restaurants`. The schedule is on `food_delivery_daily_batch`. |
| 6. Streaming | Start the generator (`databricks bundle run data_generator -t dev --params duration_minutes=40`). Notebook `02_streaming_live` on a classic cluster (live counter). Then the streaming pipeline (`src/pipelines/streaming/`): the counts in its graph keep growing. Same code style as the batch pipeline (Auto Loader, Python + SQL), different trigger: `continuous: true`. Planned failures: `mode=late_event` (watermark), `mode=schema_change` (`tip_amount` appears in `bronze.orders`). |
| 7. Bundles | `databricks.yml`, `resources/`, `bundle validate`, `bundle deploy`. Try a change: add `o.tip_amount` to `silver.orders`, deploy, refresh. |
| 8. Analyst view | `src/analyst/01_analyst_access.sql` (grants, masking), lineage of `gold.late_deliveries_per_district`, the dashboard, Genie, then the saved queries in `/Shared/databricks-ftn-analyst-queries` (`ai_classify` on review texts, district dropdown). They are Terraform `databricks_query` resources in `../infra-terraform/analyst_queries.tf`. |

Generator modes (job parameter `mode`, or the notebook widget):

```bash
databricks bundle run data_generator -t dev --params mode=bad_records,duration_minutes=0
databricks bundle run data_generator -t dev --params mode=late_event,duration_minutes=0
databricks bundle run data_generator -t dev --params mode=schema_change,duration_minutes=0
databricks bundle run data_generator -t dev --params mode=nightly_restaurants,duration_minutes=0
databricks bundle run data_generator -t dev --params mode=customer_changes,duration_minutes=0
```

`customer_changes` writes one CDC file (`customers_*_burst.json`) with 5 moves (new SCD2 history rows), 1 phone change
(updated in place, history is tracked on address/district only), 1 new customer (INSERT) and 1 deleted account
(its row gets an `__END_AT` and it disappears from `gold.customers`). Change the number of moves with
`customer_changes=10`. While `stream` runs, one customer also moves about once a minute.

All three sources at a glance:

| Source | Continuous | On demand |
|---|---|---|
| Orders + courier GPS (JSON) | `mode=stream` (live couriers, a ping every 5 s) | `mode=bad_records`, `late_event`, `schema_change` |
| Customer changes (CDC, JSON) | one move per minute in `mode=stream` | `mode=customer_changes` |
| Restaurant catalog (CSV) | - | `mode=nightly_restaurants` |


Things to know:

- **Batch vs streaming is one setting.** The two pipelines use the same building blocks; only `continuous` differs.
  To run the streaming pipeline as triggered updates instead (cheaper while you build), deploy with
  `--var streaming_continuous=false` and run it with `databricks bundle run food_delivery_streaming -t dev`.
- A continuous pipeline can't be a job's pipeline task, so the setup and reset jobs start it with a small notebook
  (`src/setup/start_streaming_pipeline.py`) that waits until the backfill is in `silver.orders`.
- Gold reports only change when the batch pipeline runs. New orders show up in `silver.orders` and the 5-minute table
  right away, and in `gold.deliveries` / `gold.daily_revenue_per_restaurant` after the next batch run.
- The 5-minute streaming gold table only writes a window once it is closed: the newest `delivered_ts` must be
  5 minutes past the window end (watermark). With the generator running, expect a delay of about 5-10 minutes.
- Timestamps are Novi Sad local time, stored without a time zone.
- The schema change makes the streaming pipeline restart once with the new schema, which is expected.

## Reset to a clean state

```bash
databricks bundle run food_delivery_reset -t dev --params confirm=yes
```

Deletes all generated files and generator state, stops the streaming pipeline, regenerates the history, then fully
refreshes the streaming pipeline (it keeps running) and the batch pipeline.

## Dashboard and Genie space

Both are bundle resources and use the SQL warehouse `Serverless Starter Warehouse` (variable `warehouse_id`, looked up
by name) and the target's catalog (`dataset_catalog` for the dashboard, `${var.catalog}` in the Genie tables).

- **Dashboard** `Food delivery Novi Sad` (`resources/food_delivery.dashboard.yml`,
  `src/dashboards/food_delivery.lvdash.json`), four pages with date, district and cuisine filters:
  *Overview* (KPIs with daily trend, orders per day on time vs late, revenue per day, orders per hour, payment
  methods), *Late deliveries* (district × hour heatmap, late share per district, how late, rating on time vs late,
  customer reviews), *Restaurants* (revenue per restaurant and cuisine, restaurant scorecard) and *Live* (the
  streaming 5-minute table for the last 3 hours, fills while the generator runs).
  `embed_credentials: false`, so viewers query with their own permissions (analysts: gold only, masked PII).
- **Genie space** `Food delivery Novi Sad` (`resources/food_delivery.genie.yml`): the four gold tables without
  personal data (`deliveries`, `late_deliveries_per_district`, `daily_revenue_per_restaurant`,
  `avg_delivery_time_per_district_5min`), instructions, sample questions and three example queries. Tested questions:
  "Why are deliveries late in Liman?" (answers from `review_text`: closed street, couriers getting lost, roadworks)
  and "Which restaurants had the highest revenue this week?".

After editing either one in the UI, pull the changes back into the files:

```bash
databricks bundle generate dashboard --resource food_delivery_dashboard --force -t dev
databricks bundle generate genie-space --existing-id <space-id> --key food_delivery_genie --force -t dev
```

The Genie API requires `data_sources.tables` sorted by `identifier`, `column_configs` sorted by `column_name`
and every list of items with an `id` sorted by `id`. Keep that order when you edit the YAML by hand.

## Not in the bundle (yet)

- **CI/CD**: a GitHub Actions workflow that deploys the bundle on merge to `main` would be a good next step.
