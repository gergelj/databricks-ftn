-- =============================================================================
-- BATCH PIPELINE - GOLD: business-ready tables that answer management's two questions:
--   1. Why are deliveries late?          -> late_deliveries_per_district, deliveries
--   2. Which restaurants make money?     -> daily_revenue_per_restaurant
-- plus a PII-masked customer table. Analysts only get access to the gold schema.
--
-- Materialized views are recomputed (incrementally where possible) on each batch run.
-- silver.orders and silver.customers come from the streaming pipeline;
-- silver.restaurants from this pipeline. The join happens here, so a restaurant
-- change shows up in all reports on the next run.
--
-- The same dp ideas as bronze.py and silver.py, written in SQL (a pipeline can mix both):
--   CREATE TEMPORARY VIEW               = @dp.temporary_view          (helper step, not stored)
--   CREATE OR REFRESH MATERIALIZED VIEW = @dp.materialized_view       (stored, recomputed each run)
--   CREATE OR REFRESH STREAMING TABLE   = @dp.table with readStream   (used in the streaming pipeline)
-- "OR REFRESH": the pipeline creates the table on the first run and refreshes it on every run after that.
--
-- Why materialized views here and not streaming tables?
--   A streaming table only processes NEW rows, once, and never looks back.
--   A materialized view stores the result of a query and brings it up to date on every run,
--   so old rows change too. If a restaurant is renamed tonight, yesterday's revenue
--   rows show the new name. That's what reports need.
-- =============================================================================

-- Orders with the current restaurant details. Pipeline-private, not published to Unity Catalog.
-- An order only knows "restaurant R001"; this join adds the restaurant's name, cuisine and district.
-- LEFT JOIN: an order for a restaurant we don't know (yet) is kept, with empty restaurant columns.
CREATE TEMPORARY VIEW orders_enriched AS
SELECT
  o.*,
  r.name     AS restaurant_name,
  r.cuisine,
  r.district AS restaurant_district
FROM silver.orders o
LEFT JOIN silver.restaurants r
  ON o.restaurant_id = r.restaurant_id;

-- The column list with COMMENTs is optional: the types come from the query anyway.
-- We add it because the descriptions end up in Unity Catalog, where people (and Genie) read them.
-- GROUP BY ALL = group by every column that is not an aggregate (COUNT, SUM, AVG...).
CREATE OR REFRESH MATERIALIZED VIEW gold.daily_revenue_per_restaurant (
  order_date          DATE   COMMENT 'Day the order was placed (Novi Sad local time)',
  restaurant_id       STRING COMMENT 'Restaurant ID, e.g. R001',
  restaurant_name     STRING COMMENT 'Restaurant name',
  cuisine             STRING COMMENT 'Type of food the restaurant serves',
  restaurant_district STRING COMMENT 'Novi Sad district where the restaurant is located',
  orders              BIGINT COMMENT 'Number of delivered orders',
  revenue_rsd         DOUBLE COMMENT 'Total order value in Serbian dinars (RSD), including delivery fee',
  avg_order_value_rsd DOUBLE COMMENT 'Average order value in RSD',
  late_orders         BIGINT COMMENT 'Orders delivered after the promised time',
  avg_rating          DOUBLE COMMENT 'Average customer rating (1-5) of rated orders'
)
COMMENT 'Daily revenue, order count, lateness and rating per restaurant. Answers: which restaurants make money?'
AS SELECT
  CAST(order_ts AS DATE)                             AS order_date,
  restaurant_id,
  restaurant_name,
  cuisine,
  restaurant_district,
  COUNT(*)                                           AS orders,
  CAST(ROUND(SUM(total_amount), 2) AS DOUBLE)        AS revenue_rsd,
  CAST(ROUND(AVG(total_amount), 2) AS DOUBLE)        AS avg_order_value_rsd,
  COUNT_IF(is_late)                                  AS late_orders,
  CAST(ROUND(AVG(rating), 2) AS DOUBLE)              AS avg_rating
FROM orders_enriched
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW gold.late_deliveries_per_district (
  order_date                DATE   COMMENT 'Day the order was placed (Novi Sad local time)',
  order_hour                INT    COMMENT 'Hour of the day the order was placed (0-23)',
  delivery_district         STRING COMMENT 'Novi Sad district the order was delivered to',
  orders                    BIGINT COMMENT 'Number of delivered orders',
  late_orders               BIGINT COMMENT 'Orders delivered after the promised time',
  late_pct                  DOUBLE COMMENT 'Share of late orders in percent',
  avg_delivery_minutes      DOUBLE COMMENT 'Average minutes from order to delivery',
  avg_delay_minutes_if_late DOUBLE COMMENT 'Average minutes late, for late orders only'
)
COMMENT 'Delivery lateness per district and hour. Answers: why (where and when) are deliveries late?'
AS SELECT
  CAST(order_ts AS DATE)                                                AS order_date,
  hour(order_ts)                                                        AS order_hour,
  delivery_district,
  COUNT(*)                                                              AS orders,
  COUNT_IF(is_late)                                                     AS late_orders,
  CAST(ROUND(COUNT_IF(is_late) * 100 / COUNT(*), 1) AS DOUBLE)          AS late_pct,
  CAST(ROUND(AVG(delivery_minutes), 1) AS DOUBLE)                       AS avg_delivery_minutes,
  CAST(ROUND(AVG(CASE WHEN is_late THEN delay_minutes END), 1) AS DOUBLE) AS avg_delay_minutes_if_late
FROM silver.orders
GROUP BY ALL;

-- Order-level facts without customer PII: for Genie, dashboards and AI functions on review_text.
CREATE OR REFRESH MATERIALIZED VIEW gold.deliveries (
  -- Expectation = a data quality rule. Plain EXPECT only warns: the row is kept, and the number of
  -- rows breaking the rule is shown in the pipeline's data quality tab.
  -- (ON VIOLATION DROP ROW would drop it, ON VIOLATION FAIL UPDATE would stop the run.)
  CONSTRAINT known_restaurant EXPECT (restaurant_name IS NOT NULL)     -- warn only: keep, but count
)
COMMENT 'One row per delivered order, without customer personal data: timing, lateness, restaurant, district, rating and the free-text review.'
AS SELECT
  order_id,
  CAST(order_ts AS DATE) AS order_date,
  order_ts,
  promised_delivery_ts,
  delivered_ts,
  delivery_minutes,
  delay_minutes,
  is_late,
  restaurant_id,
  restaurant_name,
  cuisine,
  restaurant_district,
  delivery_district,
  courier_id,
  customer_id,
  item_count,
  total_amount AS total_amount_rsd,
  payment_method,
  rating,
  review_text
FROM orders_enriched;

-- Current customer profiles. Email and phone are masked for everyone outside the
-- admin group (the mask function is created by src/setup/00_setup.py).
-- MASK <function>: a Unity Catalog column mask. Everyone queries the same table; the function
-- decides per user what they see (the real value for admins, a masked value for everyone else).
-- ${pii_mask_function} is filled in from the pipeline configuration (resources/food_delivery_batch.pipeline.yml).
-- __START_AT / __END_AT come from the SCD Type 2 history in silver.customers; __END_AT IS NULL = current row.
CREATE OR REFRESH MATERIALIZED VIEW gold.customers (
  customer_id   BIGINT COMMENT 'Customer ID',
  customer_name STRING COMMENT 'First and last name',
  email         STRING COMMENT 'Email address (masked for non-admins)' MASK ${pii_mask_function},
  phone         STRING COMMENT 'Phone number (masked for non-admins)' MASK ${pii_mask_function},
  address       STRING COMMENT 'Current street address',
  district      STRING COMMENT 'Current Novi Sad district',
  customer_since TIMESTAMP COMMENT 'Start of the current address record'
)
COMMENT 'Current customer profiles (one row per customer). Email and phone are masked for non-admins.'
AS SELECT
  customer_id,
  concat_ws(' ', first_name, last_name) AS customer_name,
  email,
  phone,
  address,
  district,
  __START_AT AS customer_since
FROM silver.customers
WHERE __END_AT IS NULL;
