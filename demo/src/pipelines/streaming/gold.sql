-- =============================================================================
-- STREAMING PIPELINE - GOLD: live delivery performance.
--
-- 5-minute windows of delivery time per district, updated continuously.
-- The watermark says "wait at most 5 minutes for late events": a window is written once
-- the newest delivered_ts seen is 5 minutes past its end. Events older than that are
-- ignored here (they are still in silver.orders and in the batch gold tables).
-- =============================================================================
CREATE OR REFRESH STREAMING TABLE gold.avg_delivery_time_per_district_5min
COMMENT 'Live delivery performance: deliveries, average delivery time and late deliveries per district in 5-minute windows.'
AS SELECT
  window(delivered_ts, '5 minutes').start             AS window_start,
  window(delivered_ts, '5 minutes').end               AS window_end,
  delivery_district,
  COUNT(*)                                            AS deliveries,
  CAST(ROUND(AVG(delivery_minutes), 1) AS DOUBLE)     AS avg_delivery_minutes,
  COUNT_IF(is_late)                                   AS late_deliveries
FROM STREAM(silver.orders) WATERMARK delivered_ts DELAY OF INTERVAL 5 MINUTES
GROUP BY window(delivered_ts, '5 minutes'), delivery_district;
