-- =============================================================================
-- Block 8: "Why are deliveries late?" answered from the numbers AND from the review text.
-- Run in the SQL editor on a SQL warehouse (works as admin or as analyst: only gold is used).
-- =============================================================================

USE CATALOG databricks_ftn_catalog;

-- 1. The numbers: where and when?
SELECT delivery_district,
       SUM(orders)                                   AS orders,
       SUM(late_orders)                              AS late_orders,
       ROUND(SUM(late_orders) * 100 / SUM(orders), 1) AS late_pct
FROM gold.late_deliveries_per_district
GROUP BY delivery_district
ORDER BY late_pct DESC;

-- 2. The words: let an LLM sort complaints into reasons with ai_classify.
SELECT order_id,
       delivery_district,
       restaurant_name,
       delay_minutes,
       review_text,
       ai_classify(review_text, ARRAY('food arrived cold', 'courier got lost', 'late pickup at the restaurant', 'traffic or roadworks')) AS complaint_reason
FROM gold.deliveries
WHERE is_late AND review_text IS NOT NULL AND delivery_district = 'Liman'
LIMIT 50;

-- 3. Reasons per district: numbers and text together.
WITH classified AS (
  SELECT delivery_district,
         ai_classify(review_text, ARRAY('food arrived cold', 'courier got lost', 'late pickup at the restaurant', 'traffic or roadworks')) AS complaint_reason
  FROM gold.deliveries
  WHERE is_late AND review_text IS NOT NULL
)
SELECT delivery_district, complaint_reason, COUNT(*) AS complaints
FROM classified
GROUP BY ALL
ORDER BY delivery_district, complaints DESC;
