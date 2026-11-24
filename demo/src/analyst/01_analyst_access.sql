-- =============================================================================
-- Block 8: give the analysts access to gold, and only to gold.
-- Run as a workspace/catalog admin in the SQL editor.
--
-- Prerequisite: an account group `databricks-ftn-analysts` with the demo analyst user in it,
-- assigned to the workspace. (Not created by Terraform yet.)
-- Replace the catalog name if you deployed to another target.
-- =============================================================================

USE CATALOG databricks_ftn_catalog;

GRANT USE CATALOG ON CATALOG databricks_ftn_catalog TO `databricks-ftn-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA gold TO `databricks-ftn-analysts`;

-- What can the analysts do now?
SHOW GRANTS `databricks-ftn-analysts` ON SCHEMA gold;

-- -----------------------------------------------------------------------------
-- Now log in as the analyst (or use a second browser profile) and run:
-- -----------------------------------------------------------------------------

-- Works: gold is shared with analysts.
SELECT * FROM gold.late_deliveries_per_district ORDER BY late_pct DESC LIMIT 10;

-- Works, but email and phone are masked (the column mask in gold.customers).
SELECT * FROM gold.customers WHERE customer_id = 77;

-- Fails with PERMISSION_DENIED: silver is not shared.
SELECT * FROM silver.customers WHERE customer_id = 77;
