-- =============================================================================
-- Give the analysts access to gold, and only to gold.
-- Run as a workspace/catalog admin in the SQL editor.
--
-- Prerequisite: the account group `databricks-ftn-analysts` with at least one analyst user, assigned to the
-- workspace (Terraform creates it from `analyst_users`, see ../../../infra-terraform/analysts.tf).
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
