# Saved SQL queries for the analysts (demo block 8): "Why are deliveries late?", answered from the numbers and
# from the customers' review texts with an AI function. They read the gold schema of the user-defined catalog,
# which the demo bundle (../demo) fills. Analysts get CAN_RUN on them; their data access comes from Unity Catalog.

locals {
  analyst_query_folder = "/Shared/${local.prefix}-analyst-queries"
  # Novi Sad districts in the demo data, for the district dropdown.
  delivery_districts = ["Liman", "Petrovaradin", "Sremska Kamenica", "Novo Naselje", "Telep", "Salajka",
  "Stari Grad", "Detelinara", "Podbara", "Grbavica"]
  # Labels for ai_classify: why was a late order late?
  complaint_reasons = "ARRAY('food arrived cold', 'courier got lost', 'late pickup at the restaurant', 'traffic or roadworks')"
}

data "databricks_sql_warehouse" "analysts" {
  name = var.sql_warehouse_name
}

resource "databricks_directory" "analyst_queries" {
  path = local.analyst_query_folder
}

# 1. The numbers: where are deliveries late?
resource "databricks_query" "late_deliveries_per_district" {
  display_name = "Why late 1 - Late deliveries per district"
  description  = "Share of late deliveries per Novi Sad district, all days."
  warehouse_id = data.databricks_sql_warehouse.analysts.id
  parent_path  = databricks_directory.analyst_queries.path
  catalog      = local.uc_catalog_name
  schema       = "gold"
  query_text   = <<-SQL
    SELECT delivery_district,
           SUM(orders)                                    AS orders,
           SUM(late_orders)                               AS late_orders,
           ROUND(SUM(late_orders) * 100 / SUM(orders), 1) AS late_pct
    FROM late_deliveries_per_district
    GROUP BY delivery_district
    ORDER BY late_pct DESC
  SQL
}

# 2. The words: an LLM sorts the complaints of one district into reasons.
resource "databricks_query" "late_reviews_by_reason" {
  display_name = "Why late 2 - Complaints in a district (ai_classify)"
  description  = "Late orders with a review in the chosen district, classified into complaint reasons by ai_classify."
  warehouse_id = data.databricks_sql_warehouse.analysts.id
  parent_path  = databricks_directory.analyst_queries.path
  catalog      = local.uc_catalog_name
  schema       = "gold"
  query_text   = <<-SQL
    SELECT order_id,
           delivery_district,
           restaurant_name,
           delay_minutes,
           review_text,
           ai_classify(review_text, ${local.complaint_reasons}) AS complaint_reason
    FROM deliveries
    WHERE is_late AND review_text IS NOT NULL AND delivery_district = :district
    LIMIT 50
  SQL

  parameter {
    name  = "district"
    title = "District"
    enum_value {
      enum_options = join("\n", local.delivery_districts)
      values       = ["Liman"]
    }
  }
}

# 3. Numbers and words together: complaint reasons per district.
resource "databricks_query" "complaint_reasons_per_district" {
  display_name = "Why late 3 - Complaint reasons per district (ai_classify)"
  description  = "Number of late-order complaints per district and reason, classified by ai_classify."
  warehouse_id = data.databricks_sql_warehouse.analysts.id
  parent_path  = databricks_directory.analyst_queries.path
  catalog      = local.uc_catalog_name
  schema       = "gold"
  query_text   = <<-SQL
    WITH classified AS (
      SELECT delivery_district,
             ai_classify(review_text, ${local.complaint_reasons}) AS complaint_reason
      FROM deliveries
      WHERE is_late AND review_text IS NOT NULL
    )
    SELECT delivery_district, complaint_reason, COUNT(*) AS complaints
    FROM classified
    GROUP BY ALL
    ORDER BY delivery_district, complaints DESC
  SQL
}

# Analysts can open and run the queries (not edit them).
resource "databricks_permissions" "analyst_queries" {
  for_each = {
    late_deliveries_per_district   = databricks_query.late_deliveries_per_district.id
    late_reviews_by_reason         = databricks_query.late_reviews_by_reason.id
    complaint_reasons_per_district = databricks_query.complaint_reasons_per_district.id
  }
  sql_query_id = each.value

  access_control {
    group_name       = databricks_group.analysts.display_name
    permission_level = "CAN_RUN"
  }
  depends_on = [databricks_mws_permission_assignment.analysts_workspace_access]
}
