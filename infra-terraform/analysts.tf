# Analyst group: an account-level group "<resource_prefix>-analysts" holding every user in analyst_users, assigned
# to the workspace as regular users. Data access (e.g. SELECT on the gold schema) is granted separately in Unity
# Catalog, so the group starts with no data privileges.

locals {
  analyst_group_name = "${local.prefix}-analysts"
}

resource "databricks_group" "analysts" {
  provider     = databricks.accounts
  display_name = local.analyst_group_name
}

data "databricks_user" "analysts" {
  for_each  = toset(var.analyst_users)
  provider  = databricks.accounts
  user_name = each.value
}

resource "databricks_group_member" "analysts" {
  for_each  = toset(var.analyst_users)
  provider  = databricks.accounts
  group_id  = databricks_group.analysts.id
  member_id = data.databricks_user.analysts[each.key].id
}

# assign the analyst group to the workspace as users, so its members can log in
resource "databricks_mws_permission_assignment" "analysts_workspace_access" {
  provider     = databricks.accounts
  workspace_id = azurerm_databricks_workspace.this.workspace_id
  principal_id = databricks_group.analysts.id
  permissions  = ["USER"]
  depends_on = [
    databricks_metastore_assignment.this,
    databricks_group_member.analysts,
  ]
}

# Workspace entitlement for the analyst group: Databricks SQL access (SQL editor, SQL warehouses, dashboards,
# Genie). Consumer access (workspace_consume) is not set: it can't be combined with Databricks SQL access, and
# the analysts need the SQL editor. Entitlements are a workspace-level setting, so this uses the workspace
# provider, after the group is assigned to the workspace.
resource "databricks_entitlements" "analysts" {
  group_id              = databricks_group.analysts.id
  databricks_sql_access = true
  depends_on            = [databricks_mws_permission_assignment.analysts_workspace_access]
}
