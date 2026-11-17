resource "azurerm_databricks_workspace" "this" {
  name                        = local.workspace_name
  resource_group_name         = local.resource_group_name
  location                    = local.resource_group_location
  sku                         = "premium"
  tags                        = var.tags
  managed_resource_group_name = local.managed_resource_group_name

  custom_parameters {
    virtual_network_id                                   = local.vnet.id
    private_subnet_name                                  = azurerm_subnet.private.name
    public_subnet_name                                   = azurerm_subnet.public.name
    public_subnet_network_security_group_association_id  = azurerm_subnet_network_security_group_association.public.id
    private_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.private.id
    storage_account_name                                 = local.dbfsname
    no_public_ip                                         = true
  }

  lifecycle {
    precondition {
      condition     = var.create_nat_gateway
      error_message = "create_nat_gateway = false is not supported by this template: without Private Link, clusters reach the Databricks control plane only through the NAT gateway and would not launch. Use the azure-privatelink-classic template to deploy without NAT."
    }
  }

  depends_on = [
    azurerm_subnet_network_security_group_association.public,
    azurerm_subnet_network_security_group_association.private
  ]
}

# Workspace-level legacy settings. Running clusters and SQL warehouses must be restarted to pick them up.

# Disables direct Hive metastore access, external location fallback mode and Databricks Runtime versions older
# than 13.3 LTS in this workspace.
resource "databricks_disable_legacy_access_setting" "this" {
  disable_legacy_access {
    value = true
  }
  depends_on = [databricks_metastore_assignment.this]
}

# Disables access to the DBFS root and DBFS mounts in this workspace.
resource "databricks_disable_legacy_dbfs_setting" "this" {
  disable_legacy_dbfs {
    value = true
  }
  depends_on = [databricks_metastore_assignment.this]
}

# Admin group: an account-level group holding every user in admin_users. It is made workspace admin and, when this
# template creates the metastore, the metastore owner (= metastore admin). Skipped when admin_users is empty.

locals {
  create_admin_group = length(var.admin_users) > 0
  # Default name keeps the former "<new_metastore_name>-admins" group so existing deployments are not renamed.
  admin_group_name = (
    var.admin_group_name != "" ? var.admin_group_name : "${local.prefix}-admins"
  )
}

resource "databricks_group" "admins" {
  count        = local.create_admin_group ? 1 : 0
  provider     = databricks.accounts
  display_name = local.admin_group_name
}

# # The group used to be created only for a new metastore; keep it instead of destroying and recreating it.
# moved {
#   from = databricks_group.metastore_owner_group
#   to   = databricks_group.admins
# }

data "databricks_user" "admins" {
  for_each  = toset(var.admin_users)
  provider  = databricks.accounts
  user_name = each.value
}

resource "databricks_group_member" "admins" {
  for_each  = toset(var.admin_users)
  provider  = databricks.accounts
  group_id  = databricks_group.admins[0].id
  member_id = data.databricks_user.admins[each.key].id
}

# assign the admin group as workspace admins
resource "databricks_mws_permission_assignment" "workspace_access" {
  count        = local.create_admin_group ? 1 : 0
  provider     = databricks.accounts
  workspace_id = azurerm_databricks_workspace.this.workspace_id
  principal_id = databricks_group.admins[0].id
  permissions  = ["ADMIN"]
  depends_on = [
    databricks_metastore_assignment.this,
    databricks_group_member.admins,
  ]
}

# metastore: assign an existing one (metastore_id) or create one (new_metastore_name) and assign it to the workspace.
# Neither set = no metastore assignment here (attach one manually in the account console).

locals {
  create_metastore = var.new_metastore_name != ""
  assign_metastore = var.metastore_id != "" || local.create_metastore
  metastore_id     = local.create_metastore ? databricks_metastore.this[0].id : var.metastore_id
}

resource "databricks_metastore" "this" {
  count         = local.create_metastore ? 1 : 0
  provider      = databricks.accounts
  name          = var.new_metastore_name
  region        = local.resource_group_location
  owner         = databricks_group.admins[0].display_name
  force_destroy = true
  depends_on    = [databricks_group_member.admins]

  lifecycle {
    precondition {
      condition     = var.metastore_id == ""
      error_message = "Set at most one of metastore_id (assign existing) or new_metastore_name (create), not both."
    }
    precondition {
      condition     = length(var.admin_users) > 0
      error_message = "admin_users is required when new_metastore_name is set (its members form the admin group that owns the metastore)."
    }
  }
}

resource "databricks_metastore_assignment" "this" {
  count        = local.assign_metastore ? 1 : 0
  provider     = databricks.accounts
  workspace_id = azurerm_databricks_workspace.this.workspace_id
  metastore_id = local.metastore_id

  depends_on = [databricks_group_member.admins]
}
