# databricks-ftn

An end-to-end Databricks project on Azure, first shown at a Databricks guest lecture at FTN Novi Sad.
The data team of a (fictional) food delivery startup in Novi Sad wants to know **why deliveries are late** and
**which restaurants make money**. Fork it and reuse it.

| Folder | What it is |
|---|---|
| [`infra-terraform/`](infra-terraform) | The platform: an Azure Databricks workspace with VNet injection, Unity Catalog (storage credential, external location, catalog), an admin and an analyst group, and an optional classic cluster. |
| [`demo/`](demo) | The data project, as a Declarative Automation Bundle: a data generator, a streaming and a batch pipeline (bronze, silver, gold), jobs, an AI/BI dashboard, a Genie space and a Databricks App with a live courier map (Lakebase). |

Start with `infra-terraform/README.md` to create the workspace, then follow `demo/README.md`.
