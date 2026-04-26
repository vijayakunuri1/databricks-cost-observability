# Databricks Cost Observability

A full-stack cost observability dashboard built as a **Databricks App** — cloud-agnostic (AWS / Azure / GCP), works on free-tier workspaces via **MOCK_MODE**, and deploys in one `git push` via GitHub Actions.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Databricks App](https://img.shields.io/badge/Databricks-App-FF3621?logo=databricks)](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html)
[![Python 3.11](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://www.python.org/)

---

## Screenshots

### Executive Summary
Real-time spend KPIs, 30/60/90-day trends, AI-powered contract-year forecast, and workspace Pareto analysis.

![Executive Summary](docs/screenshots/executive.png)

### Cost Dashboard
Daily DBU and cost trends broken down by product (Jobs, SQL Warehouse, All-Purpose Compute, DLT). Filters by workspace, subscription, resource group, and custom tags.

![Cost Dashboard](docs/screenshots/cost_dashboard.png)

### UC Graph — Unity Catalog Permissions
Interactive graph of users, groups, service principals, catalogs, schemas, row filters, and column masks across the workspace.

![UC Graph](docs/screenshots/uc_graph.png)

---

## Tabs

| Tab | What it shows |
|-----|---------------|
| **Executive** | Spend KPIs, forecast, active users, workspace Pareto |
| **Cost Dashboard** | Daily DBU/cost trends by product, workspace filters |
| **Anomalies** | Spend spikes and outlier detection |
| **User Adoption** | Per-user query count, GB scanned, cost attribution |
| **Compute Sizing** | Cluster right-sizing recommendations |
| **Query Attribution** | Top expensive and slow queries by user/warehouse |
| **AI / LLM** | AI Gateway token usage and model serving cost |
| **UC Graph** | Unity Catalog permission graph |
| **Job SLA** | Job run timelines, success rates, DLT pipeline health |
| **Storage** | Predictive optimization ops, table inventory |
| **Lineage & ML** | Data lineage graph, MLflow experiments, model registry |

---

## Architecture

```
GitHub Actions
    └── databricks bundle deploy     # provisions DAB resources
    └── setup_mock_tables.py         # creates mock system tables
    └── databricks apps deploy       # deploys source code

Databricks App (FastAPI + uvicorn)
    ├── api/v1/          # REST endpoints per tab
    ├── services/        # SQL query logic per domain
    ├── core/
    │   ├── sql_executor.py   # retry + MOCK_MODE SQL rewriter
    │   ├── config.py         # settings from app.yaml env vars
    │   └── security.py       # ALLOWED_USERS, ADMIN_USERS gate
    └── static/index.html    # single-page frontend
```

---

## Quick Start

### Prerequisites
- Databricks workspace (any tier — free tier works with MOCK_MODE)
- Databricks CLI installed and configured
- GitHub account

### 1. Fork & clone

```bash
git clone https://github.com/vijayakunuri1/databricks-cost-observability.git
cd databricks-cost-observability
```

### 2. Set GitHub Secrets

In your repo → **Settings → Secrets and variables → Actions**, add:

| Secret | Value |
|--------|-------|
| `DATABRICKS_HOST` | Your workspace URL, e.g. `https://adb-1234567890.12.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | A personal access token with `clusters`, `sql`, `apps` permissions |

### 3. Configure `app.yaml`

Edit the environment variables for your workspace:

```yaml
- name: DATABRICKS_WAREHOUSE_ID
  value: "your-sql-warehouse-id"       # get from SQL → Warehouses

- name: UC_CATALOG_NAME
  value: "workspace"                   # or your Unity Catalog name

- name: ADMIN_USERS
  value: "you@example.com"            # full access, bypasses all restrictions

- name: MOCK_MODE
  value: "true"                        # set false if you have system table access
```

### 4. Deploy

```bash
git push origin main
```

GitHub Actions will:
1. Deploy the Databricks Asset Bundle (DAB)
2. Run `setup_mock_tables.py` to create all mock system schemas and tables
3. Deploy the app source code

The app URL appears in the Actions log and in your Databricks workspace under **Apps**.

---

## MOCK_MODE

Free-tier Databricks workspaces don't have access to `system.billing`, `system.compute`, etc. **MOCK_MODE** solves this transparently:

- All `system.X.table` references in SQL are **rewritten at execution time** to `workspace.mock_system_X.table`
- 14 mock schemas and ~26 tables are auto-created on deploy with realistic sample data
- Set `MOCK_MODE=false` in `app.yaml` to switch to real system tables (requires paid account + system table enablement)

No code changes needed to switch between mock and production.

---

## Access Control

| Setting | Purpose |
|---------|---------|
| `ADMIN_USERS` | Comma-separated emails with full access + Admin panel |
| `ALLOWED_USERS` | Leave empty to allow all workspace-authenticated users |
| `ALLOWED_WORKSPACE_IDS` | `*` for all workspaces, or comma-separated IDs |

Per-user tab/workspace/tag restrictions can also be managed via the **Admin panel** in the app (no redeploy needed).

---

## Local Development

```bash
pip install -r requirements.txt

export DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
export DATABRICKS_TOKEN=dapi...
export DATABRICKS_WAREHOUSE_ID=your-warehouse-id
export MOCK_MODE=true

uvicorn app:app --reload --port 8000
```

Open [http://localhost:8000](http://localhost:8000).

---

## Project Structure

```
.
├── app.py                        # FastAPI application entry point
├── app.yaml                      # Databricks App config (env vars, command)
├── databricks.yml                # Databricks Asset Bundle config
├── requirements.txt
├── api/v1/                       # One router per dashboard tab
├── services/                     # SQL query + analysis logic
├── core/
│   ├── sql_executor.py           # Shared async SQL executor + MOCK_MODE rewriter
│   ├── config.py                 # Pydantic settings
│   ├── dependencies.py           # WorkspaceClient dependency injection
│   └── security.py               # User identity + access gating
├── scripts/
│   ├── setup_mock_tables.py      # Auto-creates mock system tables on deploy
│   └── setup_enterprise_mock_data.py  # Optional: loads 3-year enterprise dataset
├── static/index.html             # Single-page frontend
└── .github/workflows/deploy.yml  # CI/CD pipeline
```

---

## Contributing

Pull requests are welcome. For major changes, open an issue first.

---

## License

MIT — see [LICENSE](LICENSE).
