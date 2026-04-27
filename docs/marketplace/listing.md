# Databricks Marketplace — Listing Assets

Copy-paste ready content for the provider submission form.

---

## Listing Title
Databricks Cost Observability — 11-Tab Dashboard App

## Short Description (160 chars max)
Open-source cost observability app for Databricks. 11 dashboards, free-tier compatible, deploys in one git push. AWS / Azure / GCP.

## Full Description

### Overview
Databricks Cost Observability is a full-stack observability dashboard built natively as a Databricks App. It gives data platform teams and FinOps practitioners real-time visibility into Databricks spending, usage patterns, and optimization opportunities — across any cloud, any workspace tier.

Unlike external BI tools that require data exports or third-party connectors, this app runs entirely inside your Databricks workspace. It reads directly from Databricks system tables and surfaces actionable insights without moving data outside your security perimeter.

### Key Capabilities

**Executive & Cost**
- 30/60/90-day spend trends with AI_FORECAST() contract-year projection
- Per-workspace, per-SKU, per-user cost attribution
- Spend concentration (Pareto) analysis across workspaces
- Savings opportunity identification

**Compute & Jobs**
- Cluster right-sizing recommendations based on actual utilization
- Job SLA tracking — success rates, run durations, failure trends
- DLT pipeline cost and update timeline
- SQL Warehouse query attribution and cost-per-query estimation

**Data & AI**
- AI Gateway token usage and cost by model and user
- Model serving endpoint health and request volume
- Data lineage graph (table and column level)
- MLflow experiments, run history, and registered model registry

**Governance**
- Unity Catalog permission graph — users, groups, service principals, catalogs, schemas
- Row-level security and column mask inventory
- Audit log analysis and active user trends

**Storage & Platform**
- Predictive optimization operations history
- Table inventory by catalog/schema with type distribution
- Marketplace listing and consumption analytics
- Platform ops — dashboard adoption, network endpoint status

### Free-Tier Compatible (MOCK_MODE)
Works on any Databricks workspace including free trial accounts. When system tables are not accessible, MOCK_MODE automatically rewrites all SQL queries to use pre-populated mock tables — no configuration required. Switch to production data with a single environment variable change.

### Deployment
- Deploys via Databricks Asset Bundles (DAB) with one `git push`
- GitHub Actions CI/CD pipeline included
- No external dependencies — runs entirely on Databricks infrastructure

## Category
- Apps & Solutions
- FinOps & Cost Management
- Data Platform Operations

## Tags
cost-optimization, finops, observability, databricks-apps, unity-catalog, system-tables, dashboard, monitoring

## Supported Clouds
- AWS
- Azure
- GCP

## Pricing Model
- Base tier: Free (open source, self-deployed)
- Managed tier: Contact for pricing (hosted, supported, enterprise features)

## Support Contact
- GitHub Issues: https://github.com/vijayakunuri1/databricks-cost-observability/issues
- Email: vijaymohan.akunuri@gmail.com

## Technical Requirements
- Databricks workspace (any tier)
- SQL Warehouse (Starter or above)
- Unity Catalog enabled (recommended, not required)
- Python 3.11+
- Databricks Apps enabled on workspace

## Version
1.0.0

## License
Elastic License 2.0 (EL2) — free for internal use and self-deployment; commercial hosting requires a separate agreement

---

## What Reviewers Will Ask — Be Ready

| Question | Your Answer |
|----------|-------------|
| Does it work without system table access? | Yes — MOCK_MODE rewrites all SQL to mock tables automatically |
| What permissions does it need? | SQL Warehouse access, read on system tables (optional) |
| Does it store data outside Databricks? | No — all data stays in the workspace |
| Is there a demo environment? | Yes — run with MOCK_MODE=true on any workspace |
| How is it updated? | User re-runs deploy workflow on new releases |
