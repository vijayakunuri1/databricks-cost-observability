# Roadmap

This is the planned direction for Databricks Cost Observability. Items are grouped by priority. Community contributions are welcome on any of these — open an issue first to align before building.

---

## In Progress

| Item | Area | Notes |
|---|---|---|
| Mock data coverage for all tabs | Dev experience | Some tabs still return empty with MOCK_MODE=true |
| Fix Apply button / date filter on initial load | Executive tab | Dates occasionally load blank on first render |

---

## Up Next

| Item | Area | Notes |
|---|---|---|
| Serverless compute cost tab | New tab | Uses `system.billing.usage` filtered by `SERVERLESS` origin |
| Cost budget & threshold alerts | Alerting | Set monthly budget per workspace, alert when breached |
| Slack / Teams alert integration | Alerting | Send cost spike alerts to a webhook instead of email only |
| Export to CSV | Cost Dashboard | Download filtered billing data as CSV |
| Multi-workspace cost comparison | Executive tab | Side-by-side cost trend across workspaces |

---

## Planned

| Item | Area | Notes |
|---|---|---|
| Delta Live Tables cost breakdown | New tab | Per-pipeline DBU and cost attribution |
| Unity Catalog audit improvements | UC Graph | Show lineage violations and access anomalies |
| GCP-specific cost dimensions | Cloud coverage | Test and fix GCP-specific system table schema differences |
| AWS-specific cost dimensions | Cloud coverage | Test and fix AWS-specific system table schema differences |
| Terraform / IaC deployment option | Deployment | Alternative to Databricks Asset Bundle for infra-as-code shops |
| Scheduled PDF / email report | Reporting | Weekly cost summary emailed to admins |
| Custom tag-based cost allocation | Cost Dashboard | Allocate costs by arbitrary tag key (team, project, env) |
| Dark / light mode toggle | UI | Currently dark-only |

---

## Ideas (Not Yet Scoped)

These are rough ideas. Open an issue with the **New Dashboard Tab** template if you want to champion one.

- Marketplace usage and cost tab
- Job SLA trend and breach history
- Model Serving cost per endpoint
- Cost anomaly ML model improvements (custom thresholds per workspace)
- RBAC-aware cost views (users only see their team's costs)
- GitHub Actions cost (if Databricks adds this to system tables)

---

## Completed

| Item | Released |
|---|---|
| Executive summary tab | v1.0 |
| Cost Dashboard with daily trend, donut, tag breakdown | v1.0 |
| Anomaly detection (ML-based + config-based) | v1.0 |
| User Adoption tab | v1.0 |
| Compute Sizing tab | v1.0 |
| Query Attribution tab | v1.0 |
| AI / LLM usage tab | v1.0 |
| UC Graph tab | v1.0 |
| Job SLA tab | v1.0 |
| Storage Insights tab | v1.0 |
| Lineage & ML tab | v1.0 |
| MOCK_MODE toggle (mock_system_* tables) | v1.0 |
| GitHub Actions deploy pipeline (dev + prod targets) | v1.0 |
| Security hardening (CSP, rate limiting, admin guards, input validation) | v1.0 |
| Elastic License 2.0 | v1.0 |

---

> Want to work on something here? Open an issue, comment on an existing one, or jump straight to a PR. See [CONTRIBUTING.md](CONTRIBUTING.md) for the contribution workflow.
