# Contributing to Databricks Cost Observability

Thank you for your interest in contributing! This project is community-driven and welcomes contributions of all kinds — bug fixes, new dashboard tabs, documentation improvements, and more.

---

## Ways to Contribute

| Type | Examples |
|------|---------|
| **Bug fix** | Fix a broken query, UI glitch, or deployment issue |
| **New tab / feature** | Add a new dashboard tab (e.g. Serverless Cost, Unity Catalog Audit) |
| **Mock data** | Add more realistic sample data for a tab |
| **Documentation** | Improve the README, add setup guides, write a blog post |
| **Cloud-specific** | Test and fix issues on Azure / GCP |
| **Tests** | Add smoke tests or integration tests |

---

## Getting Started

### 1. Fork the repo

Click **Fork** at the top of the GitHub page. This creates your own copy at `github.com/<your-username>/databricks-cost-observability`.

### 2. Clone your fork

```bash
git clone https://github.com/<your-username>/databricks-cost-observability.git
cd databricks-cost-observability
```

### 3. Set up locally

```bash
pip install -r requirements.txt

export DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
export DATABRICKS_TOKEN=dapi...
export DATABRICKS_WAREHOUSE_ID=your-warehouse-id
export MOCK_MODE=true

uvicorn app:app --reload --port 8000
```

### 4. Create a branch

```bash
git checkout -b feat/your-feature-name
# or
git checkout -b fix/your-bug-description
```

### 5. Make your changes, then open a Pull Request

Push your branch and open a PR against `main`.

---

## Pull Request Guidelines

- **One PR per change** — keep PRs focused and small
- **Describe what and why** — not just what the code does
- **MOCK_MODE compatibility** — if you add a new SQL query, make sure it works with `MOCK_MODE=true` (add a mock table entry in `scripts/setup_mock_tables.py` if needed)
- **No org-specific values** — no hardcoded emails, workspace IDs, or company names in committed code
- **Test it** — run the app locally before submitting

### Branch naming

| Prefix | Use for |
|--------|---------|
| `feat/` | New feature or tab |
| `fix/` | Bug fix |
| `docs/` | Documentation only |
| `chore/` | Tooling, deps, CI |
| `mock/` | Mock data improvements |

---

## Adding a New Dashboard Tab

1. Create `services/your_service.py` — SQL queries + analysis logic
2. Create `api/v1/your_tab.py` — FastAPI router
3. Register the router in `api/router.py`
4. Add mock table entries to `scripts/setup_mock_tables.py` for any new `system.X` tables
5. Add the new `system.X` schema to `_MOCK_TABLE_MAP` in `core/sql_executor.py`
6. Add the tab to the frontend in `static/index.html`

---

## Reporting Bugs

Use the **Bug Report** issue template. Include:
- Databricks cloud (AWS / Azure / GCP)
- Whether you're using `MOCK_MODE=true` or real system tables
- The full error message from the app logs or browser console

---

## Code Style

- Python: follow PEP 8, no unused imports
- SQL: uppercase keywords, one clause per line for readability
- No comments explaining *what* the code does — only *why* if non-obvious
- No print statements in production paths — use `logging`

---

## License and Usage

This project is licensed under the **[Elastic License 2.0](LICENSE)** — source available, not open source.

By contributing, you agree your contributions will be covered by the same license.

**You may:**
- Fork this repo to contribute back via Pull Request
- Deploy it internally within your own organisation
- Reference, cite, or link to this project

**You may not:**
- Offer this software as a hosted or managed service to third parties
- Fork and redistribute it independently under a different name or brand
- Remove or alter the license or copyright notices

If you want to showcase or reference this project publicly, please **link to this repository** rather than copying the code independently.
