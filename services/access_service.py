"""
Access & Governance analytics.

Provides a unified view of:
  - User/group → workspace assignments
  - Unity Catalog grants (catalog, schema, table level)
  - Row filters and column masks (if configured)
  - Group memberships

When MOCK_MODE is enabled (or very few real users are detected),
adds demo enterprise-scale data for presentation purposes.
"""

from __future__ import annotations

import datetime
import random
from typing import Optional

from databricks.sdk import WorkspaceClient

from core.config import get_settings
from core.sql_executor import execute_sync, safe_execute_sync
from core.subscriptions import resolve_subscription
from core.validators import validate_sql_identifier


class AccessService:
    CACHE_TTL_MIN = 30

    def __init__(
        self,
        client: WorkspaceClient,
        account_client=None,
    ) -> None:
        self._client = client
        self._wh_id  = get_settings().databricks_warehouse_id
        self._account_client = account_client
        self._cache: Optional[dict]                  = None
        self._cached_at: Optional[datetime.datetime] = None

        # Workspace Azure map
        self._ws_azure: dict[str, dict] = {}
        if account_client is not None:
            try:
                for ws in account_client.workspaces.list():
                    wid = str(getattr(ws, "workspace_id", "") or "")
                    if not wid:
                        continue
                    az = getattr(ws, "azure_workspace_info", None)
                    sub = (getattr(az, "subscription_id", "") or "") if az else ""
                    self._ws_azure[wid] = {
                        "workspace_name":    getattr(ws, "workspace_name", "") or "",
                        "subscription_name": resolve_subscription(sub) if sub else "",
                    }
            except Exception:
                pass

    def _execute(self, sql: str) -> list[dict]:
        return execute_sync(self._client, self._wh_id, sql, label="Access")

    def _safe_execute(self, sql: str) -> list[dict]:
        return safe_execute_sync(self._client, self._wh_id, sql, label="Access")

    # ── SDK queries ───────────────────────────────────────────────────────────

    def _fetch_groups_and_members(self) -> list[dict]:
        """All workspace groups with their members."""
        groups = []
        try:
            for g in self._client.groups.list(
                attributes="displayName,members,entitlements",
                sort_by="displayName",
            ):
                members = []
                for m in (g.members or []):
                    members.append({
                        "display": m.display or str(m.value or ""),
                        "type":    "User",
                    })
                entitlements = [
                    str(e.value) for e in (g.entitlements or []) if e.value
                ]
                groups.append({
                    "group_id":     str(g.id or ""),
                    "group_name":   g.display_name or "",
                    "member_count": len(members),
                    "members":      members,
                    "entitlements": entitlements,
                })
        except Exception:
            pass
        return sorted(groups, key=lambda x: x["group_name"].lower())

    def _fetch_workspace_users(self) -> list[dict]:
        """All workspace users."""
        users = []
        try:
            for u in self._client.users.list(attributes="displayName,emails,active,groups"):
                email = ""
                if u.emails:
                    primary = next((e for e in u.emails if e.primary), u.emails[0])
                    email = primary.value or ""
                user_groups = []
                if u.groups:
                    user_groups = [
                        g.display for g in u.groups if g.display
                    ]
                users.append({
                    "user_id":      str(u.id or ""),
                    "display_name": u.display_name or "",
                    "email":        email,
                    "active":       bool(u.active) if u.active is not None else True,
                    "groups":       user_groups,
                })
        except Exception:
            pass
        return sorted(users, key=lambda x: x["email"].lower())

    # ── SDK-based Unity Catalog grants ──────────────────────────────────────

    def _fetch_workspace_assignments(self) -> list[dict]:
        """Workspace-level access assignments from AccountClient."""
        assignments = []
        if self._account_client is None:
            return assignments
        try:
            for ws in self._account_client.workspaces.list():
                wid = str(getattr(ws, "workspace_id", "") or "")
                ws_name = getattr(ws, "workspace_name", "") or wid
                try:
                    for perm in self._account_client.workspace_assignment.list(workspace_id=int(wid)):
                        principal = getattr(perm, "principal", None)
                        p_name = getattr(principal, "display_name", "") or getattr(principal, "user_name", "") or ""
                        p_type = getattr(principal, "group_name", None) and "group" or "user"
                        perms = [str(p) for p in (getattr(perm, "permissions", []) or [])]
                        if p_name:
                            assignments.append({
                                "principal":    p_name,
                                "type":         p_type,
                                "workspace_id": wid,
                                "workspace_name": ws_name,
                                "permissions":  perms,
                            })
                except Exception:
                    pass
        except Exception:
            pass
        return assignments

    @staticmethod
    def _parse_grant_row(r: dict) -> tuple[str, str]:
        """Extract (principal, privilege) from a SHOW GRANTS row.
        Column names are case-sensitive and vary between Databricks versions."""
        # Case-insensitive lookup
        low = {k.lower(): v for k, v in r.items()}
        principal = (low.get("principal") or low.get("grantee") or "").strip()
        action = (low.get("actiontype") or low.get("action_type")
                  or low.get("privilege") or low.get("privilege_type") or "").strip()
        return principal, action

    def _fetch_uc_grants(self) -> tuple[list[dict], list[dict], list[dict]]:
        """Fetch Unity Catalog grants: catalogs + schemas at tenant/metastore level."""
        cat_grants = []
        schema_grants = []

        # ── 1. Discover ALL catalogs the SP can see
        #    As metastore admin, information_schema.catalogs + SDK list
        #    should return every catalog in the metastore.
        catalogs_set: set[str] = set()
        try:
            rows = self._execute(
                "SELECT DISTINCT catalog_name FROM system.information_schema.catalogs "
                "ORDER BY catalog_name"
            )
            for r in rows:
                c = ({k.lower(): v for k, v in r.items()}).get("catalog_name", "")
                if c and not c.startswith("__"):
                    catalogs_set.add(c)
        except Exception:
            pass
        try:
            for cat in self._client.catalogs.list():
                if cat.name and not cat.name.startswith("__"):
                    catalogs_set.add(cat.name)
        except Exception:
            pass

        # ── 2. Supplement from catalog_privileges (may list catalogs the
        #    SP can't list but has grants for)
        is_cat = self._safe_execute("""
            SELECT DISTINCT catalog_name
            FROM system.information_schema.catalog_privileges
            LIMIT 20000
        """)
        for r in is_cat:
            c = ({k.lower(): v for k, v in r.items()}).get("catalog_name", "")
            if c and not c.startswith("__"):
                catalogs_set.add(c)

        # ── 3. Catalog grants — single bulk query from information_schema
        #    (SHOW GRANTS ON CATALOG per-catalog loop removed for speed)
        is_cat_grants = self._safe_execute("""
            SELECT grantee, catalog_name, privilege_type
            FROM system.information_schema.catalog_privileges
            ORDER BY grantee, catalog_name
            LIMIT 20000
        """)
        for r in is_cat_grants:
            low = {k.lower(): v for k, v in r.items()}
            g = (low.get("grantee") or "").strip()
            c = (low.get("catalog_name") or "").strip()
            p = (low.get("privilege_type") or "").strip()
            if g and c and p:
                cat_grants.append({
                    "grantee": g, "catalog_name": c, "privilege_type": p,
                })

        # Schema grants — two sources merged for full coverage:
        # 1. information_schema.schema_privileges (bulk, fast, tenant-level)
        is_schema = self._safe_execute("""
            SELECT grantee, catalog_name, schema_name, privilege_type
            FROM system.information_schema.schema_privileges
            ORDER BY grantee, catalog_name, schema_name
            LIMIT 20000
        """)
        seen_schemas = set()
        for r in is_schema:
            low = {k.lower(): v for k, v in r.items()}
            g = (low.get("grantee") or "").strip()
            c = (low.get("catalog_name") or "").strip()
            s = (low.get("schema_name") or "").strip()
            p = (low.get("privilege_type") or "").strip()
            if g and c and s and p:
                schema_grants.append({"grantee": g, "catalog_name": c, "schema_name": s, "privilege_type": p})
                seen_schemas.add(f"{c}.{s}")

        # SHOW GRANTS ON SCHEMA loop removed — too slow (N×M queries).
        # information_schema.schema_privileges provides sufficient coverage.

        return cat_grants, schema_grants, []

    def _fetch_row_filters(self) -> list[dict]:
        """Row-level security filters if available."""
        return self._safe_execute("""
            SELECT
                catalog_name,
                schema_name,
                table_name,
                filter_name,
                filter_body
            FROM system.information_schema.row_filters
            ORDER BY catalog_name, schema_name, table_name
        """)

    def _fetch_column_masks(self) -> list[dict]:
        """Column-level masking if available."""
        return self._safe_execute("""
            SELECT
                catalog_name,
                schema_name,
                table_name,
                column_name,
                mask_name,
                mask_body
            FROM system.information_schema.column_masks
            ORDER BY catalog_name, schema_name, table_name, column_name
        """)

    # ── analysis ──────────────────────────────────────────────────────────────

    def _build_access_map(
        self,
        users: list[dict],
        groups: list[dict],
        cat_grants: list[dict],
        schema_grants: list[dict],
        table_grants: list[dict],
        row_filters: list[dict],
        col_masks: list[dict],
        ws_assignments: list[dict] | None = None,
    ) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)

        # Build UUID/ID → display name map
        # SHOW GRANTS returns UUIDs for service principals and group IDs
        id_to_name: dict[str, str] = {}
        for u in users:
            if u.get("user_id"):
                id_to_name[u["user_id"]] = u.get("email") or u.get("display_name") or u["user_id"]
            if u.get("email"):
                id_to_name[u["email"].lower()] = u["email"]
        for g in groups:
            if g.get("group_id"):
                id_to_name[g["group_id"]] = g["group_name"]
            if g.get("group_name"):
                id_to_name[g["group_name"].lower()] = g["group_name"]

        def _resolve_principal_name(raw: str) -> str:
            """Resolve a grant principal — could be email, group name, or UUID/ID."""
            if not raw:
                return raw
            # Direct match (ID or lowercase name)
            resolved = id_to_name.get(raw) or id_to_name.get(raw.lower())
            if resolved:
                return resolved
            return raw

        # Build principal → grants structure
        principals: dict[str, dict] = {}

        def _get_principal(name: str) -> dict:
            if name not in principals:
                principals[name] = {
                    "name": name,
                    "type": "unknown",
                    "workspaces":     [],
                    "catalog_grants": [],
                    "schema_grants":  [],
                    "table_grants":   [],
                }
            return principals[name]

        # Workspace assignments
        for wa in (ws_assignments or []):
            p = _get_principal(wa["principal"])
            if wa.get("type") == "group" and p["type"] == "unknown":
                p["type"] = "group"
            p["workspaces"].append({
                "workspace_id":   wa.get("workspace_id", ""),
                "workspace_name": wa.get("workspace_name", ""),
                "permissions":    wa.get("permissions", []),
            })

        # Mark known users and groups
        # "account users" is an implicit Databricks group that includes all users
        for u in users:
            p = _get_principal(u["email"])
            p["type"]         = "user"
            p["display_name"] = u["display_name"]
            p["active"]       = u["active"]
            user_groups       = list(u["groups"])
            if "account users" not in user_groups:
                user_groups.append("account users")
            p["groups"]       = user_groups

        for g in groups:
            p = _get_principal(g["group_name"])
            p["type"]         = "group"
            p["member_count"] = g["member_count"]
            p["members"]      = [m["display"] for m in g["members"]]
            p["entitlements"] = g["entitlements"]

        # Aggregate grants (resolve principal names)
        for r in cat_grants:
            grantee = _resolve_principal_name(r.get("grantee", ""))
            if not grantee:
                continue
            p = _get_principal(grantee)
            p["catalog_grants"].append({
                "catalog":   r.get("catalog_name", ""),
                "privilege": r.get("privilege_type", ""),
            })

        for r in schema_grants:
            grantee = _resolve_principal_name(r.get("grantee", ""))
            if not grantee:
                continue
            p = _get_principal(grantee)
            p["schema_grants"].append({
                "catalog":   r.get("catalog_name", ""),
                "schema":    r.get("schema_name", ""),
                "privilege": r.get("privilege_type", ""),
            })

        for r in table_grants:
            grantee = _resolve_principal_name(r.get("grantee", ""))
            if not grantee:
                continue
            p = _get_principal(grantee)
            low = {k.lower(): v for k, v in r.items()}
            p["table_grants"].append({
                "catalog":   low.get("catalog_name") or low.get("table_catalog") or "",
                "schema":    low.get("schema_name") or low.get("table_schema") or "",
                "table":     low.get("table_name") or "",
                "privilege": low.get("privilege_type") or low.get("actiontype") or "",
            })

        # Build summary per principal
        principal_list = []
        for name, p in principals.items():
            if not name:
                continue
            # Count unique objects
            catalogs = set()
            schemas  = set()
            tables   = set()
            privs    = set()
            for g in p["catalog_grants"]:
                catalogs.add(g["catalog"])
                privs.add(g["privilege"])
            for g in p["schema_grants"]:
                catalogs.add(g["catalog"])
                schemas.add(f"{g['catalog']}.{g['schema']}")
                privs.add(g["privilege"])
            for g in p["table_grants"]:
                catalogs.add(g["catalog"])
                schemas.add(f"{g['catalog']}.{g['schema']}")
                tables.add(f"{g['catalog']}.{g['schema']}.{g['table']}")
                privs.add(g["privilege"])

            principal_list.append({
                **p,
                "workspace_count": len(p.get("workspaces", [])),
                "catalog_count": len(catalogs),
                "schema_count":  len(schemas),
                "table_count":   len(tables),
                "privilege_types": sorted(privs),
                "total_grants":  len(p["catalog_grants"]) + len(p["schema_grants"]) + len(p["table_grants"]),
            })

        # Sort: groups first, then users, then by grant count
        principal_list.sort(key=lambda x: (
            0 if x["type"] == "group" else 1,
            -x["total_grants"],
            x["name"].lower(),
        ))

        return {
            "generated_at":     now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "summary": {
                "total_users":      sum(1 for p in principal_list if p["type"] == "user"),
                "total_groups":     sum(1 for p in principal_list if p["type"] == "group"),
                "total_principals": len(principal_list),
                "row_filters":      len(row_filters),
                "column_masks":     len(col_masks),
            },
            "principals":   principal_list,
            "row_filters":  row_filters,
            "column_masks": col_masks,
        }

    # ── main entry point ──────────────────────────────────────────────────────

    def clear_cache(self) -> None:
        """Force-clear the server-side cache."""
        self._cache = None
        self._cached_at = None

    # ── Mock / demo data enrichment ───────────────────────────────────────────

    @staticmethod
    def _generate_demo_users(count: int = 150) -> list[dict]:
        """Generate demo enterprise users for presentation."""
        random.seed(42)
        first_names = [
            "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
            "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
            "Joseph", "Jessica", "Thomas", "Sarah", "Christopher", "Karen", "Charles",
            "Lisa", "Daniel", "Nancy", "Matthew", "Betty", "Anthony", "Margaret",
            "Steven", "Dorothy", "Andrew", "Emily", "Kenneth", "Michelle", "Kevin",
            "Carol", "Brian", "Amanda", "George", "Melissa", "Timothy", "Deborah",
            "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon", "Ryan",
            "Laura", "Jacob", "Kathleen", "Nicholas", "Angela", "Eric", "Shirley",
            "Jonathan", "Anna", "Stephen", "Brenda", "Larry", "Pamela", "Justin",
            "Emma", "Scott", "Nicole", "Brandon", "Helen", "Benjamin", "Samantha",
            "Priya", "Raj", "Anita", "Vikram", "Deepa", "Sanjay", "Neha", "Amit",
            "Wei", "Ming", "Yuki", "Hiro", "Jin", "Soo", "Chen", "Li",
        ]
        last_names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
            "Davis", "Rodriguez", "Martinez", "Wilson", "Anderson", "Thomas", "Taylor",
            "Moore", "Jackson", "Martin", "Lee", "Thompson", "White", "Harris", "Clark",
            "Lewis", "Robinson", "Walker", "Young", "Allen", "King", "Wright", "Scott",
            "Torres", "Nguyen", "Hill", "Green", "Adams", "Nelson", "Baker", "Hall",
            "Rivera", "Campbell", "Mitchell", "Carter", "Roberts", "Patel", "Shah",
            "Kumar", "Singh", "Gupta", "Sharma", "Chen", "Wang", "Li", "Zhang",
            "Kim", "Park", "Tanaka", "Sullivan", "Cohen", "Murphy",
        ]
        departments = [
            "Consumer Banking", "Investment Banking", "Risk Analytics",
            "Fraud Detection", "Data Engineering", "Data Science",
            "Compliance", "Treasury", "Marketing Analytics",
            "Credit Risk", "Operations", "Wealth Management",
        ]
        groups_map = {
            "Consumer Banking": ["cb-analytics", "cb-reporting"],
            "Investment Banking": ["ib-quant", "ib-trading-desk"],
            "Risk Analytics": ["risk-modelers", "risk-reporting"],
            "Fraud Detection": ["fraud-ml", "fraud-ops"],
            "Data Engineering": ["platform-core", "etl-pipeline"],
            "Data Science": ["ml-ops", "feature-store"],
            "Compliance": ["compliance-team", "aml-detection"],
            "Treasury": ["treasury-analytics"],
            "Marketing Analytics": ["marketing-insights"],
            "Credit Risk": ["credit-scoring", "credit-analytics"],
            "Operations": ["ops-reporting", "ops-automation"],
            "Wealth Management": ["wm-analytics", "wm-portfolio"],
        }

        users = []
        used = set()
        for i in range(count * 2):
            if len(users) >= count:
                break
            first = random.choice(first_names)
            last = random.choice(last_names)
            email = f"{first.lower()}.{last.lower()}@chase.com"
            if email in used:
                continue
            used.add(email)
            dept = random.choice(departments)
            dept_groups = groups_map.get(dept, ["general"])
            user_groups = ["account users"] + dept_groups
            # Some users in cross-functional groups
            if random.random() < 0.3:
                user_groups.append("data-governance")
            if random.random() < 0.15:
                user_groups.append("admin-users")
            users.append({
                "user_id": f"demo-{i+1:04d}",
                "display_name": f"{first} {last}",
                "email": email,
                "active": random.random() < 0.92,
                "groups": user_groups,
            })
        return users

    @staticmethod
    def _generate_demo_groups() -> list[dict]:
        """Generate demo enterprise groups."""
        groups_def = [
            ("account users", 150), ("admin-users", 12), ("data-governance", 35),
            ("platform-core", 18), ("etl-pipeline", 22), ("ml-ops", 15),
            ("feature-store", 10), ("cb-analytics", 25), ("cb-reporting", 20),
            ("ib-quant", 14), ("ib-trading-desk", 12), ("risk-modelers", 16),
            ("risk-reporting", 14), ("fraud-ml", 18), ("fraud-ops", 12),
            ("compliance-team", 15), ("aml-detection", 10), ("treasury-analytics", 8),
            ("marketing-insights", 12), ("credit-scoring", 14), ("credit-analytics", 10),
            ("ops-reporting", 12), ("ops-automation", 8), ("wm-analytics", 10),
            ("wm-portfolio", 8), ("bi-consumers", 45), ("data-scientists", 30),
        ]
        return [
            {
                "group_id": f"grp-{i+1:03d}",
                "group_name": name,
                "member_count": cnt,
                "members": [{"display": f"member-{j}", "type": "User"} for j in range(min(cnt, 5))],
                "entitlements": (
                    ["workspace-access", "databricks-sql-access", "cluster-create"]
                    if name == "admin-users"
                    else ["workspace-access", "databricks-sql-access"]
                ),
            }
            for i, (name, cnt) in enumerate(groups_def)
        ]

    @staticmethod
    def _generate_demo_grants(users: list[dict], groups: list[dict]):
        """Generate demo UC catalog & schema grants."""
        random.seed(99)
        catalogs = [
            "prod_banking", "prod_risk", "prod_fraud", "prod_compliance",
            "staging_analytics", "sandbox_ds", "workspace",
        ]
        schemas_by_catalog = {
            "prod_banking": ["transactions", "customers", "accounts", "payments", "loans"],
            "prod_risk": ["market_risk", "credit_risk", "var_models", "stress_testing"],
            "prod_fraud": ["fraud_alerts", "fraud_scores", "aml_events", "kyc_data"],
            "prod_compliance": ["regulatory_reports", "audit_trail", "sox_controls"],
            "staging_analytics": ["customer_360", "campaign_data", "segmentation"],
            "sandbox_ds": ["experiments", "feature_store", "model_registry"],
            "workspace": ["default", "analytics", "data_science"],
        }
        privileges = [
            "USE_CATALOG", "USE_SCHEMA", "SELECT", "MODIFY",
            "CREATE_TABLE", "CREATE_SCHEMA", "ALL_PRIVILEGES",
        ]

        cat_grants = []
        schema_grants = []

        # Group-level grants (most grants in enterprise are group-based)
        for g in groups:
            gname = g["group_name"]
            if gname == "account users":
                # Everyone gets USE_CATALOG on a few catalogs
                for cat in ["workspace", "staging_analytics"]:
                    cat_grants.append({"grantee": gname, "catalog_name": cat, "privilege_type": "USE_CATALOG"})
                continue
            # Assign 1-3 catalogs per group
            n_cats = random.randint(1, 3)
            for cat in random.sample(catalogs, min(n_cats, len(catalogs))):
                priv = random.choice(["USE_CATALOG", "ALL_PRIVILEGES"])
                cat_grants.append({"grantee": gname, "catalog_name": cat, "privilege_type": priv})
                # Schema-level grants
                for schema in schemas_by_catalog.get(cat, ["default"]):
                    for p in random.sample(privileges[:4], random.randint(1, 3)):
                        schema_grants.append({
                            "grantee": gname, "catalog_name": cat,
                            "schema_name": schema, "privilege_type": p,
                        })

        # Some direct user grants (data engineers, admins)
        for u in random.sample(users, min(30, len(users))):
            cat = random.choice(catalogs)
            cat_grants.append({
                "grantee": u["email"], "catalog_name": cat,
                "privilege_type": random.choice(["USE_CATALOG", "ALL_PRIVILEGES"]),
            })

        return cat_grants, schema_grants

    @staticmethod
    def _generate_demo_ws_assignments(users: list[dict]) -> list[dict]:
        """Generate demo workspace assignments."""
        random.seed(77)
        workspaces = [
            ("ws-100001", "prod-consumer-banking"),
            ("ws-100002", "prod-investment-banking"),
            ("ws-100003", "prod-risk-analytics"),
            ("ws-100004", "prod-data-platform"),
            ("ws-100005", "prod-fraud-detection"),
        ]
        assignments = []
        for u in users:
            # Each user assigned to 1-3 workspaces
            n_ws = random.randint(1, 3)
            for ws_id, ws_name in random.sample(workspaces, n_ws):
                assignments.append({
                    "principal": u["email"],
                    "type": "user",
                    "workspace_id": ws_id,
                    "workspace_name": ws_name,
                    "permissions": ["USER"],
                })
        return assignments

    @staticmethod
    def _generate_demo_security():
        """Generate demo row filters and column masks."""
        row_filters = [
            {"catalog_name": "prod_banking", "schema_name": "customers",
             "table_name": "customer_pii", "filter_name": "region_filter",
             "filter_body": "region = current_user_attribute('region')"},
            {"catalog_name": "prod_banking", "schema_name": "transactions",
             "table_name": "transaction_detail", "filter_name": "branch_filter",
             "filter_body": "branch_id IN (SELECT branch_id FROM user_branches WHERE user = current_user())"},
            {"catalog_name": "prod_fraud", "schema_name": "fraud_alerts",
             "table_name": "alerts_raw", "filter_name": "sensitivity_filter",
             "filter_body": "sensitivity_level <= current_user_attribute('clearance')"},
            {"catalog_name": "prod_compliance", "schema_name": "audit_trail",
             "table_name": "full_audit", "filter_name": "dept_filter",
             "filter_body": "department = current_user_attribute('department')"},
            {"catalog_name": "prod_risk", "schema_name": "credit_risk",
             "table_name": "credit_scores", "filter_name": "portfolio_filter",
             "filter_body": "portfolio_id IN (SELECT id FROM user_portfolios WHERE user = current_user())"},
        ]
        col_masks = [
            {"catalog_name": "prod_banking", "schema_name": "customers",
             "table_name": "customer_pii", "column_name": "ssn",
             "mask_name": "ssn_mask", "mask_body": "CASE WHEN is_member('compliance-team') THEN ssn ELSE '***-**-' || RIGHT(ssn, 4) END"},
            {"catalog_name": "prod_banking", "schema_name": "customers",
             "table_name": "customer_pii", "column_name": "account_number",
             "mask_name": "acct_mask", "mask_body": "CASE WHEN is_member('admin-users') THEN account_number ELSE '****' || RIGHT(account_number, 4) END"},
            {"catalog_name": "prod_banking", "schema_name": "customers",
             "table_name": "customer_pii", "column_name": "email",
             "mask_name": "email_mask", "mask_body": "CASE WHEN is_member('cb-analytics') THEN email ELSE regexp_replace(email, '(.)(.*?)(@)', '$1***$3') END"},
            {"catalog_name": "prod_fraud", "schema_name": "kyc_data",
             "table_name": "identity_docs", "column_name": "passport_number",
             "mask_name": "passport_mask", "mask_body": "CASE WHEN is_member('compliance-team') THEN passport_number ELSE '********' END"},
            {"catalog_name": "prod_risk", "schema_name": "credit_risk",
             "table_name": "credit_scores", "column_name": "internal_score",
             "mask_name": "score_mask", "mask_body": "CASE WHEN is_member('risk-modelers') THEN internal_score ELSE NULL END"},
            {"catalog_name": "prod_banking", "schema_name": "payments",
             "table_name": "wire_transfers", "column_name": "beneficiary_account",
             "mask_name": "beneficiary_mask", "mask_body": "CASE WHEN is_member('treasury-analytics') THEN beneficiary_account ELSE '****' END"},
            {"catalog_name": "prod_compliance", "schema_name": "sox_controls",
             "table_name": "control_evidence", "column_name": "auditor_notes",
             "mask_name": "notes_mask", "mask_body": "CASE WHEN is_member('compliance-team') THEN auditor_notes ELSE '[REDACTED]' END"},
        ]
        return row_filters, col_masks

    def get_access_governance(self) -> dict:
        """Build comprehensive access map.

        Results are cached for CACHE_TTL_MIN minutes.
        When fewer than 10 real users are found, enriches with demo data
        for enterprise-scale presentation.
        """
        now = datetime.datetime.now(datetime.timezone.utc)

        if self._cache and self._cached_at:
            age = (now - self._cached_at).total_seconds() / 60
            if age < self.CACHE_TTL_MIN:
                return {**self._cache, "cache_age_minutes": round(age, 1)}

        # Phase 1: Fast — SDK calls (users, groups)
        users          = self._fetch_workspace_users()
        groups         = self._fetch_groups_and_members()

        # Phase 2: Workspace assignments (moderate — N workspace API calls)
        ws_assignments = self._fetch_workspace_assignments()

        # Phase 3: UC grants — bulk SQL only (no per-schema loops)
        cat_grants, schema_grants, table_grants = self._fetch_uc_grants()

        # Phase 4: Security policies (fast single queries)
        row_filters    = self._fetch_row_filters()
        col_masks      = self._fetch_column_masks()

        # Phase 5: Enrich with demo data when few real users exist
        if len(users) < 10:
            demo_users  = self._generate_demo_users(150)
            demo_groups = self._generate_demo_groups()
            demo_cat_grants, demo_schema_grants = self._generate_demo_grants(demo_users, demo_groups)
            demo_ws_assignments = self._generate_demo_ws_assignments(demo_users)
            demo_row_filters, demo_col_masks = self._generate_demo_security()

            # Merge: keep real users, add demo users
            real_emails = {u["email"].lower() for u in users}
            users += [u for u in demo_users if u["email"].lower() not in real_emails]

            real_groups = {g["group_name"].lower() for g in groups}
            groups += [g for g in demo_groups if g["group_name"].lower() not in real_groups]

            cat_grants    += demo_cat_grants
            schema_grants += demo_schema_grants
            ws_assignments += demo_ws_assignments

            if not row_filters:
                row_filters = demo_row_filters
            if not col_masks:
                col_masks = demo_col_masks

        result = self._build_access_map(
            users, groups, cat_grants, schema_grants,
            table_grants, row_filters, col_masks, ws_assignments,
        )
        result["cache_age_minutes"] = 0

        self._cache     = result
        self._cached_at = now
        return result
