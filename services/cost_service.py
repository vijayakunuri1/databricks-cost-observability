import re
import datetime
from typing import Optional

from databricks.sdk import WorkspaceClient, AccountClient

from core.config import get_settings
from core.sql_executor import execute_sync, safe_execute_sync
from core.validators import (
    validate_date,
    validate_workspace_id,
    validate_sku,
    validate_tag_key,
    escape_tag_value,
    validate_subscription_id,
    validate_resource_group,
)
from core.subscriptions import resolve_subscription, SUBSCRIPTION_MAP

# ── Anomaly helpers ───────────────────────────────────────────────────────────

_DEV_KEYWORDS = frozenset({
    "dev", "development", "sandbox", "nonprod",
})

_TEST_KEYWORDS = frozenset({
    "test", "testing", "qa", "staging", "stage",
})

_UAT_KEYWORDS = frozenset({"uat"})

_PROD_KEYWORDS = frozenset({"prod", "production", "prd", "live"})

# (critical_hours, warning_hours) per env tier for (service, job_run)
_TIER_THRESHOLDS: dict[str, tuple[tuple[float, float], tuple[float, float]]] = {
    "dev":   ((8.0,  4.0),  (4.0,  2.0)),
    "test":  ((12.0, 6.0),  (6.0,  3.0)),
    "uat":   ((24.0, 12.0), (8.0,  4.0)),
    "prod":  ((72.0, 48.0), (24.0, 8.0)),
    "other": ((8.0,  4.0),  (4.0,  2.0)),  # untagged clusters running >4h
}


def _check_environment(name: str, tags, workspace_name: str = "") -> tuple[str, str, str]:
    """Return (env_tier, env_label, detected_by).
    env_tier is one of: 'dev' | 'test' | 'uat' | 'prod' | ''
    Priority: compute tags → service name → workspace name.
    detected_by: 'tag' | 'name' | 'workspace'
    """
    tag_dict: dict = {}
    if isinstance(tags, dict):
        tag_dict = {k: str(v or "") for k, v in tags.items()}
    elif hasattr(tags, "custom_tags") and tags.custom_tags:
        for item in tags.custom_tags:
            tag_dict[getattr(item, "key", "")] = str(getattr(item, "value", "") or "")

    for key, val in tag_dict.items():
        if key.lower() in ("environment", "env", "deployment_env", "stage", "tier"):
            v = val.lower()
            if v in _DEV_KEYWORDS:
                return "dev", v, "tag"
            if v in _TEST_KEYWORDS:
                return "test", v, "tag"
            if v in _UAT_KEYWORDS:
                return "uat", v, "tag"
            if v in _PROD_KEYWORDS:
                return "prod", v, "tag"

    def _scan(text: str, source: str):
        t = text.lower()
        # Check dev first so "nonprod" is caught before "prod"
        for kw in sorted(_DEV_KEYWORDS, key=len, reverse=True):
            if kw in t:
                return "dev", kw, source
        for kw in sorted(_TEST_KEYWORDS, key=len, reverse=True):
            if kw in t:
                return "test", kw, source
        for kw in sorted(_UAT_KEYWORDS, key=len, reverse=True):
            if kw in t:
                return "uat", kw, source
        for kw in sorted(_PROD_KEYWORDS, key=len, reverse=True):
            if kw in t:
                return "prod", kw, source
        return "", "", ""

    if name:
        result = _scan(name, "name")
        if result[0]:
            return result

    if workspace_name:
        result = _scan(workspace_name, "workspace")
        if result[0]:
            return result

    return "", "", ""


def _ms_to_iso(ms) -> str:
    if not ms:
        return ""
    try:
        dt = datetime.datetime.fromtimestamp(int(ms) / 1000, tz=datetime.timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return ""


def _duration_hours(start_ms) -> float:
    if not start_ms:
        return 0.0
    now_ms = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
    return max(0.0, (now_ms - int(start_ms)) / 3_600_000)


def _anomaly_level(hours: float, critical: float = 8.0, warning: float = 4.0) -> str:
    if hours >= critical:
        return "critical"
    if hours >= warning:
        return "warning"
    return "info"


def _state_val(state_obj) -> str:
    if state_obj is None:
        return ""
    if hasattr(state_obj, "value"):
        return str(state_obj.value)
    return str(state_obj).split(".")[-1].upper()


def _parse_ts_str(ts_str: str):
    """Parse a Spark/SQL timestamp string to a UTC-aware datetime, or None."""
    if not ts_str:
        return None
    try:
        s = ts_str.strip().replace("T", " ").split(".")[0].split("+")[0][:19]
        dt = datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=datetime.timezone.utc)
    except Exception:
        return None


def _ts_to_duration_hours(ts_str: str) -> float:
    dt = _parse_ts_str(ts_str)
    if dt is None:
        return -1.0
    return max(0.0, (datetime.datetime.now(datetime.timezone.utc) - dt).total_seconds() / 3600)


def _ts_to_display(ts_str: str) -> str:
    dt = _parse_ts_str(ts_str)
    if dt is None:
        return ""
    return dt.strftime("%Y-%m-%d %H:%M UTC")


_DATE_RE      = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_WORKSPACE_RE = re.compile(r"^[A-Za-z0-9_\-]+$")
_SKU_RE       = re.compile(r"^[A-Z0-9_]+$")
_TAG_KEY_RE   = re.compile(r"^[A-Za-z0-9 _\-\.\:]+$")  # Azure tag keys

_PRODUCT_CASE = """
    CASE
        WHEN {col} LIKE '%ALL_PURPOSE%' THEN 'All Purpose Compute'
        WHEN {col} LIKE '%JOBS%'        THEN 'Jobs Compute'
        WHEN {col} LIKE '%SQL%'         THEN 'SQL Warehouse'
        WHEN {col} LIKE '%DLT%' OR {col} LIKE '%DELTA_LIVE%' THEN 'Delta Live Tables'
        WHEN {col} LIKE '%SERVING%' OR {col} LIKE '%MODEL_INFERENCE%' THEN 'Model Serving'
        WHEN {col} LIKE '%VECTOR_SEARCH%' THEN 'Vector Search'
        ELSE 'Other'
    END
"""


def _product_case(col: str = "sku_name") -> str:
    return _PRODUCT_CASE.format(col=col)


def _product_case_py(sku: str) -> str:
    """Python version of product category mapping — more granular."""
    s = (sku or "").upper()
    if "SERVERLESS" in s and "SQL" in s:      return "Serverless SQL"
    if "SERVERLESS" in s and "JOBS" in s:     return "Serverless Jobs"
    if "SERVERLESS" in s and "ALL_PURPOSE" in s: return "Serverless Compute"
    if "SERVERLESS" in s and "INFERENCE" in s: return "Serverless Inference"
    if "SERVERLESS" in s and "DATABASE" in s:  return "Serverless Database"
    if "ALL_PURPOSE" in s:    return "Interactive Cluster"
    if "SQL" in s:            return "SQL Warehouse"
    if "JOBS" in s:           return "Jobs Cluster"
    if "DLT" in s or "DELTA_LIVE" in s: return "DLT Pipeline"
    if "MODEL_SERVING" in s or "FOUNDATION_MODEL" in s or "EXTERNAL_MODEL" in s: return "Model Serving"
    if "VECTOR_SEARCH" in s:  return "Vector Search"
    if "STORAGE" in s:        return "Storage"
    if "CONNECTIVITY" in s:   return "Connectivity"
    if "EGRESS" in s:         return "Egress"
    return "Other"


def _ui_path(r: dict) -> str:
    """Return a Databricks UI navigation path for a billing resource.

    The path is relative to the workspace root URL, e.g.
    '#/setting/clusters/0406-xyz789' so the frontend (or a human)
    can append it to https://<workspace-host>/ to deep-link.
    """
    cid  = r.get("cluster_id") or ""
    wid  = r.get("warehouse_id") or ""
    jid  = r.get("job_id") or ""
    jrid = r.get("job_run_id") or ""
    plid = r.get("dlt_pipeline_id") or ""
    nbid = r.get("notebook_id") or ""
    epnm = r.get("endpoint_name") or ""
    epid = r.get("endpoint_id") or ""
    prod = r.get("product") or r.get("product_category") or ""

    if wid:
        return f"sql/warehouses/{wid}"
    if plid:
        return f"#joblist/pipelines/{plid}"
    if epnm:
        return f"ml/endpoints/{epnm}"
    if epid:
        return f"ml/endpoints/{epid}"
    if "Serving" in prod or "Inference" in prod:
        return "ml/endpoints"
    if jid and jrid:
        return f"#job/{jid}/run/{jrid}"
    if jid:
        return f"#job/{jid}"
    if jrid:
        return f"#job/runs/{jrid}"
    if cid:
        if "Jobs" in prod:
            return f"#setting/clusters/{cid}/configuration"
        return f"#setting/clusters/{cid}"
    if nbid:
        return f"#notebook/{nbid}"
    return ""


class CostService:
    def __init__(self, client: WorkspaceClient, account_client: Optional[AccountClient] = None):
        self._client = client
        self._account_client = account_client
        self._warehouse_id = get_settings().databricks_warehouse_id

        # Resolve the current workspace ID for query scoping
        try:
            self._current_workspace_id = str(client.get_workspace_id())
        except Exception:
            self._current_workspace_id = get_settings().databricks_workspace_id

        # Resolve allowed workspace IDs for IDOR protection
        settings = get_settings()
        configured = settings.allowed_workspace_ids_set
        if "*" in configured:
            # Admin mode — no restriction on workspace queries
            self._allowed_ws: set[str] | None = None
        elif configured:
            self._allowed_ws = configured
        else:
            # Default: only the current workspace
            self._allowed_ws = {self._current_workspace_id} if self._current_workspace_id else None

        # Build workspace_id → Azure context map from AccountClient
        # Maps: ws_id → {subscription, resource_group, workspace_name}
        self._ws_azure: dict[str, dict] = {}
        if account_client is not None:
            try:
                for ws in account_client.workspaces.list():
                    wid = str(getattr(ws, "workspace_id", "") or "")
                    if not wid:
                        continue
                    az = getattr(ws, "azure_workspace_info", None)
                    sub = (getattr(az, "subscription_id", "") or "") if az else ""
                    rg  = (getattr(az, "resource_group",  "") or "") if az else ""
                    self._ws_azure[wid] = {
                        "workspace_name": getattr(ws, "workspace_name", "") or "",
                        "subscription":   sub,
                        "resource_group": rg,
                        "workspace_url":  getattr(ws, "workspace_url", "") or "",
                    }
            except Exception:
                pass

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _execute(self, statement: str) -> list[dict]:
        return execute_sync(
            self._client, self._warehouse_id, statement,
            wait_timeout="30s", label="Cost",
        )

    @staticmethod
    def _validate_date(value: str) -> str:
        return validate_date(value)

    @staticmethod
    def _validate_workspace(value: str) -> str:
        return validate_workspace_id(value)

    @staticmethod
    def _validate_sku(value: str) -> str:
        return validate_sku(value)

    @staticmethod
    def _validate_tag_key(value: str) -> str:
        return validate_tag_key(value)

    @staticmethod
    def _escape_tag_value(value: str) -> str:
        return escape_tag_value(value)

    _SUB_TAGS = ("SubscriptionId", "subscriptionid", "SubscriptionID",
                  "Subscription", "subscription", "AzureSubscriptionId",
                  "SubscriptionName", "subscriptionname")
    _RG_TAGS  = ("ResourceGroup", "resourcegroup", "ResourceGroupName",
                  "resource_group", "resourcegroupname", "RG", "rg")

    def _assert_workspace_access(self, workspace_id: str) -> None:
        """Raise ValueError if the requested workspace is not in the allowed set."""
        if self._allowed_ws is not None and workspace_id not in self._allowed_ws:
            raise ValueError("Access denied: workspace not in allowed set")

    def _where(
        self,
        start_date: str,
        end_date: str,
        workspace_id: Optional[str] = None,
        subscription: Optional[str] = None,
        resource_group: Optional[str] = None,
        tag_key: Optional[str] = None,
        tag_value: Optional[str] = None,
        alias: str = "u",
    ) -> str:
        p = f"{alias}." if alias else ""
        conds = [
            f"{p}usage_date >= '{self._validate_date(start_date)}'",
            f"{p}usage_date <= '{self._validate_date(end_date)}'",
        ]
        if workspace_id:
            ws_list = [w.strip() for w in workspace_id.split(",") if w.strip()]
            safe_ws_list = []
            for w in ws_list:
                safe_w = self._validate_workspace(w)
                self._assert_workspace_access(safe_w)
                safe_ws_list.append(safe_w)
            if len(safe_ws_list) == 1:
                conds.append(f"{p}workspace_id = '{safe_ws_list[0]}'")
            elif safe_ws_list:
                in_list = ", ".join(f"'{w}'" for w in safe_ws_list)
                conds.append(f"{p}workspace_id IN ({in_list})")
        elif self._allowed_ws is not None:
            # No explicit workspace requested — scope to allowed set
            in_list = ", ".join(f"'{self._validate_workspace(w)}'" for w in self._allowed_ws)
            conds.append(f"{p}workspace_id IN ({in_list})")
        if subscription:
            # Resolve subscription IDs → workspace IDs via account map
            sub_list = [validate_subscription_id(s.strip()) for s in subscription.split(",") if s.strip()]
            ws_ids = self._ws_ids_for_subscriptions(sub_list)
            if ws_ids:
                in_list = ", ".join(f"'{w}'" for w in ws_ids)
                conds.append(f"{p}workspace_id IN ({in_list})")
            else:
                conds.append("1 = 0")  # no matching workspaces
        if resource_group:
            # Resolve resource group → workspace IDs via account map
            safe_rg = validate_resource_group(resource_group.strip())
            rg_lower = safe_rg.lower()
            ws_ids = [
                wid for wid, info in self._ws_azure.items()
                if (info.get("resource_group", "")).lower() == rg_lower
            ]
            if ws_ids:
                in_list = ", ".join(f"'{w}'" for w in ws_ids)
                conds.append(f"{p}workspace_id IN ({in_list})")
            else:
                conds.append("1 = 0")
        if tag_key:
            safe_key = self._validate_tag_key(tag_key)
            if tag_value:
                safe_val = self._escape_tag_value(tag_value)
                conds.append(f"{p}custom_tags['{safe_key}'] = '{safe_val}'")
            else:
                conds.append(f"{p}custom_tags['{safe_key}'] IS NOT NULL")
        return " AND ".join(conds)

    _PRICE_JOIN = """
        LEFT JOIN system.billing.list_prices p
            ON u.sku_name = p.sku_name
            AND u.cloud    = p.cloud
            AND u.usage_start_time >= p.price_start_time
            AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
    """

    # ── Public API ────────────────────────────────────────────────────────────

    def get_summary(
        self,
        start_date: str,
        end_date: str,
        workspace_id: Optional[str] = None,
        subscription: Optional[str] = None,
        resource_group: Optional[str] = None,
        tag_key: Optional[str] = None,
        tag_value: Optional[str] = None,
    ) -> dict:
        where = self._where(start_date, end_date, workspace_id, subscription, resource_group, tag_key, tag_value)
        rows = self._execute(f"""
            SELECT
                COUNT(DISTINCT u.workspace_id)                                     AS workspace_count,
                COUNT(DISTINCT u.sku_name)                                         AS product_count,
                ROUND(SUM(u.usage_quantity), 2)                                    AS total_dbus,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2)   AS estimated_cost_usd
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            WHERE {where}
        """)
        return rows[0] if rows else {
            "workspace_count": "0",
            "product_count": "0",
            "total_dbus": "0",
            "estimated_cost_usd": "0",
        }

    def get_daily_trend(
        self,
        start_date: str,
        end_date: str,
        workspace_id: Optional[str] = None,
        subscription: Optional[str] = None,
        resource_group: Optional[str] = None,
        tag_key: Optional[str] = None,
        tag_value: Optional[str] = None,
    ) -> list[dict]:
        where = self._where(start_date, end_date, workspace_id, subscription, resource_group, tag_key, tag_value)
        return self._execute(f"""
            SELECT
                u.usage_date,
                {_product_case("u.sku_name")} AS product_category,
                ROUND(SUM(u.usage_quantity), 4) AS total_dbus,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2) AS estimated_cost_usd
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            WHERE {where}
            GROUP BY u.usage_date, product_category
            ORDER BY u.usage_date, product_category
        """)

    def get_by_product(
        self,
        start_date: str,
        end_date: str,
        workspace_id: Optional[str] = None,
        subscription: Optional[str] = None,
        resource_group: Optional[str] = None,
        tag_key: Optional[str] = None,
        tag_value: Optional[str] = None,
    ) -> list[dict]:
        where = self._where(start_date, end_date, workspace_id, subscription, resource_group, tag_key, tag_value)
        return self._execute(f"""
            SELECT
                u.sku_name,
                {_product_case("u.sku_name")} AS product_category,
                ROUND(SUM(u.usage_quantity), 2)                                  AS total_dbus,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2) AS estimated_cost_usd,
                COUNT(DISTINCT u.workspace_id)                                   AS workspace_count
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            WHERE {where}
            GROUP BY u.sku_name, product_category
            ORDER BY total_dbus DESC
        """)

    def get_by_workspace(
        self,
        start_date: str,
        end_date: str,
        workspace_id: Optional[str] = None,
        subscription: Optional[str] = None,
        resource_group: Optional[str] = None,
        tag_key: Optional[str] = None,
        tag_value: Optional[str] = None,
    ) -> list[dict]:
        where = self._where(start_date, end_date, workspace_id, subscription, resource_group, tag_key, tag_value)
        return self._execute(f"""
            SELECT
                u.workspace_id,
                ROUND(SUM(u.usage_quantity), 2)                                  AS total_dbus,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2) AS estimated_cost_usd,
                COUNT(DISTINCT u.sku_name)                                       AS product_count
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            WHERE {where}
            GROUP BY u.workspace_id
            ORDER BY total_dbus DESC
            LIMIT 20
        """)

    def get_spike_drilldown(self, spike_date: str) -> dict:
        """Break down a single day's cost by workspace, product, and top resources."""
        d = self._validate_date(spike_date)
        where = f"u.usage_date = '{d}'"
        # Scope to allowed workspaces
        if self._allowed_ws is not None:
            in_list = ", ".join(f"'{self._validate_workspace(w)}'" for w in self._allowed_ws)
            where += f" AND u.workspace_id IN ({in_list})"

        by_workspace = self._execute(f"""
            SELECT
                u.workspace_id,
                ROUND(SUM(u.usage_quantity), 2)                                  AS total_dbus,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2) AS estimated_cost_usd,
                COUNT(DISTINCT u.sku_name)                                       AS sku_count
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            WHERE {where}
            GROUP BY u.workspace_id
            ORDER BY estimated_cost_usd DESC
        """)

        # Top compute resources per workspace for this date
        ws_resources = self._execute(f"""
            SELECT
                u.workspace_id,
                COALESCE(
                    u.usage_metadata.cluster_id,
                    u.usage_metadata.warehouse_id,
                    CAST(u.usage_metadata.job_run_id AS STRING),
                    u.usage_metadata.dlt_pipeline_id
                )                                                                AS resource_id,
                u.sku_name,
                any_value(u.usage_metadata.cluster_id)                            AS cluster_id,
                any_value(u.usage_metadata.warehouse_id)                          AS warehouse_id,
                any_value(CAST(u.usage_metadata.job_id AS STRING))               AS job_id,
                any_value(CAST(u.usage_metadata.job_run_id AS STRING))            AS job_run_id,
                any_value(u.usage_metadata.dlt_pipeline_id)                       AS dlt_pipeline_id,
                any_value(CAST(u.usage_metadata.notebook_id AS STRING))           AS notebook_id,
                any_value(u.usage_metadata.endpoint_name)                         AS endpoint_name,
                any_value(u.usage_metadata.endpoint_id)                           AS endpoint_id,
                any_value(u.identity_metadata.run_as)                             AS run_as,
                ROUND(SUM(u.usage_quantity), 2)                                  AS dbus,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2) AS cost
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            WHERE {where}
              AND (u.usage_metadata.cluster_id IS NOT NULL
                   OR u.usage_metadata.warehouse_id IS NOT NULL
                   OR u.usage_metadata.job_run_id IS NOT NULL
                   OR u.usage_metadata.dlt_pipeline_id IS NOT NULL)
            GROUP BY u.workspace_id, resource_id, u.sku_name
            ORDER BY cost DESC
        """)
        # Group resources by workspace
        ws_res_map: dict[str, list] = {}
        for r in ws_resources:
            wid = r.get("workspace_id", "")
            rid = r.get("resource_id", "")
            if wid and rid:
                az = self._ws_azure.get(wid, {})
                entry = {
                    "resource_id": rid,
                    "product": _product_case_py(r.get("sku_name", "")),
                    "dbus": float(r.get("dbus") or 0),
                    "cost": float(r.get("cost") or 0),
                    "cluster_id": r.get("cluster_id") or None,
                    "warehouse_id": r.get("warehouse_id") or None,
                    "job_id": r.get("job_id") or None,
                    "job_run_id": r.get("job_run_id") or None,
                    "dlt_pipeline_id": r.get("dlt_pipeline_id") or None,
                    "notebook_id": r.get("notebook_id") or None,
                    "endpoint_name": r.get("endpoint_name") or None,
                    "endpoint_id": r.get("endpoint_id") or None,
                    "run_as": r.get("run_as") or None,
                    "workspace_url": az.get("workspace_url", ""),
                }
                entry["ui_path"] = _ui_path(entry)
                ws_res_map.setdefault(wid, []).append(entry)

        by_product = self._execute(f"""
            SELECT
                u.sku_name,
                {_product_case("u.sku_name")} AS product_category,
                u.workspace_id,
                ROUND(SUM(u.usage_quantity), 2)                                  AS total_dbus,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2) AS estimated_cost_usd
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            WHERE {where}
            GROUP BY u.sku_name, product_category, u.workspace_id
            ORDER BY estimated_cost_usd DESC
        """)
        # Enrich by_product with workspace info
        for r in by_product:
            wid = r.get("workspace_id", "")
            az = self._ws_azure.get(wid, {})
            r["workspace_name"]    = az.get("workspace_name", "")
            r["subscription_name"] = az.get("subscription_name", "")
            r["resource_group"]    = az.get("resource_group", "")

        top_resources = self._execute(f"""
            SELECT
                COALESCE(
                    u.usage_metadata.cluster_id,
                    u.usage_metadata.warehouse_id,
                    CAST(u.usage_metadata.job_run_id AS STRING),
                    u.usage_metadata.dlt_pipeline_id
                )                                                                AS resource_id,
                u.sku_name,
                u.workspace_id,
                any_value(u.usage_metadata.cluster_id)                            AS cluster_id,
                any_value(u.usage_metadata.warehouse_id)                          AS warehouse_id,
                any_value(CAST(u.usage_metadata.job_id AS STRING))               AS job_id,
                any_value(CAST(u.usage_metadata.job_run_id AS STRING))            AS job_run_id,
                any_value(u.usage_metadata.dlt_pipeline_id)                       AS dlt_pipeline_id,
                any_value(CAST(u.usage_metadata.notebook_id AS STRING))           AS notebook_id,
                any_value(u.usage_metadata.endpoint_name)                         AS endpoint_name,
                any_value(u.usage_metadata.endpoint_id)                           AS endpoint_id,
                any_value(u.identity_metadata.run_as)                             AS run_as,
                ROUND(SUM(u.usage_quantity), 2)                                  AS total_dbus,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2) AS estimated_cost_usd
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            WHERE {where}
            GROUP BY resource_id, u.sku_name, u.workspace_id
            ORDER BY estimated_cost_usd DESC
            LIMIT 30
        """)

        # Enrich with workspace name and top compute resources
        for r in by_workspace:
            wid = r.get("workspace_id", "")
            az = self._ws_azure.get(wid, {})
            r["workspace_name"] = az.get("workspace_name", "")
            r["workspace_url"]  = az.get("workspace_url", "")
            r["top_resources"]  = ws_res_map.get(wid, [])[:20]

        for r in top_resources:
            wid = r.get("workspace_id", "")
            az = self._ws_azure.get(wid, {})
            r["workspace_name"]    = az.get("workspace_name", "")
            r["subscription_name"] = az.get("subscription_name", "")
            r["resource_group"]    = az.get("resource_group", "")
            r["product_category"]  = _product_case_py(r.get("sku_name", ""))
            r["cluster_id"]        = r.get("cluster_id") or None
            r["warehouse_id"]      = r.get("warehouse_id") or None
            r["job_id"]            = r.get("job_id") or None
            r["job_run_id"]        = r.get("job_run_id") or None
            r["dlt_pipeline_id"]   = r.get("dlt_pipeline_id") or None
            r["notebook_id"]       = r.get("notebook_id") or None
            r["endpoint_name"]     = r.get("endpoint_name") or None
            r["endpoint_id"]       = r.get("endpoint_id") or None
            r["run_as"]            = r.get("run_as") or None
            r["workspace_url"]     = az.get("workspace_url", "")
            r["ui_path"]           = _ui_path(r)

        by_job = self._execute(f"""
            SELECT
                u.usage_metadata.job_id                                             AS job_id,
                any_value(j.name)                                                   AS job_name,
                any_value(w.workspace_name)                                         AS workspace_name,
                u.workspace_id,
                u.sku_name,
                COUNT(DISTINCT u.usage_metadata.job_run_id)                                          AS run_count,
                ARRAY_JOIN(ARRAY_DISTINCT(COLLECT_LIST(CAST(u.usage_metadata.job_run_id AS STRING))), ', ') AS run_ids,
                ROUND(SUM(u.usage_quantity), 2)                                                      AS total_dbus,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2)                     AS total_cost
            FROM system.billing.usage u
            LEFT JOIN system.lakeflow.jobs j
                ON CAST(u.usage_metadata.job_id AS BIGINT) = j.job_id
               AND u.workspace_id = j.workspace_id
               AND j.delete_time IS NULL
            LEFT JOIN system.access.workspaces_latest w
                ON u.workspace_id = w.workspace_id
            {self._PRICE_JOIN}
            WHERE {where}
              AND u.usage_metadata.job_id IS NOT NULL
            GROUP BY u.usage_metadata.job_id, u.workspace_id, u.sku_name
            ORDER BY total_cost DESC
            LIMIT 100
        """)

        total_cost = sum(float(r.get("estimated_cost_usd") or 0) for r in by_workspace)
        total_dbus = sum(float(r.get("total_dbus") or 0) for r in by_workspace)

        return {
            "date": d,
            "total_cost": round(total_cost, 2),
            "total_dbus": round(total_dbus, 2),
            "workspaces": len(by_workspace),
            "by_workspace": by_workspace,
            "by_product": by_product,
            "by_job": by_job,
            "top_resources": top_resources,
        }

    def get_by_tag(
        self,
        tag_key: str,
        start_date: str,
        end_date: str,
        workspace_id: Optional[str] = None,
        subscription: Optional[str] = None,
        resource_group: Optional[str] = None,
    ) -> list[dict]:
        safe_key = self._validate_tag_key(tag_key)
        where = self._where(start_date, end_date, workspace_id, subscription, resource_group)
        return self._execute(f"""
            SELECT
                u.custom_tags['{safe_key}']                                          AS tag_value,
                ROUND(SUM(u.usage_quantity), 2)                                      AS total_dbus,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2)     AS estimated_cost_usd,
                COUNT(DISTINCT u.workspace_id)                                       AS workspace_count
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            WHERE {where}
                AND u.custom_tags['{safe_key}'] IS NOT NULL
            GROUP BY tag_value
            ORDER BY total_dbus DESC
            LIMIT 20
        """)

    def get_by_tag_env(
        self,
        tag_key: str,
        start_date: str,
        end_date: str,
        workspace_id: Optional[str] = None,
        subscription: Optional[str] = None,
        resource_group: Optional[str] = None,
    ) -> list[dict]:
        """DBU by tag value broken down by environment (dev/test/uat/prod)."""
        safe_key = self._validate_tag_key(tag_key)
        where = self._where(start_date, end_date, workspace_id, subscription, resource_group)
        return self._execute(f"""
            SELECT
                u.custom_tags['{safe_key}']                                          AS tag_value,
                CASE
                    WHEN lower(w.workspace_name) LIKE '%prod%' THEN 'prod'
                    WHEN lower(w.workspace_name) LIKE '%uat%'  THEN 'uat'
                    WHEN lower(w.workspace_name) LIKE '%test%' THEN 'test'
                    WHEN lower(w.workspace_name) LIKE '%dev%'  THEN 'dev'
                    ELSE 'other'
                END                                                                  AS environment,
                ROUND(SUM(u.usage_quantity), 2)                                      AS total_dbus,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2)     AS estimated_cost_usd
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            LEFT JOIN system.access.workspaces_latest w ON u.workspace_id = w.workspace_id
            WHERE {where}
                AND u.custom_tags['{safe_key}'] IS NOT NULL
            GROUP BY tag_value, environment
            ORDER BY SUM(u.usage_quantity) DESC, tag_value, environment
            LIMIT 200
        """)

    def get_resource_groups(self, subscriptions: list[str] | None = None) -> list[str]:
        """Distinct resource groups from workspace map, filtered by subscription IDs."""
        lowers = {s.lower() for s in subscriptions} if subscriptions else set()
        rg_set: set[str] = set()
        for wid, info in self._ws_azure.items():
            # Only include workspaces the user is allowed to see
            if self._allowed_ws is not None and wid not in self._allowed_ws:
                continue
            rg = info.get("resource_group", "")
            if not rg:
                continue
            if not lowers or info.get("subscription", "").lower() in lowers:
                rg_set.add(rg)
        return sorted(rg_set, key=str.lower)

    def get_tag_values(self, tag_key: str) -> list[str]:
        safe_key = self._validate_tag_key(tag_key)
        ws_filter = ""
        if self._allowed_ws is not None:
            in_list = ", ".join(f"'{self._validate_workspace(w)}'" for w in self._allowed_ws)
            ws_filter = f"AND workspace_id IN ({in_list})"
        rows = self._execute(f"""
            SELECT DISTINCT custom_tags['{safe_key}'] AS tag_value
            FROM system.billing.usage
            WHERE usage_date >= date_add(current_date(), -90)
                AND custom_tags['{safe_key}'] IS NOT NULL
                {ws_filter}
            ORDER BY tag_value
            LIMIT 100
        """)
        return [r["tag_value"] for r in rows if r["tag_value"]]

    def _billing_snapshot(self) -> dict[str, dict]:
        """Today + yesterday DBU/cost keyed by 'type:id'.
        Four targeted queries so a failure in one doesn't break the others.
        """
        ws_filter = ""
        if self._allowed_ws is not None:
            in_list = ", ".join(f"'{self._validate_workspace(w)}'" for w in self._allowed_ws)
            ws_filter = f"AND u.workspace_id IN ({in_list})"
        result: dict[str, dict] = {}
        specs = [
            ("cluster",   "u.usage_metadata.cluster_id",
             "u.usage_metadata.cluster_id IS NOT NULL"),
            ("warehouse", "u.usage_metadata.warehouse_id",
             "u.usage_metadata.warehouse_id IS NOT NULL"),
            ("run",       "CAST(u.usage_metadata.run_id AS STRING)",
             "u.usage_metadata.run_id IS NOT NULL"),
            ("pipeline",  "u.usage_metadata.pipeline_id",
             "u.usage_metadata.pipeline_id IS NOT NULL"),
        ]
        for rtype, rid_col, filt in specs:
            try:
                rows = self._execute(f"""
                    SELECT
                        {rid_col}                                                           AS resource_id,
                        CAST(MIN(u.usage_start_time) AS STRING)                            AS first_billing_ts,
                        ROUND(SUM(u.usage_quantity), 3)                                    AS total_dbus,
                        ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2)   AS cost_usd
                    FROM system.billing.usage u
                    {self._PRICE_JOIN}
                    WHERE u.usage_date >= date_add(current_date(), -1)
                        AND {filt}
                        {ws_filter}
                    GROUP BY resource_id
                """)
                for r in rows:
                    rid = r.get("resource_id")
                    if rid:
                        result[f"{rtype}:{rid}"] = r
            except Exception:
                pass
        return result

    def get_anomalies(self) -> list[dict]:
        """Return all services running in monitored environments with anomaly scoring."""
        # Resolve the current workspace name once — used as env-detection fallback.
        # Workspace names like "dbw-edp-dev-01" carry env info even when services are untagged.
        workspace_name = ""
        try:
            if self._account_client:
                host = (self._client.config.host or "").rstrip("/").lower()
                for ws in self._account_client.workspaces.list():
                    # Azure host: adb-<workspace_id>.<region>.azuredatabricks.net
                    if str(ws.workspace_id) in host:
                        workspace_name = ws.workspace_name or ""
                        break
        except Exception:
            pass

        # One billing query covers all resource types for today + yesterday
        billing = self._billing_snapshot()

        results: list[dict] = []

        # ── 1. All-purpose clusters ───────────────────────────────────────────
        try:
            for c in self._client.clusters.list():
                if _state_val(c.state) != "RUNNING":
                    continue
                tier, env, by = _check_environment(c.cluster_name or "", c.custom_tags or {}, workspace_name)
                hours = _duration_hours(c.start_time)
                # Include if env tier detected, OR cluster has been running >4h regardless of env
                if not tier:
                    if hours < 4.0:
                        continue
                    tier, env, by = "other", "unknown", "duration"
                crit, warn = _TIER_THRESHOLDS[tier][0]
                snap = billing.get(f"cluster:{c.cluster_id or ''}")
                results.append({
                    "service_type":   "Cluster",
                    "name":           c.cluster_name or c.cluster_id or "",
                    "resource_id":    c.cluster_id or "",
                    "workspace_name": workspace_name,
                    "custom_tags":    dict(c.custom_tags or {}),
                    "state":          "RUNNING",
                    "environment":    env,
                    "env_tier":       tier,
                    "detected_by":    by,
                    "started_at":     _ms_to_iso(c.start_time),
                    "duration_hours": round(hours, 1),
                    "anomaly_level":  _anomaly_level(hours, critical=crit, warning=warn),
                    "dbus":           snap["total_dbus"] if snap else None,
                    "cost_usd":       snap["cost_usd"]   if snap else None,
                    "details": {
                        "id":      c.cluster_id or "",
                        "creator": c.creator_user_name or "",
                        "source":  _state_val(c.cluster_source),
                    },
                })
        except Exception:
            pass

        # ── 2. SQL Warehouses ─────────────────────────────────────────────────
        try:
            # Raw API client lives on the WarehousesAPI service object
            _wh_api = getattr(getattr(self._client, 'warehouses', None), '_api', None)

            for wh in self._client.warehouses.list():
                if _state_val(wh.state) not in ("RUNNING", "STARTING"):
                    continue
                tier, env, by = _check_environment(wh.name or "", wh.tags, workspace_name)
                if not tier:
                    continue

                # ── Start time from warehouse events (newest-first, paginated) ──
                # Events arrive newest-first. A warehouse that scales up/down
                # emits a new RUNNING event on each scale-up, so there can be
                # many RUNNING events inside a single continuous session.
                # We must NOT stop at the first RUNNING seen (that would give
                # a recent scale-up time, not the true session start).
                # Strategy: keep scanning backward in time, updating the
                # candidate timestamp on every RUNNING/STARTING event.  Stop
                # only when STOPPED is encountered (session boundary) or all
                # pages are exhausted.  The last-updated candidate (oldest in
                # chronological time) is the real session start.
                # Safety cap: 20 pages × 200 events = 4 000 events.
                wh_start_ms = None
                try:
                    if _wh_api and wh.id:
                        page_token = None
                        candidate_ms = None
                        done = False
                        for _ in range(20):
                            if done:
                                break
                            q: dict = {"limit": 200}
                            if page_token:
                                q["page_token"] = page_token
                            resp = _wh_api.do(
                                "GET",
                                f"/api/2.0/sql/warehouses/{wh.id}/events",
                                query=q,
                            )
                            page_token = (
                                resp.get("next_page_token")
                                or resp.get("nextPageToken")
                            )
                            for evt in (resp.get("events") or []):
                                etype = evt.get("event_type", "")
                                ts    = int(evt.get("timestamp") or 0)
                                if etype == "STOPPED" and candidate_ms is not None:
                                    # Session boundary — but only once we've already
                                    # found a RUNNING/STARTING event.  Serverless
                                    # warehouses auto-suspend (also emitting STOPPED)
                                    # without generating a new RUNNING on auto-resume,
                                    # so a STOPPED that appears before any RUNNING
                                    # (scanning newest-first) is an auto-suspend that
                                    # must be skipped to reach the true session start.
                                    done = True
                                    break
                                if etype in ("RUNNING", "STARTING") and ts:
                                    candidate_ms = ts  # keep updating → ends up as oldest seen
                            if not page_token:
                                break  # no more pages
                        wh_start_ms = candidate_ms
                except Exception:
                    pass

                # ── Billing: gap-detection finds current session start + totals ──
                # Scans last 365 days of billing records ordered by start time.
                # A gap >1h between consecutive periods signals the beginning of
                # a new session; MAX(usage_start_time) across all such gaps gives
                # the most recent session start.  Events-API timestamp (wh_start_ms)
                # is more precise and is preferred when it was found above.
                snap_fb = billing.get(f"warehouse:{wh.id or ''}")
                wh_dbus = wh_cost = None
                billing_start_ms = None
                if wh.id:
                    try:
                        rows = self._execute(f"""
                            WITH sorted AS (
                                SELECT
                                    usage_start_time,
                                    usage_end_time,
                                    LAG(usage_end_time) OVER (ORDER BY usage_start_time) AS prev_end
                                FROM system.billing.usage
                                WHERE usage_metadata.warehouse_id = '{wh.id}'
                                  AND usage_start_time >= timestamp(date_add(current_date(), -365))
                            ),
                            sess AS (
                                SELECT MAX(usage_start_time) AS sess_start
                                FROM sorted
                                WHERE prev_end IS NULL
                                   OR (unix_timestamp(usage_start_time) - unix_timestamp(prev_end)) > 3600
                            )
                            SELECT
                                CAST(sess.sess_start AS STRING)                                      AS first_ts,
                                ROUND(SUM(u.usage_quantity), 3)                                      AS total_dbus,
                                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2)     AS cost_usd
                            FROM system.billing.usage u
                            CROSS JOIN sess
                            {self._PRICE_JOIN}
                            WHERE u.usage_metadata.warehouse_id = '{wh.id}'
                              AND u.usage_start_time >= sess.sess_start
                            GROUP BY sess.sess_start
                        """)
                        if rows and rows[0].get("total_dbus") is not None:
                            wh_dbus = rows[0]["total_dbus"]
                            wh_cost = rows[0]["cost_usd"]
                            dt = _parse_ts_str(rows[0].get("first_ts", ""))
                            if dt:
                                billing_start_ms = int(dt.timestamp() * 1000)
                    except Exception:
                        if snap_fb:
                            wh_dbus = snap_fb.get("total_dbus")
                            wh_cost = snap_fb.get("cost_usd")
                            dt = _parse_ts_str(snap_fb.get("first_billing_ts", ""))
                            if dt:
                                billing_start_ms = int(dt.timestamp() * 1000)

                # Events API = precise start; billing gap-detection = approximate.
                # If the events loop hit the page cap without finding a STOPPED
                # boundary, candidate_ms is the oldest event within that window —
                # NOT the true session start.  Billing scans 365 days of data and
                # correctly finds the gap for very long sessions.  Take whichever
                # timestamp is earlier so long-running warehouses aren't under-counted.
                if billing_start_ms is not None and (
                    wh_start_ms is None or billing_start_ms < wh_start_ms
                ):
                    wh_start_ms = billing_start_ms

                wh_hours = _duration_hours(wh_start_ms) if wh_start_ms else -1.0
                crit, warn = _TIER_THRESHOLDS[tier][0]
                wh_tags: dict = {}
                if wh.tags and hasattr(wh.tags, "custom_tags") and wh.tags.custom_tags:
                    for t in wh.tags.custom_tags:
                        wh_tags[getattr(t, "key", "")] = str(getattr(t, "value", "") or "")
                results.append({
                    "service_type":   "SQL Warehouse",
                    "name":           wh.name or wh.id or "",
                    "resource_id":    wh.id or "",
                    "workspace_name": workspace_name,
                    "custom_tags":    wh_tags,
                    "state":          _state_val(wh.state),
                    "environment":    env,
                    "env_tier":       tier,
                    "detected_by":    by,
                    "started_at":     _ms_to_iso(wh_start_ms) if wh_start_ms else "",
                    "duration_hours": round(wh_hours, 1) if wh_hours >= 0 else -1,
                    "anomaly_level":  _anomaly_level(wh_hours, critical=crit, warning=warn) if wh_hours >= 0 else "info",
                    "dbus":           wh_dbus,
                    "cost_usd":       wh_cost,
                    "details": {
                        "id":   wh.id or "",
                        "size": wh.cluster_size or "",
                    },
                })
        except Exception:
            pass

        # ── 3. Active job runs ────────────────────────────────────────────────
        try:
            for run in self._client.jobs.list_runs(active_only=True):
                lc = _state_val(run.state.life_cycle_state) if run.state else ""
                if lc not in ("RUNNING", "PENDING"):
                    continue
                tags: dict = {}
                if run.cluster_spec and run.cluster_spec.new_cluster:
                    tags = dict(run.cluster_spec.new_cluster.custom_tags or {})
                job_name = run.run_name or f"Job run {run.run_id}"
                tier, env, by = _check_environment(job_name, tags, workspace_name)
                if not tier:
                    continue
                hours = _duration_hours(run.start_time)
                crit, warn = _TIER_THRESHOLDS[tier][1]
                snap = billing.get(f"run:{run.run_id or ''}")
                results.append({
                    "service_type":   "Job Run",
                    "name":           job_name,
                    "resource_id":    str(run.run_id or ""),
                    "workspace_name": workspace_name,
                    "state":          lc,
                    "environment":    env,
                    "env_tier":       tier,
                    "detected_by":    by,
                    "started_at":     _ms_to_iso(run.start_time),
                    "duration_hours": round(hours, 1),
                    "anomaly_level":  _anomaly_level(hours, critical=crit, warning=warn),
                    "dbus":           snap["total_dbus"] if snap else None,
                    "cost_usd":       snap["cost_usd"]   if snap else None,
                    "details": {
                        "run_id": str(run.run_id or ""),
                        "job_id": str(run.job_id or ""),
                    },
                })
        except Exception:
            pass

        # ── 4. DLT Pipelines ─────────────────────────────────────────────────
        try:
            for p in self._client.pipelines.list_pipelines():
                if _state_val(p.state) not in ("RUNNING", "RESETTING", "STARTING"):
                    continue
                tier, env, by = _check_environment(p.name or "", {}, workspace_name)
                if not tier:
                    continue
                snap = billing.get(f"pipeline:{p.pipeline_id or ''}")
                first_ts = snap["first_billing_ts"] if snap else ""
                pl_hours = _ts_to_duration_hours(first_ts)
                crit, warn = _TIER_THRESHOLDS[tier][0]
                results.append({
                    "service_type":   "DLT Pipeline",
                    "name":           p.name or p.pipeline_id or "",
                    "resource_id":    p.pipeline_id or "",
                    "workspace_name": workspace_name,
                    "state":          _state_val(p.state),
                    "environment":    env,
                    "env_tier":       tier,
                    "detected_by":    by,
                    "started_at":     _ts_to_display(first_ts),
                    "duration_hours": round(pl_hours, 1) if pl_hours >= 0 else -1,
                    "anomaly_level":  _anomaly_level(pl_hours, critical=crit, warning=warn) if pl_hours >= 0 else "info",
                    "dbus":           snap["total_dbus"] if snap else None,
                    "cost_usd":       snap["cost_usd"]   if snap else None,
                    "details": {
                        "id":      p.pipeline_id or "",
                        "creator": p.creator_user_name or "",
                    },
                })
        except Exception:
            pass

        _LEVEL = {"critical": 0, "warning": 1, "info": 2}
        results.sort(key=lambda x: (_LEVEL.get(x["anomaly_level"], 3), -x["duration_hours"]))
        return results

    def get_workspace_access(self) -> list[dict]:
        """Account-level workspace → principal (group/user/SP) assignments."""
        if not self._account_client:
            raise RuntimeError("Account client not configured — set DATABRICKS_ACCOUNT_ID")

        result = []
        for ws in self._account_client.workspaces.list():
            ws_id   = ws.workspace_id
            # Skip workspaces outside the allowed set
            if self._allowed_ws is not None and str(ws_id) not in self._allowed_ws:
                continue
            ws_name = ws.workspace_name or str(ws_id)

            principals = []
            try:
                for a in self._account_client.workspace_assignment.list(workspace_id=ws_id):
                    p = a.principal
                    if not p:
                        continue
                    perms = []
                    for perm in (a.permissions or []):
                        pval = perm.value if hasattr(perm, "value") else str(perm)
                        perms.append(pval)

                    if p.group_name:
                        ptype = "Group"
                        name  = p.display_name or p.group_name
                    elif p.service_principal_name:
                        ptype = "Service Principal"
                        name  = p.display_name or p.service_principal_name
                    else:
                        ptype = "User"
                        name  = p.display_name or p.user_name or ""

                    if name:
                        principals.append({
                            "name":        name,
                            "type":        ptype,
                            "permissions": perms,
                        })
            except Exception:
                pass

            principals.sort(key=lambda x: (x["type"], x["name"].lower()))
            result.append({
                "workspace_id":      str(ws_id),
                "workspace_name":    ws_name,
                "principal_count":   str(len(principals)),
                "principals":        principals,
            })

        result.sort(key=lambda x: x["workspace_name"].lower())
        return result

    def get_workspace_groups(self) -> list[dict]:
        _ENTITLEMENT_LABELS = {
            "workspace-admin":             "Workspace Admin",
            "allow-cluster-create":        "Can Create Clusters",
            "allow-instance-pool-create":  "Can Create Instance Pools",
            "databricks-sql-access":       "Databricks SQL Access",
            "allow-databricks-sql-access": "Databricks SQL Access",
        }

        result = []
        # count=25 is required — Databricks silently drops the `members` field
        # when the page size is large (default). Small page sizes include members.
        for group in self._client.groups.list(
            attributes="id,displayName,members,entitlements",
            count=25,
        ):
            members = []
            for m in (group.members or []):
                display = m.display or str(m.value or "")
                if display:
                    members.append({"display": display, "type": "User"})

            entitlements = []
            for e in (group.entitlements or []):
                raw = str(e.value) if e.value else ""
                label = _ENTITLEMENT_LABELS.get(raw, raw.replace("-", " ").title())
                if label and label not in entitlements:
                    entitlements.append(label)

            result.append({
                "group_name":   group.display_name or "",
                "member_count": str(len(members)),
                "entitlements": entitlements,
                "members":      members,
            })

        return sorted(result, key=lambda x: x["group_name"].lower())

    def get_workspace_users(self) -> list[dict]:
        result = []
        for u in self._client.users.list(attributes="displayName,emails,active"):
            primary_email = ""
            if u.emails:
                primary = next((e for e in u.emails if e.primary), u.emails[0])
                primary_email = primary.value or ""
            result.append({
                "display_name": u.display_name or "",
                "email": primary_email,
                "active": str(u.active) if u.active is not None else "True",
            })
        return sorted(result, key=lambda x: x["display_name"].lower())

    def _ws_ids_for_subscriptions(self, sub_ids: list[str]) -> list[str]:
        """Return workspace IDs that belong to the given subscription IDs."""
        lowers = {s.lower() for s in sub_ids}
        return [
            wid for wid, info in self._ws_azure.items()
            if info.get("subscription", "").lower() in lowers
            and (self._allowed_ws is None or wid in self._allowed_ws)
        ]

    def get_filters(self) -> dict:
        ws_filter = ""
        if self._allowed_ws is not None:
            in_list = ", ".join(f"'{self._validate_workspace(w)}'" for w in self._allowed_ws)
            ws_filter = f"AND workspace_id IN ({in_list})"
        workspaces = self._execute(f"""
            SELECT DISTINCT workspace_id
            FROM system.billing.usage
            WHERE usage_date >= date_add(current_date(), -90)
                {ws_filter}
            ORDER BY workspace_id
        """)
        tag_keys = self._execute(f"""
            SELECT DISTINCT explode(map_keys(custom_tags)) AS tag_key
            FROM system.billing.usage
            WHERE usage_date >= date_add(current_date(), -90)
                AND size(custom_tags) > 0
                {ws_filter}
            ORDER BY tag_key
        """)

        # Build subscription options from workspace map + static mapping
        active_ws = {r["workspace_id"] for r in workspaces}
        seen_subs: dict[str, str] = {}  # sub_id → name
        for wid, info in self._ws_azure.items():
            if self._allowed_ws is not None and wid not in self._allowed_ws:
                continue
            sub = info.get("subscription", "")
            if sub and wid in active_ws:
                seen_subs[sub] = resolve_subscription(sub)
        # Add any from static map not yet seen
        for sid, sname in SUBSCRIPTION_MAP.items():
            if sid not in seen_subs:
                seen_subs[sid] = sname
        sub_options = sorted(
            [{"id": sid, "name": name} for sid, name in seen_subs.items()],
            key=lambda x: x["name"].lower(),
        )

        # Build resource group options from workspace map
        rg_set: set[str] = set()
        for wid, info in self._ws_azure.items():
            if self._allowed_ws is not None and wid not in self._allowed_ws:
                continue
            rg = info.get("resource_group", "")
            if rg and wid in active_ws:
                rg_set.add(rg)
        rg_options = sorted(rg_set, key=str.lower)

        ws_options = sorted(
            [
                {
                    "id": r["workspace_id"],
                    "name": self._ws_azure.get(r["workspace_id"], {}).get("workspace_name", "") or r["workspace_id"],
                }
                for r in workspaces
            ],
            key=lambda x: x["name"].lower(),
        )
        return {
            "workspaces": ws_options,
            "subscriptions": sub_options,
            "resource_groups": rg_options,
            "tag_keys": [r["tag_key"] for r in tag_keys],
        }

    def get_jobs_compute_cost(self) -> list[dict]:
        """Cost per job (last 30 days) from system.billing.usage joined to system.lakeflow.jobs."""
        import datetime as _dt
        d30 = (_dt.date.today() - _dt.timedelta(days=30)).isoformat()
        return safe_execute_sync(self._client, self._warehouse_id, f"""
            SELECT
                u.usage_metadata.job_id                                             AS job_id,
                any_value(j.name)                                                   AS job_name,
                any_value(w.workspace_name)                                         AS workspace_name,
                u.workspace_id,
                u.sku_name,
                COUNT(DISTINCT u.usage_metadata.job_run_id)                         AS run_count,
                ROUND(SUM(u.usage_quantity), 2)                                     AS total_dbus,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2)    AS total_cost,
                MIN(CAST(u.usage_start_time AS STRING))                             AS first_run,
                MAX(CAST(u.usage_start_time AS STRING))                             AS last_run,
                COUNT(DISTINCT DATE(u.usage_start_time))                            AS active_days
            FROM system.billing.usage u
            LEFT JOIN system.lakeflow.jobs j
                ON CAST(u.usage_metadata.job_id AS BIGINT) = j.job_id
               AND u.workspace_id = j.workspace_id
               AND j.delete_time IS NULL
            LEFT JOIN system.access.workspaces_latest w
                ON u.workspace_id = w.workspace_id
            LEFT JOIN system.billing.list_prices p
                ON u.sku_name          = p.sku_name
               AND u.cloud             = p.cloud
               AND u.usage_start_time >= p.price_start_time
               AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
            WHERE u.usage_start_time >= timestamp('{d30}')
              AND u.usage_metadata.job_id IS NOT NULL
            GROUP BY u.usage_metadata.job_id, u.workspace_id, u.sku_name
            ORDER BY total_cost DESC
            LIMIT 200
        """, label="Cost-JobCost")
