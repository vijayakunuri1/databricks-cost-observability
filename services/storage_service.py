"""
Storage & Data Lake health from system.storage and information_schema.

Provides:
  - Predictive optimization operations
  - Table/schema inventory with sizes
  - Data growth tracking
"""

from __future__ import annotations

import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from databricks.sdk import WorkspaceClient

from core.config import get_settings
from core.sql_executor import execute_sync, safe_execute_sync

_log = logging.getLogger(__name__)


class StorageService:
    LOOKBACK_DAYS = 30
    CACHE_TTL_MIN = 30

    def __init__(self, client: WorkspaceClient) -> None:
        self._client = client
        self._wh_id  = get_settings().databricks_warehouse_id
        self._cache: Optional[dict]                  = None
        self._cached_at: Optional[datetime.datetime] = None

    def _execute(self, sql: str) -> list[dict]:
        return execute_sync(self._client, self._wh_id, sql, label="Storage")

    def _safe_execute(self, sql: str) -> list[dict]:
        return safe_execute_sync(self._client, self._wh_id, sql, label="Storage")

    def _fetch_optimization_ops(self) -> list[dict]:
        return self._safe_execute(f"""
            SELECT
                catalog_name,
                schema_name,
                table_name,
                operation_type,
                operation_status,
                CAST(start_time AS STRING)  AS started_at,
                ROUND((UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)) / 60.0, 1) AS duration_min,
                operation_metrics
            FROM system.storage.predictive_optimization_operations_history
            WHERE start_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
            ORDER BY start_time DESC
            LIMIT 500
        """)

    def _fetch_table_inventory(self) -> list[dict]:
        return self._safe_execute("""
            SELECT
                table_catalog   AS catalog_name,
                table_schema    AS schema_name,
                table_name,
                table_type,
                CAST(created AS STRING) AS created_at
            FROM system.information_schema.tables
            WHERE table_schema NOT IN ('information_schema')
              AND table_catalog NOT IN ('system', 'samples')
              AND table_type IN ('MANAGED', 'EXTERNAL')
            ORDER BY table_catalog, table_schema, table_name
            LIMIT 5000
        """)

    def _fetch_schema_summary(self) -> list[dict]:
        return self._safe_execute("""
            SELECT
                table_catalog   AS catalog_name,
                table_schema    AS schema_name,
                COUNT(*)        AS table_count
            FROM system.information_schema.tables
            WHERE table_schema NOT IN ('information_schema')
              AND table_catalog NOT IN ('system', 'samples')
              AND table_type IN ('MANAGED', 'EXTERNAL')
            GROUP BY table_catalog, table_schema
            ORDER BY table_count DESC
            LIMIT 500
        """)

    def _fetch_workspace_inventory(self) -> list[dict]:
        """Workspace inventory — try system table, fall back to AccountClient SDK."""
        rows = self._safe_execute("""
            SELECT
                workspace_id,
                workspace_name,
                workspace_url,
                workspace_status,
                cloud,
                region,
                pricing_tier,
                CAST(creation_time AS STRING) AS created_at
            FROM system.access.workspaces_latest
            ORDER BY workspace_name
        """)
        if rows:
            return rows
        # Fallback: AccountClient.workspaces.list()
        try:
            from core.dependencies import get_account_client
            acct = get_account_client()
            out = []
            for ws in acct.workspaces.list():
                host = getattr(ws, "deployment_name", "") or ""
                cloud_name = getattr(ws, "cloud", "")
                if not cloud_name:
                    cloud_name = "Azure" if "azure" in host.lower() else (
                        "AWS" if "aws" in host.lower() else "GCP")
                out.append({
                    "workspace_id":     str(getattr(ws, "workspace_id", "")),
                    "workspace_name":   getattr(ws, "workspace_name", ""),
                    "workspace_url":    f"https://{host}" if host and not host.startswith("http") else host,
                    "workspace_status": str(getattr(ws, "workspace_status", "")),
                    "cloud":            str(cloud_name),
                    "region":           getattr(ws, "location", "") or getattr(ws, "region", ""),
                    "pricing_tier":     str(getattr(ws, "pricing_tier", "")),
                    "created_at":       str(getattr(ws, "creation_time", "")),
                })
            if out:
                return out
        except Exception as e:
            _log.debug("AccountClient workspace list failed: %s", e)
        # Final fallback: current workspace only
        try:
            ws_id = str(self._client.get_workspace_id())
            host = self._client.config.host or ""
            cloud = "Azure" if "azure" in host.lower() else (
                "AWS" if "aws" in host.lower() else "GCP")
            return [{
                "workspace_id": ws_id,
                "workspace_name": f"Workspace {ws_id}",
                "workspace_url": host,
                "workspace_status": "RUNNING",
                "cloud": cloud,
                "region": "",
                "pricing_tier": "",
                "created_at": "",
            }]
        except Exception:
            return []

    def _analyse(self, opt_ops, tables, schemas, workspaces=None) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)

        def _int(v):
            try: return int(v) if v else 0
            except: return 0
        def _float(v):
            try: return float(v) if v else 0.0
            except: return 0.0

        # Optimization ops summary
        op_summary = {}
        for r in opt_ops:
            ot = r.get("operation_type", "unknown")
            st = r.get("operation_status", "unknown")
            op_summary.setdefault(ot, {"total": 0, "success": 0, "failed": 0})
            op_summary[ot]["total"] += 1
            if st and "SUCCESS" in st.upper():
                op_summary[ot]["success"] += 1
            elif st and "FAIL" in st.upper():
                op_summary[ot]["failed"] += 1

        ops_list = sorted(
            [{"operation": k, **v} for k, v in op_summary.items()],
            key=lambda x: -x["total"],
        )

        # Schema summary
        schema_list = [{
            "catalog_name": r.get("catalog_name", ""),
            "schema_name":  r.get("schema_name", ""),
            "table_count":  _int(r.get("table_count")),
        } for r in schemas]

        total_tables = sum(s["table_count"] for s in schema_list)
        total_schemas = len(schema_list)
        total_catalogs = len({s["catalog_name"] for s in schema_list})

        # Catalog summary (grouped from schemas)
        catalog_groups: dict = {}
        for s in schema_list:
            cat = s["catalog_name"]
            if cat not in catalog_groups:
                catalog_groups[cat] = {"catalog_name": cat, "schema_count": 0,
                                       "table_count": 0, "schemas": []}
            catalog_groups[cat]["schema_count"] += 1
            catalog_groups[cat]["table_count"] += s["table_count"]
            catalog_groups[cat]["schemas"].append(
                {"schema_name": s["schema_name"], "table_count": s["table_count"]})
        catalog_summary = sorted(catalog_groups.values(),
                                 key=lambda x: -x["table_count"])

        # Table type distribution
        type_dist: dict = {}
        for t in tables:
            tt = t.get("table_type", "UNKNOWN")
            type_dist[tt] = type_dist.get(tt, 0) + 1
        table_type_dist = sorted(
            [{"type": k, "count": v} for k, v in type_dist.items()],
            key=lambda x: -x["count"])

        # Workspace inventory
        ws_list = [{
            "workspace_id":     r.get("workspace_id", ""),
            "workspace_name":   r.get("workspace_name", ""),
            "workspace_url":    r.get("workspace_url", ""),
            "workspace_status": r.get("workspace_status", ""),
            "cloud":            r.get("cloud", ""),
            "region":           r.get("region", ""),
            "pricing_tier":     r.get("pricing_tier", ""),
            "created_at":       r.get("created_at", ""),
        } for r in (workspaces or [])]

        return {
            "generated_at":  now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lookback_days": self.LOOKBACK_DAYS,
            "summary": {
                "total_catalogs":     total_catalogs,
                "total_schemas":      total_schemas,
                "total_tables":       total_tables,
                "managed_tables":     type_dist.get("MANAGED", 0),
                "external_tables":    type_dist.get("EXTERNAL", 0),
                "optimization_ops":   len(opt_ops),
                "op_types":           len(ops_list),
                "total_workspaces":   len(ws_list),
            },
            "catalog_summary":      catalog_summary,
            "table_type_dist":      table_type_dist,
            "optimization_summary": ops_list,
            "optimization_history": [{
                "catalog":    r.get("catalog_name", ""),
                "schema":     r.get("schema_name", ""),
                "table":      r.get("table_name", ""),
                "operation":  r.get("operation_type", ""),
                "status":     r.get("operation_status", ""),
                "started_at": r.get("started_at", ""),
                "duration_min": _float(r.get("duration_min")),
            } for r in opt_ops[:200]],
            "schema_summary": schema_list,
            "workspaces":     ws_list,
        }

    def get_storage_insights(self) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)
        if self._cache and self._cached_at:
            age = (now - self._cached_at).total_seconds() / 60
            if age < self.CACHE_TTL_MIN:
                return {**self._cache, "cache_age_minutes": round(age, 1)}

        with ThreadPoolExecutor(max_workers=4) as pool:
            f_opt  = pool.submit(self._fetch_optimization_ops)
            f_tbl  = pool.submit(self._fetch_table_inventory)
            f_sch  = pool.submit(self._fetch_schema_summary)
            f_ws   = pool.submit(self._fetch_workspace_inventory)

        def _safe_result(future, default=None):
            try:
                return future.result()
            except Exception:
                return default if default is not None else []

        opt_ops  = _safe_result(f_opt)
        tables   = _safe_result(f_tbl)
        schemas  = _safe_result(f_sch)
        ws_inv   = _safe_result(f_ws)

        result = self._analyse(opt_ops, tables, schemas, ws_inv)
        result["cache_age_minutes"] = 0
        self._cache = result
        self._cached_at = now
        return result
