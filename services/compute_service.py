"""
Compute Right-Sizing analytics from system.compute tables.

Provides executive-level visibility into:
  - Cluster and warehouse inventory with configurations
  - Over-provisioned resources (autoscaling disabled, large min workers)
  - Auto-stop / auto-terminate disabled resources
  - DBR version sprawl
  - Warehouse sizing distribution
"""

from __future__ import annotations

import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from databricks.sdk import WorkspaceClient

from core.config import get_settings
from core.sql_executor import execute_sync, safe_execute_sync
from core.subscriptions import resolve_subscription


class ComputeService:
    CACHE_TTL_MIN = 30

    def __init__(self, client: WorkspaceClient, account_client=None) -> None:
        self._client = client
        self._wh_id  = get_settings().databricks_warehouse_id
        self._cache: Optional[dict]                  = None
        self._cached_at: Optional[datetime.datetime] = None

        # Build workspace_id → Azure context map
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
                        "workspace_name":    getattr(ws, "workspace_name", "") or "",
                        "subscription":      sub,
                        "subscription_name": resolve_subscription(sub) if sub else "",
                        "resource_group":    rg,
                    }
            except Exception:
                pass

    def _execute(self, sql: str) -> list[dict]:
        return execute_sync(self._client, self._wh_id, sql, label="Compute")

    def _safe_execute(self, sql: str) -> list[dict]:
        return safe_execute_sync(self._client, self._wh_id, sql, label="Compute")

    # ── data queries ──────────────────────────────────────────────────────────

    def _fetch_clusters(self) -> list[dict]:
        """Latest config per active interactive cluster.

        A cluster is considered deleted when its most-recent change_time
        record has a non-null delete_time.  This handles clusters that were
        deleted and re-created — only the current status matters.
        Ephemeral job clusters and DLT execution clusters are excluded.
        """
        return self._execute("""
            WITH all_clusters AS (
                SELECT
                    cluster_id,
                    cluster_name,
                    workspace_id,
                    owned_by                             AS owner,
                    dbr_version                          AS runtime_version,
                    cluster_source,
                    driver_node_type,
                    worker_node_type,
                    min_autoscale_workers,
                    max_autoscale_workers,
                    auto_termination_minutes,
                    CAST(delete_time  AS STRING)         AS delete_time,
                    CAST(change_time  AS STRING)         AS last_modified,
                    ROW_NUMBER() OVER (
                        PARTITION BY cluster_id
                        ORDER BY change_time DESC
                    )                                    AS rn
                FROM system.compute.clusters
                WHERE (cluster_source NOT IN ('JOB') OR cluster_source IS NULL)
                  AND cluster_name NOT LIKE 'dlt-execution-%'
            ),
            deleted_clusters AS (
                SELECT cluster_id
                FROM (
                    SELECT
                        cluster_id,
                        delete_time,
                        ROW_NUMBER() OVER (
                            PARTITION BY cluster_id
                            ORDER BY change_time DESC
                        ) AS rn
                    FROM system.compute.clusters
                ) latest
                WHERE rn = 1
                  AND delete_time IS NOT NULL
            )
            SELECT
                cluster_id,
                cluster_name,
                workspace_id,
                owner,
                runtime_version,
                cluster_source,
                driver_node_type,
                worker_node_type,
                min_autoscale_workers,
                max_autoscale_workers,
                auto_termination_minutes,
                delete_time,
                last_modified
            FROM all_clusters
            WHERE rn = 1
              AND cluster_id NOT IN (SELECT cluster_id FROM deleted_clusters)
            ORDER BY last_modified DESC
        """)

    def _fetch_warehouses(self) -> list[dict]:
        """Latest config per warehouse (deduplicated — one row per warehouse)."""
        return self._execute("""
            WITH latest AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY warehouse_id
                        ORDER BY change_time DESC
                    ) AS rn
                FROM system.compute.warehouses
                WHERE delete_time IS NULL
            )
            SELECT
                warehouse_id,
                warehouse_name,
                workspace_id,
                warehouse_type,
                warehouse_size,
                min_clusters,
                max_clusters,
                auto_stop_minutes,
                CAST(change_time AS STRING)                       AS last_modified
            FROM latest
            WHERE rn = 1
            LIMIT 200
        """)

    def _fetch_sdk_pipeline_info(self) -> dict[str, dict]:
        """Trigger/schedule info from SDK for pipelines in current workspace."""
        info: dict[str, dict] = {}
        try:
            for p in self._client.pipelines.list_pipelines():
                pid = p.pipeline_id or ""
                if not pid:
                    continue
                name = p.name or ""
                state = str(getattr(p, "state", "")) or ""

                has_trigger = False
                trigger_type = "None"
                trigger_detail = ""
                try:
                    detail = self._client.pipelines.get(pid)
                    spec = getattr(detail, "spec", None)
                    if spec:
                        trigger = getattr(spec, "trigger", None)
                        continuous = getattr(spec, "continuous", None)
                        if continuous:
                            has_trigger = True
                            trigger_type = "Continuous"
                            trigger_detail = "Always running"
                        elif trigger:
                            cron = getattr(trigger, "cron", None)
                            manual = getattr(trigger, "manual", None)
                            file_arrival = getattr(trigger, "file_arrival", None)
                            if cron:
                                has_trigger = True
                                trigger_type = "Cron"
                                trigger_detail = getattr(cron, "quartz_cron_schedule", "") or ""
                            elif file_arrival:
                                has_trigger = True
                                trigger_type = "File Arrival"
                                url = getattr(file_arrival, "url", "") or ""
                                trigger_detail = url[:80] if url else ""
                            elif manual:
                                trigger_type = "Manual"
                            else:
                                has_trigger = True
                                trigger_type = "Scheduled"
                except Exception:
                    pass

                info[pid] = {
                    "name":           name,
                    "state":          state,
                    "has_trigger":    has_trigger,
                    "trigger_type":   trigger_type,
                    "trigger_detail": trigger_detail,
                }
        except Exception:
            pass
        return info

    def _fetch_dlt_billing(self) -> list[dict]:
        """All DLT pipelines from billing (all workspaces) with 30/60/90 breakdowns."""
        return self._execute("""
            SELECT
                usage_metadata.dlt_pipeline_id                                  AS pipeline_id,
                workspace_id,
                ROUND(SUM(CASE WHEN usage_start_time >= timestamp(date_add(current_date(), -30))
                    THEN usage_quantity ELSE 0 END), 2)                         AS dbus_30d,
                ROUND(SUM(CASE WHEN usage_start_time >= timestamp(date_add(current_date(), -30))
                    THEN usage_quantity * COALESCE(p.pricing.default, 0) ELSE 0 END), 2) AS cost_30d,
                ROUND(SUM(CASE WHEN usage_start_time >= timestamp(date_add(current_date(), -60))
                    THEN usage_quantity ELSE 0 END), 2)                         AS dbus_60d,
                ROUND(SUM(CASE WHEN usage_start_time >= timestamp(date_add(current_date(), -60))
                    THEN usage_quantity * COALESCE(p.pricing.default, 0) ELSE 0 END), 2) AS cost_60d,
                ROUND(SUM(usage_quantity), 2)                                   AS dbus_90d,
                ROUND(SUM(usage_quantity * COALESCE(p.pricing.default, 0)), 2)  AS cost_90d,
                COUNT(DISTINCT DATE(usage_start_time))                          AS active_days
            FROM system.billing.usage u
            LEFT JOIN system.billing.list_prices p
                ON u.sku_name          = p.sku_name
               AND u.cloud             = p.cloud
               AND u.usage_start_time >= p.price_start_time
               AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
            WHERE usage_start_time >= timestamp(date_add(current_date(), -90))
              AND usage_metadata.dlt_pipeline_id IS NOT NULL
            GROUP BY 1, 2
        """)

    def _fetch_recent_active_ids(self) -> set[str]:
        """Cluster/warehouse IDs with any billing in the last 30 days."""
        rows = self._execute("""
            SELECT DISTINCT COALESCE(
                usage_metadata.cluster_id,
                usage_metadata.warehouse_id
            ) AS resource_id
            FROM system.billing.usage
            WHERE usage_start_time >= timestamp(date_add(current_date(), -30))
              AND (usage_metadata.cluster_id IS NOT NULL
                   OR usage_metadata.warehouse_id IS NOT NULL)
        """)
        return {r["resource_id"] for r in rows if r.get("resource_id")}

    def _fetch_node_utilization(self) -> list[dict]:
        """Per-cluster task utilization from system.compute.node_timeline (last 7d)."""
        return self._safe_execute("""
            SELECT
                cluster_id,
                workspace_id,
                COUNT(DISTINCT node_id)          AS node_count,
                ROUND(SUM(uptime_seconds) / 3600.0, 1) AS total_node_hours,
                ROUND(AVG(CASE WHEN num_task_slots > 0
                    THEN avg_num_running_tasks / num_task_slots * 100
                    ELSE 0 END), 1)              AS avg_task_util_pct,
                ROUND(MAX(CASE WHEN num_task_slots > 0
                    THEN avg_num_running_tasks / num_task_slots * 100
                    ELSE 0 END), 1)              AS peak_task_util_pct,
                ROUND(AVG(avg_num_queued_tasks), 2) AS avg_queued_tasks,
                SUM(CASE WHEN is_driver THEN 1 ELSE 0 END)  AS driver_samples,
                SUM(CASE WHEN is_driver THEN 0 ELSE 1 END)  AS worker_samples
            FROM system.compute.node_timeline
            WHERE start_time >= timestamp(date_add(current_date(), -7))
            GROUP BY cluster_id, workspace_id
            ORDER BY total_node_hours DESC
            LIMIT 300
        """)

    def _fetch_node_types_info(self) -> dict[str, dict]:
        """Hardware specs lookup — tries system.compute.node_types first,
        falls back to the SDK clusters.list_node_types() API."""
        rows = self._safe_execute("""
            SELECT
                node_type_id,
                num_cpus,
                memory_mb,
                COALESCE(num_gpus, 0)  AS num_gpus,
                COALESCE(gpu_name, '') AS gpu_name,
                is_deprecated,
                category
            FROM system.compute.node_types
            ORDER BY memory_mb DESC
            LIMIT 300
        """)
        if rows:
            return {
                r["node_type_id"]: {
                    "num_cpus":   int(r.get("num_cpus") or 0),
                    "memory_mb":  int(r.get("memory_mb") or 0),
                    "memory_gb":  round(int(r.get("memory_mb") or 0) / 1024, 1),
                    "num_gpus":   int(r.get("num_gpus") or 0),
                    "gpu_name":   r.get("gpu_name", ""),
                    "is_deprecated": str(r.get("is_deprecated", "")).lower() == "true",
                    "category":   r.get("category", ""),
                }
                for r in rows if r.get("node_type_id")
            }

        # Fallback: SDK API (works even without system table access)
        try:
            resp = self._client.clusters.list_node_types()
            result = {}
            for nt in (resp.node_types or []):
                ntid = getattr(nt, "node_type_id", None)
                if not ntid:
                    continue
                mem_mb = getattr(nt, "memory_mb", 0) or 0
                result[ntid] = {
                    "num_cpus":      getattr(nt, "num_cpus", 0) or 0,
                    "memory_mb":     mem_mb,
                    "memory_gb":     round(mem_mb / 1024, 1),
                    "num_gpus":      getattr(nt, "num_gpus", 0) or 0,
                    "gpu_name":      getattr(nt, "node_instance_type", None)
                                     and getattr(nt.node_instance_type, "instance_type_id", "") or "",
                    "is_deprecated": getattr(nt, "is_deprecated", False) or False,
                    "category":      getattr(nt, "category", "") or "",
                }
            return result
        except Exception:
            return {}

    def _fetch_cluster_utilisation(self) -> list[dict]:
        """DBU/cost per cluster/warehouse with 30d, 60d, 90d breakdowns."""
        return self._execute("""
            SELECT
                COALESCE(
                    usage_metadata.cluster_id,
                    usage_metadata.warehouse_id
                )                                                          AS resource_id,
                ROUND(SUM(CASE WHEN usage_start_time >= timestamp(date_add(current_date(), -30))
                    THEN usage_quantity ELSE 0 END), 2)                    AS dbus_30d,
                ROUND(SUM(CASE WHEN usage_start_time >= timestamp(date_add(current_date(), -30))
                    THEN usage_quantity * COALESCE(p.pricing.default, 0) ELSE 0 END), 2) AS cost_30d,
                SUM(CASE WHEN usage_start_time >= timestamp(date_add(current_date(), -30))
                    THEN 1 ELSE 0 END)                                     AS rows_30d,
                ROUND(SUM(CASE WHEN usage_start_time >= timestamp(date_add(current_date(), -60))
                    THEN usage_quantity ELSE 0 END), 2)                    AS dbus_60d,
                ROUND(SUM(CASE WHEN usage_start_time >= timestamp(date_add(current_date(), -60))
                    THEN usage_quantity * COALESCE(p.pricing.default, 0) ELSE 0 END), 2) AS cost_60d,
                SUM(CASE WHEN usage_start_time >= timestamp(date_add(current_date(), -60))
                    THEN 1 ELSE 0 END)                                     AS rows_60d,
                ROUND(SUM(usage_quantity), 2)                              AS dbus_90d,
                ROUND(SUM(usage_quantity * COALESCE(p.pricing.default, 0)), 2) AS cost_90d,
                COUNT(*)                                                   AS rows_90d,
                COUNT(DISTINCT DATE(usage_start_time))                     AS active_days
            FROM system.billing.usage u
            LEFT JOIN system.billing.list_prices p
                ON u.sku_name          = p.sku_name
               AND u.cloud             = p.cloud
               AND u.usage_start_time >= p.price_start_time
               AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
            WHERE usage_start_time >= timestamp(date_add(current_date(), -90))
              AND (usage_metadata.cluster_id IS NOT NULL
                   OR usage_metadata.warehouse_id IS NOT NULL)
            GROUP BY 1
        """)

    # ── analysis ──────────────────────────────────────────────────────────────

    def _analyse(
        self,
        clusters: list[dict],
        warehouses: list[dict],
        utilisation: list[dict],
        recent_active: set[str],
        dlt_billing: list[dict] | None = None,
        sdk_pipeline_info: dict[str, dict] | None = None,
        node_util: list[dict] | None = None,
        node_types: dict[str, dict] | None = None,
    ) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)

        # Build utilisation lookup with 30/60/90 day breakdowns
        util_map = {}
        for r in utilisation:
            rid = r.get("resource_id", "")
            if rid:
                util_map[rid] = {
                    "dbus_30d":    float(r.get("dbus_30d") or 0),
                    "cost_30d":    float(r.get("cost_30d") or 0),
                    "dbus_60d":    float(r.get("dbus_60d") or 0),
                    "cost_60d":    float(r.get("cost_60d") or 0),
                    "dbus_90d":    float(r.get("dbus_90d") or 0),
                    "cost_90d":    float(r.get("cost_90d") or 0),
                    "active_days": int(r.get("active_days") or 0),
                }

        # Build node utilization lookup (cluster_id → metrics)
        nu_map: dict[str, dict] = {}
        for r in (node_util or []):
            cid = r.get("cluster_id", "")
            if cid:
                nu_map[cid] = {
                    "node_count":          int(r.get("node_count") or 0),
                    "total_node_hours":    float(r.get("total_node_hours") or 0),
                    "avg_task_util_pct":   float(r.get("avg_task_util_pct") or 0),
                    "peak_task_util_pct":  float(r.get("peak_task_util_pct") or 0),
                    "avg_queued_tasks":    float(r.get("avg_queued_tasks") or 0),
                }

        # Node types lookup
        nt_map = node_types or {}

        # ── Cluster analysis ──────────────────────────────────────────────────
        cluster_list = []
        dbr_versions = {}
        no_autoterminate = 0
        no_autoscale     = 0
        oversized        = 0

        for c in clusters:
            cid          = c.get("cluster_id", "")
            is_terminated = bool(c.get("delete_time"))

            # Always show active clusters (delete_time IS NULL) — they are
            # running right now even if billing hasn't yet propagated (new
            # clusters, billing latency, etc.).
            # Terminated clusters are already scoped to 7 days by the SQL query.

            u   = util_map.get(cid, {})

            min_w  = int(c.get("min_autoscale_workers") or 0)
            max_w  = int(c.get("max_autoscale_workers") or 0)
            at_min = int(c.get("auto_termination_minutes") or 0)
            dbr    = c.get("runtime_version") or "unknown"

            # Flags — only add config-warning flags for active clusters
            flags = []
            if is_terminated:
                flags.append("terminated")
            else:
                if at_min == 0:
                    flags.append("no_auto_terminate")
                    no_autoterminate += 1
                if min_w == max_w and max_w > 0:
                    flags.append("fixed_size")
                    no_autoscale += 1
                if min_w >= 8:
                    flags.append("large_min_workers")
                    oversized += 1

            dbr_versions[dbr] = dbr_versions.get(dbr, 0) + 1

            wid = c.get("workspace_id", "")
            az  = self._ws_azure.get(wid, {})
            entry = {
                "cluster_id":              cid,
                "cluster_name":            c.get("cluster_name", ""),
                "workspace_id":            wid,
                "workspace_name":          az.get("workspace_name", ""),
                "subscription_name":       az.get("subscription_name", ""),
                "resource_group":          az.get("resource_group", ""),
                "owner":                   c.get("owner", ""),
                "runtime_version":         dbr,
                "cluster_source":          c.get("cluster_source", ""),
                "driver_node_type":        c.get("driver_node_type", ""),
                "worker_node_type":        c.get("worker_node_type", ""),
                "min_workers":             min_w,
                "max_workers":             max_w,
                "auto_terminate_minutes":  at_min,
                "delete_time":             c.get("delete_time", ""),
                "dbus_30d":               u.get("dbus_30d", 0),
                "cost_30d":               u.get("cost_30d", 0),
                "dbus_60d":               u.get("dbus_60d", 0),
                "cost_60d":               u.get("cost_60d", 0),
                "dbus_90d":               u.get("dbus_90d", 0),
                "cost_90d":               u.get("cost_90d", 0),
                "active_days":            u.get("active_days", 0),
                "flags":                  flags,
            }
            # Enrich with node utilization from node_timeline
            nu = nu_map.get(cid, {})
            entry["avg_task_util_pct"]  = nu.get("avg_task_util_pct", None)
            entry["peak_task_util_pct"] = nu.get("peak_task_util_pct", None)
            entry["avg_queued_tasks"]   = nu.get("avg_queued_tasks", None)
            entry["total_node_hours"]   = nu.get("total_node_hours", None)
            entry["node_count"]         = nu.get("node_count", None)
            # Enrich with hardware specs from node_types
            driver_nt = nt_map.get(c.get("driver_node_type", ""), {})
            worker_nt = nt_map.get(c.get("worker_node_type", ""), {})
            entry["driver_cpus"]   = driver_nt.get("num_cpus")
            entry["driver_mem_gb"] = driver_nt.get("memory_gb")
            entry["worker_cpus"]   = worker_nt.get("num_cpus")
            entry["worker_mem_gb"] = worker_nt.get("memory_gb")
            entry["worker_gpus"]   = worker_nt.get("num_gpus") or None
            cluster_list.append(entry)

        # Sort: active flagged first → active OK → terminated last (all by 90d cost)
        cluster_list.sort(key=lambda x: (
            1 if "terminated" in x["flags"] else 0,
            -len([f for f in x["flags"] if f != "terminated"]),
            -x["cost_90d"],
        ))

        # ── Warehouse analysis ────────────────────────────────────────────────
        wh_list = []
        no_autostop_wh  = 0
        size_dist       = {}

        for w in warehouses:
            wid  = w.get("warehouse_id", "")
            u    = util_map.get(wid, {})
            size = w.get("warehouse_size", "unknown")
            astop = int(w.get("auto_stop_minutes") or 0)
            min_c = int(w.get("min_clusters") or 0)
            max_c = int(w.get("max_clusters") or 0)

            flags = []
            if astop == 0:
                flags.append("no_auto_stop")
                no_autostop_wh += 1
            if min_c > 1:
                flags.append("min_clusters_gt_1")

            size_dist[size] = size_dist.get(size, 0) + 1

            ws_id = w.get("workspace_id", "")
            az    = self._ws_azure.get(ws_id, {})
            entry = {
                "warehouse_id":       wid,
                "warehouse_name":     w.get("warehouse_name", ""),
                "workspace_id":       ws_id,
                "workspace_name":     az.get("workspace_name", ""),
                "subscription_name":  az.get("subscription_name", ""),
                "resource_group":     az.get("resource_group", ""),
                "warehouse_type":     w.get("warehouse_type", ""),
                "warehouse_size":     size,
                "min_clusters":       min_c,
                "max_clusters":       max_c,
                "auto_stop_minutes":  astop,
                "dbus_30d":          u.get("dbus_30d", 0),
                "cost_30d":          u.get("cost_30d", 0),
                "dbus_60d":          u.get("dbus_60d", 0),
                "cost_60d":          u.get("cost_60d", 0),
                "dbus_90d":          u.get("dbus_90d", 0),
                "cost_90d":          u.get("cost_90d", 0),
                "active_days":       u.get("active_days", 0),
                "flags":             flags,
            }
            wh_list.append(entry)

        wh_list.sort(key=lambda x: (-len(x["flags"]), -x["cost_90d"]))

        # ── DBR version breakdown ─────────────────────────────────────────────
        dbr_list = sorted(
            [{"version": k, "cluster_count": v} for k, v in dbr_versions.items()],
            key=lambda x: -x["cluster_count"],
        )

        # ── Size distribution ─────────────────────────────────────────────────
        size_list = sorted(
            [{"size": k, "count": v} for k, v in size_dist.items()],
            key=lambda x: -x["count"],
        )

        # ── DLT Pipeline analysis ─────────────────────────────────────────────
        # Primary source: billing (all workspaces). Enriched with SDK info
        # for pipelines in the current workspace.
        dlt_list = []
        dlt_no_trigger = 0
        sdk_info = sdk_pipeline_info or {}
        if dlt_billing:
            for r in dlt_billing:
                pid = r.get("pipeline_id", "")
                if not pid:
                    continue
                wid = r.get("workspace_id", "")
                az  = self._ws_azure.get(wid, {})
                si  = sdk_info.get(pid, {})

                has_trigger  = si.get("has_trigger", None)   # None = unknown
                trigger_type = si.get("trigger_type", "Unknown")
                name         = si.get("name", "")
                state        = si.get("state", "")

                # If SDK didn't have this pipeline, mark as unknown
                in_sdk = pid in sdk_info

                if in_sdk and not has_trigger:
                    dlt_no_trigger += 1

                entry = {
                    "pipeline_id":     pid,
                    "name":            name or pid[:20] + "…",
                    "state":           state or ("—" if not in_sdk else ""),
                    "has_trigger":     has_trigger if in_sdk else None,
                    "trigger_type":    trigger_type if in_sdk else "Unknown",
                    "trigger_detail":  si.get("trigger_detail", ""),
                    "workspace_id":    wid,
                    "workspace_name":  az.get("workspace_name", ""),
                    "subscription_name": az.get("subscription_name", ""),
                    "resource_group":  az.get("resource_group", ""),
                    "dbus_30d":        float(r.get("dbus_30d") or 0),
                    "cost_30d":        float(r.get("cost_30d") or 0),
                    "dbus_60d":        float(r.get("dbus_60d") or 0),
                    "cost_60d":        float(r.get("cost_60d") or 0),
                    "dbus_90d":        float(r.get("dbus_90d") or 0),
                    "cost_90d":        float(r.get("cost_90d") or 0),
                    "active_days":     int(r.get("active_days") or 0),
                }
                dlt_list.append(entry)
            dlt_list.sort(key=lambda x: -x["cost_90d"])

        # Count underutilized clusters (avg util < 20% with significant cost)
        underutilized = sum(
            1 for c in cluster_list
            if c.get("avg_task_util_pct") is not None
            and c["avg_task_util_pct"] < 20
            and c["cost_30d"] > 10
        )

        # Build node types reference list for the frontend
        nt_list = sorted(
            [{"node_type_id": k, **v} for k, v in nt_map.items()],
            key=lambda x: -x["memory_mb"],
        )[:50]  # top 50 by memory

        return {
            "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "summary": {
                "total_clusters":      len(cluster_list),
                "total_warehouses":    len(wh_list),
                "total_dlt_pipelines": len(dlt_list),
                "no_auto_terminate":   no_autoterminate,
                "no_auto_stop":        no_autostop_wh,
                "no_autoscale":        no_autoscale,
                "oversized_clusters":  oversized,
                "dlt_no_trigger":      dlt_no_trigger,
                "dbr_versions_in_use": len(dbr_versions),
                "underutilized":       underutilized,
            },
            "clusters":              cluster_list,
            "warehouses":            wh_list,
            "dlt_pipelines":         dlt_list,
            "dbr_version_breakdown": dbr_list,
            "warehouse_sizes":       size_list,
            "node_types":            nt_list,
        }

    def _fetch_jobs_compute_cost(self) -> list[dict]:
        """Cost per job (last 30 days) from system.billing.usage joined to system.lakeflow.jobs."""
        d30 = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
        return safe_execute_sync(self._client, self._wh_id, f"""
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
        """, label="Compute-JobCost")

    # ── main entry point ──────────────────────────────────────────────────────

    def get_compute_insights(self) -> dict:
        """Fetch compute configs and billing, return right-sizing analytics.

        Results are cached for CACHE_TTL_MIN minutes.
        """
        now = datetime.datetime.now(datetime.timezone.utc)

        if self._cache and self._cached_at:
            age = (now - self._cached_at).total_seconds() / 60
            if age < self.CACHE_TTL_MIN:
                return {**self._cache, "cache_age_minutes": round(age, 1)}

        with ThreadPoolExecutor(max_workers=9) as pool:
            f_clusters    = pool.submit(self._fetch_clusters)
            f_warehouses  = pool.submit(self._fetch_warehouses)
            f_util        = pool.submit(self._fetch_cluster_utilisation)
            f_recent      = pool.submit(self._fetch_recent_active_ids)
            f_dlt         = pool.submit(self._fetch_dlt_billing)
            f_pipe        = pool.submit(self._fetch_sdk_pipeline_info)
            f_node_util   = pool.submit(self._fetch_node_utilization)
            f_node_types  = pool.submit(self._fetch_node_types_info)
            f_job_cost    = pool.submit(self._fetch_jobs_compute_cost)

        def _safe_result(future, default=None):
            try:
                return future.result()
            except Exception:
                return default if default is not None else []

        clusters        = _safe_result(f_clusters)
        warehouses      = _safe_result(f_warehouses)
        utilisation     = _safe_result(f_util)
        recent_active   = _safe_result(f_recent, set())
        dlt_billing     = _safe_result(f_dlt)
        sdk_pipe_info   = _safe_result(f_pipe, {})
        node_util       = _safe_result(f_node_util)
        node_types_info = _safe_result(f_node_types, {})
        jobs_cost       = _safe_result(f_job_cost)

        result = self._analyse(
            clusters, warehouses, utilisation, recent_active,
            dlt_billing, sdk_pipe_info, node_util, node_types_info,
        )
        result["jobs_compute_cost"] = [
            {
                "job_id":        str(r.get("job_id") or ""),
                "job_name":      r.get("job_name") or f'Job {r.get("job_id","")}',
                "workspace_name": r.get("workspace_name") or r.get("workspace_id") or "",
                "sku_name":      r.get("sku_name") or "",
                "run_count":     int(r.get("run_count") or 0),
                "total_dbus":    float(r.get("total_dbus") or 0),
                "total_cost":    float(r.get("total_cost") or 0),
                "active_days":   int(r.get("active_days") or 0),
                "last_run":      (r.get("last_run") or "")[:10],
            }
            for r in jobs_cost
        ]
        result["cache_age_minutes"] = 0

        self._cache     = result
        self._cached_at = now
        return result
