"""
Cluster Health: cluster events timeline and node-level utilisation.

Queries system.compute.cluster_events and system.compute.node_timeline.
Falls back to SDK cluster list when system tables are unavailable.
"""

from __future__ import annotations

import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from databricks.sdk import WorkspaceClient

from core.config import get_settings
from core.sql_executor import safe_execute_sync

_log = logging.getLogger(__name__)


class ClusterHealthService:
    LOOKBACK_DAYS = 30
    NODE_LOOKBACK_DAYS = 7      # node_timeline can be very large
    CACHE_TTL_MIN = 20

    def __init__(self, client: WorkspaceClient) -> None:
        self._client = client
        self._wh_id  = get_settings().databricks_warehouse_id
        self._cache: Optional[dict]                  = None
        self._cached_at: Optional[datetime.datetime] = None

    def _safe(self, sql: str) -> list[dict]:
        return safe_execute_sync(self._client, self._wh_id, sql, label="ClusterHealth")

    # ── Cluster Events ────────────────────────────────────────────────────────

    def _fetch_cluster_events(self) -> list[dict]:
        return self._safe(f"""
            SELECT
                workspace_id,
                cluster_id,
                CAST(timestamp AS STRING) AS event_time,
                type                      AS event_type,
                CAST(details AS STRING)   AS details
            FROM system.compute.cluster_events
            WHERE timestamp >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
            ORDER BY timestamp DESC
            LIMIT 5000
        """)

    def _fetch_cluster_event_summary(self) -> list[dict]:
        return self._safe(f"""
            SELECT
                cluster_id,
                type          AS event_type,
                COUNT(*)      AS event_count,
                CAST(MIN(timestamp) AS STRING) AS first_seen,
                CAST(MAX(timestamp) AS STRING) AS last_seen
            FROM system.compute.cluster_events
            WHERE timestamp >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
            GROUP BY cluster_id, type
            ORDER BY event_count DESC
            LIMIT 2000
        """)

    # ── Node Timeline ─────────────────────────────────────────────────────────

    def _fetch_node_utilisation(self) -> list[dict]:
        return self._safe(f"""
            SELECT
                cluster_id,
                driver,
                CAST(DATE(start_time) AS STRING)      AS date,
                ROUND(AVG(cpu_user_percent + cpu_system_percent), 1) AS avg_cpu_pct,
                ROUND(MAX(cpu_user_percent + cpu_system_percent), 1) AS peak_cpu_pct,
                ROUND(AVG(mem_used_percent), 1)        AS avg_mem_pct,
                ROUND(MAX(mem_used_percent), 1)        AS peak_mem_pct,
                COUNT(DISTINCT COALESCE(node_id, instance_id)) AS node_count
            FROM system.compute.node_timeline
            WHERE start_time >= timestamp(date_add(current_date(), -{self.NODE_LOOKBACK_DAYS}))
            GROUP BY cluster_id, driver, DATE(start_time)
            ORDER BY date DESC, cluster_id
            LIMIT 3000
        """)

    def _fetch_node_peak_clusters(self) -> list[dict]:
        return self._safe(f"""
            SELECT
                cluster_id,
                ROUND(AVG(cpu_user_percent + cpu_system_percent), 1) AS avg_cpu_pct,
                ROUND(MAX(cpu_user_percent + cpu_system_percent), 1) AS peak_cpu_pct,
                ROUND(AVG(mem_used_percent), 1)        AS avg_mem_pct,
                ROUND(MAX(mem_used_percent), 1)        AS peak_mem_pct,
                COUNT(DISTINCT COALESCE(node_id, instance_id)) AS total_nodes,
                COUNT(DISTINCT DATE(start_time))       AS active_days
            FROM system.compute.node_timeline
            WHERE start_time >= timestamp(date_add(current_date(), -{self.NODE_LOOKBACK_DAYS}))
            GROUP BY cluster_id
            ORDER BY avg_cpu_pct DESC
            LIMIT 200
        """)

    # ── SDK fallback: cluster list ────────────────────────────────────────────

    def _fetch_cluster_inventory_sdk(self) -> list[dict]:
        try:
            out = []
            for c in self._client.clusters.list():
                state = str(getattr(c, "state", ""))
                out.append({
                    "cluster_id":   getattr(c, "cluster_id", ""),
                    "cluster_name": getattr(c, "cluster_name", ""),
                    "state":        state,
                    "driver_type":  getattr(c, "driver_node_type_id", ""),
                    "worker_type":  getattr(c, "node_type_id", ""),
                    "num_workers":  str(getattr(c, "num_workers", 0) or 0),
                    "spark_version": getattr(c, "spark_version", ""),
                    "creator":      getattr(c, "creator_user_name", ""),
                })
            return out
        except Exception as e:
            _log.debug("SDK cluster list failed: %s", e)
            return []

    # ── Analysis ──────────────────────────────────────────────────────────────

    def _analyse(self, events, event_summary, node_util, node_peaks, sdk_clusters) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)

        # Event type counts
        type_counts: dict = {}
        cluster_event_counts: dict = {}
        daily_events: dict = {}
        for e in events:
            et  = e.get("event_type", "unknown")
            cid = e.get("cluster_id", "")
            day = (e.get("event_time") or "")[:10]
            type_counts[et]        = type_counts.get(et, 0) + 1
            cluster_event_counts[cid] = cluster_event_counts.get(cid, 0) + 1
            if day:
                daily_events[day] = daily_events.get(day, 0) + 1

        # Failure events
        failure_types = {"FAILED", "ERROR", "DRIVER_NOT_FOUND",
                         "DRIVER_UNAVAILABLE", "SPARK_ERROR"}
        failures = [e for e in events
                    if (e.get("event_type") or "").upper() in failure_types]

        # Restart / scale events
        scale_types = {"RESIZING", "UPSIZE_COMPLETED", "NODES_LOST",
                       "AUTOSCALING_STATS_REPORT"}
        scale_events = [e for e in events
                        if (e.get("event_type") or "").upper() in scale_types]

        # Unique clusters seen in events
        unique_clusters = len({e.get("cluster_id") for e in events if e.get("cluster_id")})

        # Node utilisation summary
        high_cpu = [n for n in node_peaks if (n.get("avg_cpu_pct") or 0) > 70]
        high_mem = [n for n in node_peaks if (n.get("avg_mem_pct") or 0) > 80]

        return {
            "generated_at":  now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lookback_days": self.LOOKBACK_DAYS,
            "summary": {
                "total_events":    len(events),
                "unique_clusters": unique_clusters,
                "failure_events":  len(failures),
                "scale_events":    len(scale_events),
                "high_cpu_clusters": len(high_cpu),
                "high_mem_clusters": len(high_mem),
                "sdk_clusters":    len(sdk_clusters),
            },
            "event_type_counts": sorted(
                [{"type": k, "count": v} for k, v in type_counts.items()],
                key=lambda x: -x["count"]),
            "daily_trend": sorted(
                [{"date": k, "count": v} for k, v in daily_events.items()],
                key=lambda x: x["date"]),
            "failure_events": [{
                "event_time":  e.get("event_time", ""),
                "cluster_id":  e.get("cluster_id", ""),
                "event_type":  e.get("event_type", ""),
                "details":     (e.get("details") or "")[:300],
                "workspace_id": e.get("workspace_id", ""),
            } for e in failures[:200]],
            "scale_events": [{
                "event_time":  e.get("event_time", ""),
                "cluster_id":  e.get("cluster_id", ""),
                "event_type":  e.get("event_type", ""),
                "details":     (e.get("details") or "")[:200],
            } for e in scale_events[:200]],
            "cluster_event_summary": sorted(
                [{"cluster_id": k, "total_events": v}
                 for k, v in cluster_event_counts.items()],
                key=lambda x: -x["total_events"])[:50],
            "node_utilisation_daily": [{
                "cluster_id":  n.get("cluster_id", ""),
                "date":        n.get("date", ""),
                "driver":      n.get("driver", False),
                "avg_cpu_pct": float(n.get("avg_cpu_pct") or 0),
                "peak_cpu_pct": float(n.get("peak_cpu_pct") or 0),
                "avg_mem_pct": float(n.get("avg_mem_pct") or 0),
                "peak_mem_pct": float(n.get("peak_mem_pct") or 0),
                "node_count":  int(n.get("node_count") or 0),
            } for n in node_util[:500]],
            "cluster_utilisation": [{
                "cluster_id":   n.get("cluster_id", ""),
                "avg_cpu_pct":  float(n.get("avg_cpu_pct") or 0),
                "peak_cpu_pct": float(n.get("peak_cpu_pct") or 0),
                "avg_mem_pct":  float(n.get("avg_mem_pct") or 0),
                "peak_mem_pct": float(n.get("peak_mem_pct") or 0),
                "total_nodes":  int(n.get("total_nodes") or 0),
                "active_days":  int(n.get("active_days") or 0),
            } for n in node_peaks],
            "recent_events": [{
                "event_time":  e.get("event_time", ""),
                "cluster_id":  e.get("cluster_id", ""),
                "event_type":  e.get("event_type", ""),
                "workspace_id": e.get("workspace_id", ""),
            } for e in events[:500]],
            "sdk_clusters": sdk_clusters,
        }

    def get_cluster_health(self) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)
        if self._cache and self._cached_at:
            age = (now - self._cached_at).total_seconds() / 60
            if age < self.CACHE_TTL_MIN:
                return {**self._cache, "cache_age_minutes": round(age, 1)}

        with ThreadPoolExecutor(max_workers=5) as pool:
            f_events  = pool.submit(self._fetch_cluster_events)
            f_summ    = pool.submit(self._fetch_cluster_event_summary)
            f_util    = pool.submit(self._fetch_node_utilisation)
            f_peaks   = pool.submit(self._fetch_node_peak_clusters)
            f_sdk     = pool.submit(self._fetch_cluster_inventory_sdk)

        def _safe_result(future, default=None):
            try:
                return future.result()
            except Exception:
                return default if default is not None else []

        events       = _safe_result(f_events)
        event_summ   = _safe_result(f_summ)
        node_util    = _safe_result(f_util)
        node_peaks   = _safe_result(f_peaks)
        sdk_clusters = _safe_result(f_sdk)

        result = self._analyse(events, event_summ, node_util, node_peaks, sdk_clusters)
        result["cache_age_minutes"] = 0
        self._cache    = result
        self._cached_at = now
        return result
