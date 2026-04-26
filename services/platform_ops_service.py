"""
Platform Operations: Networking, Dashboards, and Alerts.

Queries:
  - system.networking.private_endpoint_rules  (network posture)
  - system.lakeview.dashboard_events          (dashboard adoption)
  - system.dashboards.dashboard_events        (fallback path)
  - SDK: workspace.alerts / alerts_v2         (alert inventory)
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


class PlatformOpsService:
    LOOKBACK_DAYS = 30
    CACHE_TTL_MIN = 30

    def __init__(self, client: WorkspaceClient) -> None:
        self._client = client
        self._wh_id  = get_settings().databricks_warehouse_id
        self._cache: Optional[dict]                  = None
        self._cached_at: Optional[datetime.datetime] = None

    def _safe(self, sql: str) -> list[dict]:
        return safe_execute_sync(self._client, self._wh_id, sql, label="PlatformOps")

    # ── Networking: private endpoints ─────────────────────────────────────────

    def _fetch_private_endpoints(self) -> list[dict]:
        return self._safe("""
            SELECT
                workspace_id,
                COALESCE(name, endpoint_name, '')          AS endpoint_name,
                COALESCE(private_endpoint_id, endpoint_id, '') AS endpoint_id,
                COALESCE(resource_id, '')                  AS resource_id,
                COALESCE(sub_resource_target, group_id, '') AS sub_resource_target,
                COALESCE(domain, fqdn, '')                 AS domain,
                COALESCE(state, status, '')                AS status,
                CAST(creation_time AS STRING)              AS created_at
            FROM system.networking.private_endpoint_rules
            ORDER BY workspace_id
            LIMIT 500
        """)

    def _fetch_vpc_endpoints(self) -> list[dict]:
        """Alternative table name for VPC/private endpoints."""
        return self._safe("""
            SELECT
                workspace_id,
                vpc_endpoint_id   AS endpoint_id,
                endpoint_name,
                aws_account_id    AS resource_id,
                region,
                state             AS status,
                CAST(creation_time AS STRING) AS created_at
            FROM system.networking.vpc_endpoints
            ORDER BY workspace_id
            LIMIT 500
        """)

    # ── Dashboards ────────────────────────────────────────────────────────────

    def _fetch_dashboard_events(self) -> list[dict]:
        # Try system.lakeview first
        rows = self._safe(f"""
            SELECT
                workspace_id,
                dashboard_id,
                dashboard_name,
                event_type,
                user_id,
                CAST(event_time AS STRING) AS event_time
            FROM system.lakeview.dashboard_events
            WHERE event_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
            ORDER BY event_time DESC
            LIMIT 5000
        """)
        if rows:
            return rows
        # Fallback path
        return self._safe(f"""
            SELECT
                workspace_id,
                dashboard_id,
                dashboard_name,
                event_type,
                user_id,
                CAST(event_time AS STRING) AS event_time
            FROM system.dashboards.dashboard_events
            WHERE event_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
            ORDER BY event_time DESC
            LIMIT 5000
        """)

    def _fetch_dashboard_inventory(self) -> list[dict]:
        rows = self._safe("""
            SELECT
                dashboard_id,
                name         AS dashboard_name,
                owner_username AS owner,
                CAST(created_time  AS STRING) AS created_at,
                CAST(updated_time  AS STRING) AS updated_at
            FROM system.lakeview.dashboards
            ORDER BY updated_time DESC
            LIMIT 500
        """)
        if rows:
            return rows
        return self._safe("""
            SELECT
                dashboard_id,
                name         AS dashboard_name,
                owner_username AS owner,
                CAST(created_time  AS STRING) AS created_at,
                CAST(updated_time  AS STRING) AS updated_at
            FROM system.dashboards.dashboards
            ORDER BY updated_time DESC
            LIMIT 500
        """)

    # ── Alerts (SDK) ──────────────────────────────────────────────────────────

    def _fetch_alerts(self) -> list[dict]:
        # Try alerts_v2 first (newer API)
        try:
            out = []
            for alert in self._client.alerts_v2.list():
                out.append({
                    "alert_id":    getattr(alert, "id", ""),
                    "name":        getattr(alert, "display_name", "") or getattr(alert, "name", ""),
                    "state":       str(getattr(alert, "state", "") or ""),
                    "severity":    str(getattr(alert, "severity", "") or ""),
                    "owner":       getattr(alert, "owner_user_name", "") or "",
                    "created_at":  str(getattr(alert, "create_time", "") or ""),
                    "updated_at":  str(getattr(alert, "update_time", "") or ""),
                    "query_id":    str(getattr(alert, "query_id", "") or ""),
                })
            if out:
                return out
        except Exception as e:
            _log.debug("alerts_v2.list failed: %s", e)

        # Legacy alerts
        try:
            out = []
            for alert in self._client.alerts.list():
                out.append({
                    "alert_id":   getattr(alert, "id", ""),
                    "name":       getattr(alert, "name", ""),
                    "state":      str(getattr(alert, "state", "") or ""),
                    "severity":   "",
                    "owner":      getattr(alert, "user", {}).get("email", "") if isinstance(
                        getattr(alert, "user", None), dict) else "",
                    "created_at": str(getattr(alert, "created_at", "") or ""),
                    "updated_at": str(getattr(alert, "updated_at", "") or ""),
                    "query_id":   str(getattr(getattr(alert, "query", None), "id", "") or ""),
                })
            return out
        except Exception as e:
            _log.debug("alerts.list failed: %s", e)
            return []

    # ── Analysis ──────────────────────────────────────────────────────────────

    def _analyse(self, priv_endpoints, vpc_endpoints, dash_events,
                 dash_inventory, alerts) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)

        # Merge endpoint sources
        all_endpoints = priv_endpoints or vpc_endpoints

        # Endpoint status distribution
        ep_status: dict = {}
        ep_by_ws:  dict = {}
        for ep in all_endpoints:
            st = ep.get("status", "unknown") or "unknown"
            ws = ep.get("workspace_id", "unknown") or "unknown"
            ep_status[st] = ep_status.get(st, 0) + 1
            ep_by_ws[ws]  = ep_by_ws.get(ws, 0) + 1

        # Dashboard events
        event_type_counts: dict = {}
        user_counts:       dict = {}
        dashboard_counts:  dict = {}
        daily_dash:        dict = {}
        for ev in dash_events:
            et  = ev.get("event_type", "unknown")
            uid = ev.get("user_id", "unknown") or "unknown"
            did = ev.get("dashboard_name", "") or ev.get("dashboard_id", "unknown")
            day = (ev.get("event_time") or "")[:10]
            event_type_counts[et] = event_type_counts.get(et, 0) + 1
            user_counts[uid]      = user_counts.get(uid, 0) + 1
            dashboard_counts[did] = dashboard_counts.get(did, 0) + 1
            if day:
                daily_dash[day] = daily_dash.get(day, 0) + 1

        top_dashboards = sorted(
            [{"dashboard": k, "views": v} for k, v in dashboard_counts.items()],
            key=lambda x: -x["views"])[:20]

        top_users = sorted(
            [{"user": k, "events": v} for k, v in user_counts.items()],
            key=lambda x: -x["events"])[:20]

        # Alert summary
        alert_states: dict = {}
        for a in alerts:
            st = a.get("state", "unknown") or "unknown"
            alert_states[st] = alert_states.get(st, 0) + 1

        return {
            "generated_at":  now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lookback_days": self.LOOKBACK_DAYS,
            "summary": {
                "private_endpoints": len(all_endpoints),
                "dashboard_events":  len(dash_events),
                "dashboards":        len(dash_inventory),
                "active_alerts":     len(alerts),
                "unique_dash_users": len(user_counts),
            },
            "endpoint_status_dist": sorted(
                [{"status": k, "count": v} for k, v in ep_status.items()],
                key=lambda x: -x["count"]),
            "endpoints_by_workspace": sorted(
                [{"workspace_id": k, "count": v} for k, v in ep_by_ws.items()],
                key=lambda x: -x["count"]),
            "private_endpoints": [{
                "workspace_id":       ep.get("workspace_id", ""),
                "endpoint_name":      ep.get("endpoint_name", ""),
                "endpoint_id":        ep.get("endpoint_id", ""),
                "resource_id":        ep.get("resource_id", ""),
                "sub_resource_target": ep.get("sub_resource_target", ""),
                "domain":             ep.get("domain", ""),
                "status":             ep.get("status", ""),
                "created_at":         ep.get("created_at", ""),
            } for ep in all_endpoints],
            "dashboard_daily_trend": sorted(
                [{"date": k, "events": v} for k, v in daily_dash.items()],
                key=lambda x: x["date"]),
            "top_dashboards": top_dashboards,
            "top_dashboard_users": top_users,
            "dashboard_event_types": sorted(
                [{"type": k, "count": v} for k, v in event_type_counts.items()],
                key=lambda x: -x["count"]),
            "dashboard_inventory": [{
                "dashboard_id":   d.get("dashboard_id", ""),
                "dashboard_name": d.get("dashboard_name", ""),
                "owner":          d.get("owner", ""),
                "created_at":     d.get("created_at", ""),
                "updated_at":     d.get("updated_at", ""),
            } for d in dash_inventory],
            "alerts": alerts,
            "alert_state_dist": sorted(
                [{"state": k, "count": v} for k, v in alert_states.items()],
                key=lambda x: -x["count"]),
        }

    def get_platform_ops(self) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)
        if self._cache and self._cached_at:
            age = (now - self._cached_at).total_seconds() / 60
            if age < self.CACHE_TTL_MIN:
                return {**self._cache, "cache_age_minutes": round(age, 1)}

        with ThreadPoolExecutor(max_workers=5) as pool:
            f_priv  = pool.submit(self._fetch_private_endpoints)
            f_vpc   = pool.submit(self._fetch_vpc_endpoints)
            f_devt  = pool.submit(self._fetch_dashboard_events)
            f_dinv  = pool.submit(self._fetch_dashboard_inventory)
            f_alrt  = pool.submit(self._fetch_alerts)

        def _safe_result(future, default=None):
            try:
                return future.result()
            except Exception:
                return default if default is not None else []

        priv_endpoints = _safe_result(f_priv)
        # Use VPC results only when private endpoint table returned nothing
        vpc_endpoints  = _safe_result(f_vpc) if not priv_endpoints else []
        dash_events    = _safe_result(f_devt)
        dash_inventory = _safe_result(f_dinv)
        alerts         = _safe_result(f_alrt)

        result = self._analyse(priv_endpoints, vpc_endpoints,
                               dash_events, dash_inventory, alerts)
        result["cache_age_minutes"] = 0
        self._cache    = result
        self._cached_at = now
        return result
