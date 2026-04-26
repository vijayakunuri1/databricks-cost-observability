"""
Platform Governance: IAM events, Service Principals, Groups, Tokens.

Queries system.access.audit for identity lifecycle events and falls
back to the SDK for SP / group inventory.
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

_IAM_SERVICES = (
    "'iamService','accountsManager','tokenManagement','groups',"
    "'servicePrincipals','users','permissions','iam','accounts'"
)

_IAM_ACTIONS = (
    "'createServicePrincipal','deleteServicePrincipal','updateServicePrincipal',"
    "'createGroup','deleteGroup','updateGroup','addMember','removeMember',"
    "'createToken','deleteToken','revokeToken',"
    "'setPermissions','updatePermissions','changePermissions',"
    "'assignPrincipalToWorkspace','unassignPrincipalFromWorkspace',"
    "'createUser','deleteUser','updateUser',"
    "'addPrincipalToGroup','removePrincipalFromGroup'"
)


class GovernanceService:
    LOOKBACK_DAYS = 30
    CACHE_TTL_MIN = 30

    def __init__(self, client: WorkspaceClient) -> None:
        self._client = client
        self._wh_id = get_settings().databricks_warehouse_id
        self._cache: Optional[dict] = None
        self._cached_at: Optional[datetime.datetime] = None

    def _safe(self, sql: str) -> list[dict]:
        return safe_execute_sync(self._client, self._wh_id, sql, label="Governance")

    # ── IAM audit events ──────────────────────────────────────────────────────

    def _fetch_iam_events(self) -> list[dict]:
        return self._safe(f"""
            SELECT
                CAST(event_time AS STRING)          AS event_time,
                workspace_id,
                service_name,
                action_name,
                COALESCE(user_identity.email, '')    AS actor,
                CAST(request_params AS STRING)      AS request_params,
                CAST(response AS STRING)            AS response_str
            FROM system.access.audit
            WHERE event_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
              AND (
                    service_name IN ({_IAM_SERVICES})
                 OR action_name  IN ({_IAM_ACTIONS})
              )
            ORDER BY event_time DESC
            LIMIT 3000
        """)

    # ── Service Principals ────────────────────────────────────────────────────

    def _fetch_service_principals(self) -> list[dict]:
        rows = self._safe(f"""
            SELECT
                CAST(action_name AS STRING) AS action_name,
                CAST(request_params AS STRING) AS request_params,
                CAST(event_time AS STRING) AS event_time
            FROM system.access.audit
            WHERE event_date >= date_add(current_date(), -{self.LOOKBACK_DAYS})
              AND action_name IN ('createServicePrincipal','deleteServicePrincipal','updateServicePrincipal')
            ORDER BY event_time DESC
            LIMIT 500
        """)
        if rows:
            return rows
        # SDK fallback — list current SPs
        try:
            out = []
            for sp in self._client.service_principals.list(count=200):
                out.append({
                    "action_name":    "inventory",
                    "request_params": f"id={sp.id} displayName={getattr(sp, 'display_name', '')}",
                    "event_time":     "",
                })
            return out
        except Exception as e:
            _log.debug("SP SDK list failed: %s", e)
            return []

    # ── Token events ──────────────────────────────────────────────────────────

    def _fetch_token_events(self) -> list[dict]:
        return self._safe(f"""
            SELECT
                CAST(event_time AS STRING) AS event_time,
                workspace_id,
                action_name,
                COALESCE(user_identity.email, '') AS actor,
                CAST(request_params AS STRING)    AS request_params
            FROM system.access.audit
            WHERE event_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
              AND action_name IN ('createToken','deleteToken','revokeToken')
            ORDER BY event_time DESC
            LIMIT 1000
        """)

    # ── Group membership events ───────────────────────────────────────────────

    def _fetch_group_events(self) -> list[dict]:
        return self._safe(f"""
            SELECT
                CAST(event_time AS STRING) AS event_time,
                workspace_id,
                action_name,
                COALESCE(user_identity.email, '') AS actor,
                CAST(request_params AS STRING)    AS request_params
            FROM system.access.audit
            WHERE event_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
              AND action_name IN (
                    'createGroup','deleteGroup','updateGroup',
                    'addMember','removeMember',
                    'addPrincipalToGroup','removePrincipalFromGroup'
              )
            ORDER BY event_time DESC
            LIMIT 1000
        """)

    # ── Permission change events ──────────────────────────────────────────────

    def _fetch_permission_events(self) -> list[dict]:
        return self._safe(f"""
            SELECT
                CAST(event_time AS STRING) AS event_time,
                workspace_id,
                action_name,
                COALESCE(user_identity.email, '') AS actor,
                CAST(request_params AS STRING)    AS request_params
            FROM system.access.audit
            WHERE event_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
              AND action_name IN (
                    'setPermissions','updatePermissions','changePermissions'
              )
            ORDER BY event_time DESC
            LIMIT 1000
        """)

    # ── SDK: current SP & group inventory ────────────────────────────────────

    def _fetch_sp_inventory(self) -> list[dict]:
        try:
            out = []
            for sp in self._client.service_principals.list(count=500):
                out.append({
                    "id":           str(getattr(sp, "id", "")),
                    "display_name": getattr(sp, "display_name", "") or "",
                    "active":       str(getattr(sp, "active", True)),
                    "app_id":       str(getattr(sp, "application_id", "") or ""),
                })
            return out
        except Exception as e:
            _log.debug("SP inventory failed: %s", e)
            return []

    def _fetch_group_inventory(self) -> list[dict]:
        try:
            out = []
            for g in self._client.groups.list(count=500):
                members = getattr(g, "members", None) or []
                out.append({
                    "id":           str(getattr(g, "id", "")),
                    "display_name": getattr(g, "display_name", "") or "",
                    "member_count": len(members),
                })
            return out
        except Exception as e:
            _log.debug("Group inventory failed: %s", e)
            return []

    # ── Analysis ──────────────────────────────────────────────────────────────

    def _analyse(self, iam_events, token_events, group_events,
                 perm_events, sp_inv, grp_inv) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)

        # Breakdown by action
        action_counts: dict = {}
        actor_counts:  dict = {}
        daily_counts:  dict = {}
        for ev in iam_events:
            action = ev.get("action_name", "unknown")
            actor  = ev.get("actor", "unknown") or "unknown"
            day    = (ev.get("event_time") or "")[:10]
            action_counts[action] = action_counts.get(action, 0) + 1
            actor_counts[actor]   = actor_counts.get(actor, 0) + 1
            if day:
                daily_counts[day] = daily_counts.get(day, 0) + 1

        top_actions = sorted(
            [{"action": k, "count": v} for k, v in action_counts.items()],
            key=lambda x: -x["count"])[:20]

        top_actors = sorted(
            [{"actor": k, "count": v} for k, v in actor_counts.items()],
            key=lambda x: -x["count"])[:20]

        daily_trend = sorted(
            [{"date": k, "count": v} for k, v in daily_counts.items()],
            key=lambda x: x["date"])

        # Token summary
        token_creates = sum(1 for t in token_events if "create" in t.get("action_name", "").lower())
        token_deletes  = len(token_events) - token_creates

        # Group change summary
        group_adds    = sum(1 for g in group_events if "add" in g.get("action_name", "").lower() or "create" in g.get("action_name", "").lower())
        group_removes = len(group_events) - group_adds

        return {
            "generated_at":  now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lookback_days": self.LOOKBACK_DAYS,
            "summary": {
                "total_iam_events":  len(iam_events),
                "unique_actors":     len(actor_counts),
                "unique_actions":    len(action_counts),
                "token_events":      len(token_events),
                "group_events":      len(group_events),
                "permission_events": len(perm_events),
                "sp_count":          len(sp_inv),
                "group_count":       len(grp_inv),
            },
            "top_actions":  top_actions,
            "top_actors":   top_actors,
            "daily_trend":  daily_trend,
            "token_summary": {
                "creates": token_creates,
                "deletes": token_deletes,
                "events":  [{
                    "event_time":    e.get("event_time", ""),
                    "action":        e.get("action_name", ""),
                    "actor":         e.get("actor", ""),
                    "workspace_id":  e.get("workspace_id", ""),
                } for e in token_events[:200]],
            },
            "group_summary": {
                "adds":    group_adds,
                "removes": group_removes,
                "events":  [{
                    "event_time":   e.get("event_time", ""),
                    "action":       e.get("action_name", ""),
                    "actor":        e.get("actor", ""),
                    "workspace_id": e.get("workspace_id", ""),
                    "params":       e.get("request_params", ""),
                } for e in group_events[:200]],
            },
            "permission_events": [{
                "event_time":   e.get("event_time", ""),
                "action":       e.get("action_name", ""),
                "actor":        e.get("actor", ""),
                "workspace_id": e.get("workspace_id", ""),
                "params":       e.get("request_params", ""),
            } for e in perm_events[:200]],
            "recent_events": [{
                "event_time":   e.get("event_time", ""),
                "action":       e.get("action_name", ""),
                "service":      e.get("service_name", ""),
                "actor":        e.get("actor", ""),
                "workspace_id": e.get("workspace_id", ""),
            } for e in iam_events[:500]],
            "sp_inventory":    sp_inv,
            "group_inventory": grp_inv,
        }

    def get_governance_insights(self) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)
        if self._cache and self._cached_at:
            age = (now - self._cached_at).total_seconds() / 60
            if age < self.CACHE_TTL_MIN:
                return {**self._cache, "cache_age_minutes": round(age, 1)}

        with ThreadPoolExecutor(max_workers=6) as pool:
            f_iam   = pool.submit(self._fetch_iam_events)
            f_tok   = pool.submit(self._fetch_token_events)
            f_grp   = pool.submit(self._fetch_group_events)
            f_perm  = pool.submit(self._fetch_permission_events)
            f_sp    = pool.submit(self._fetch_sp_inventory)
            f_ginv  = pool.submit(self._fetch_group_inventory)

        def _safe_result(future, default=None):
            try:
                return future.result()
            except Exception:
                return default if default is not None else []

        iam_events   = _safe_result(f_iam)
        token_events = _safe_result(f_tok)
        group_events = _safe_result(f_grp)
        perm_events  = _safe_result(f_perm)
        sp_inv       = _safe_result(f_sp)
        grp_inv      = _safe_result(f_ginv)

        result = self._analyse(iam_events, token_events, group_events,
                               perm_events, sp_inv, grp_inv)
        result["cache_age_minutes"] = 0
        self._cache    = result
        self._cached_at = now
        return result
