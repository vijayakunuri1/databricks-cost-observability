"""
User Adoption & Audit analytics from system.access.audit.

Provides executive-level visibility into:
  - Active vs inactive users (license utilisation)
  - Daily active user trends
  - Service usage breakdown (clusters, SQL, jobs, etc.)
  - Top users by activity volume
  - Inactive user detection (licence waste candidates)
"""

from __future__ import annotations

import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from collections import Counter

from databricks.sdk import WorkspaceClient

from core.config import get_settings
from core.sql_executor import execute_sync


class AuditService:
    LOOKBACK_DAYS   = 90
    CACHE_TTL_MIN   = 30

    def __init__(self, client: WorkspaceClient) -> None:
        self._client = client
        self._wh_id  = get_settings().databricks_warehouse_id
        self._cache: Optional[dict]                  = None
        self._cached_at: Optional[datetime.datetime] = None

    # ── SQL execution ─────────────────────────────────────────────────────────

    def _execute(self, sql: str) -> list[dict]:
        return execute_sync(self._client, self._wh_id, sql, label="Audit")

    # ── data queries ──────────────────────────────────────────────────────────

    def _fetch_user_activity(self) -> list[dict]:
        """Per-user activity summary over the lookback window."""
        return self._execute(f"""
            SELECT
                user_identity.email                           AS user_email,
                CAST(MAX(event_time) AS STRING)               AS last_active,
                COUNT(*)                                      AS action_count,
                COUNT(DISTINCT event_date)                    AS active_days,
                COUNT(DISTINCT workspace_id)                  AS workspaces_accessed,
                COUNT(DISTINCT service_name)                  AS services_used
            FROM system.access.audit
            WHERE event_date >= date_add(current_date(), -{self.LOOKBACK_DAYS})
              AND user_identity.email IS NOT NULL
              AND TRIM(user_identity.email) != ''
            GROUP BY user_identity.email
            ORDER BY action_count DESC
            LIMIT 500
        """)

    def _fetch_daily_active(self) -> list[dict]:
        """Daily active user counts and total actions."""
        return self._execute(f"""
            SELECT
                CAST(event_date AS STRING)                    AS date,
                COUNT(DISTINCT user_identity.email)           AS active_users,
                COUNT(*)                                      AS total_actions
            FROM system.access.audit
            WHERE event_date >= date_add(current_date(), -{self.LOOKBACK_DAYS})
              AND user_identity.email IS NOT NULL
              AND TRIM(user_identity.email) != ''
            GROUP BY event_date
            ORDER BY event_date
        """)

    def _fetch_service_breakdown(self) -> list[dict]:
        """Action counts by service name (last 30 days for relevance)."""
        return self._execute("""
            SELECT
                service_name,
                COUNT(*)                                      AS action_count,
                COUNT(DISTINCT user_identity.email)           AS unique_users
            FROM system.access.audit
            WHERE event_date >= date_add(current_date(), -30)
              AND user_identity.email IS NOT NULL
              AND TRIM(user_identity.email) != ''
            GROUP BY service_name
            ORDER BY action_count DESC
        """)

    def _fetch_top_actions(self) -> list[dict]:
        """Top action types across all services (last 30 days)."""
        return self._execute("""
            SELECT
                service_name,
                action_name,
                COUNT(*)                                      AS action_count,
                COUNT(DISTINCT user_identity.email)           AS unique_users
            FROM system.access.audit
            WHERE event_date >= date_add(current_date(), -30)
              AND user_identity.email IS NOT NULL
              AND TRIM(user_identity.email) != ''
            GROUP BY service_name, action_name
            ORDER BY action_count DESC
            LIMIT 50
        """)

    def _fetch_user_top_services(self) -> list[dict]:
        """Per-user top service by action count (last 90 days)."""
        return self._execute(f"""
            WITH ranked AS (
                SELECT
                    user_identity.email           AS user_email,
                    service_name,
                    COUNT(*)                      AS cnt,
                    ROW_NUMBER() OVER (
                        PARTITION BY user_identity.email
                        ORDER BY COUNT(*) DESC
                    )                             AS rn
                FROM system.access.audit
                WHERE event_date >= date_add(current_date(), -{self.LOOKBACK_DAYS})
                  AND user_identity.email IS NOT NULL
                  AND TRIM(user_identity.email) != ''
                GROUP BY user_identity.email, service_name
            )
            SELECT user_email, service_name AS top_service
            FROM ranked
            WHERE rn = 1
        """)

    # ── analysis ──────────────────────────────────────────────────────────────

    @staticmethod
    def _is_service_principal(email: str) -> bool:
        """Heuristic: SPs often have UUIDs or specific patterns."""
        if not email:
            return True
        e = email.lower().strip()
        # Numeric-only local part (Databricks Apps service identity)
        local = e.split("@")[0] if "@" in e else e
        if local.replace("-", "").replace("_", "").isdigit():
            return True
        # UUID-style emails
        parts = local.split("-")
        if len(parts) == 5 and all(len(p) in (8, 4, 4, 4, 12) for p in parts):
            return True
        return False

    def _compute_results(
        self,
        user_activity: list[dict],
        daily_active: list[dict],
        service_breakdown: list[dict],
        top_actions: list[dict],
        user_top_svc: list[dict],
    ) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)
        today = datetime.date.today()

        # Build top-service lookup
        top_svc_map = {
            r["user_email"]: r["top_service"]
            for r in user_top_svc if r.get("user_email")
        }

        # Classify users
        humans = []
        sps = []
        for row in user_activity:
            email = (row.get("user_email") or "").strip()
            if not email:
                continue
            entry = {
                "email":               email,
                "last_active":         row.get("last_active", ""),
                "action_count":        int(row.get("action_count") or 0),
                "active_days":         int(row.get("active_days") or 0),
                "workspaces_accessed": int(row.get("workspaces_accessed") or 0),
                "services_used":       int(row.get("services_used") or 0),
                "top_service":         top_svc_map.get(email, ""),
            }
            if self._is_service_principal(email):
                sps.append(entry)
            else:
                humans.append(entry)

        # Active / inactive classification (among humans)
        active_7d  = 0
        active_30d = 0
        inactive   = []
        cutoff_7d  = (today - datetime.timedelta(days=7)).isoformat()
        cutoff_30d = (today - datetime.timedelta(days=30)).isoformat()

        for u in humans:
            last = (u["last_active"] or "")[:10]  # YYYY-MM-DD
            if last >= cutoff_7d:
                active_7d += 1
            if last >= cutoff_30d:
                active_30d += 1
            else:
                days_gone = (today - datetime.date.fromisoformat(last)).days if last else 999
                inactive.append({**u, "days_inactive": days_gone})

        inactive.sort(key=lambda x: x["days_inactive"], reverse=True)

        return {
            "generated_at":    now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lookback_days":   self.LOOKBACK_DAYS,
            "summary": {
                "total_users":        len(humans),
                "active_7d":          active_7d,
                "active_30d":         active_30d,
                "inactive":           len(inactive),
                "service_principals": len(sps),
            },
            "daily_active_users":  [
                {
                    "date":         r.get("date", ""),
                    "active_users": int(r.get("active_users") or 0),
                    "total_actions": int(r.get("total_actions") or 0),
                }
                for r in daily_active
            ],
            "service_breakdown": [
                {
                    "service_name": r.get("service_name", ""),
                    "action_count": int(r.get("action_count") or 0),
                    "unique_users": int(r.get("unique_users") or 0),
                }
                for r in service_breakdown
            ],
            "top_actions": [
                {
                    "service_name": r.get("service_name", ""),
                    "action_name":  r.get("action_name", ""),
                    "action_count": int(r.get("action_count") or 0),
                    "unique_users": int(r.get("unique_users") or 0),
                }
                for r in top_actions
            ],
            "users":          humans,
            "inactive_users": inactive,
            "service_principals": sps,
        }

    # ── main entry point ──────────────────────────────────────────────────────

    def get_user_adoption(self) -> dict:
        """Fetch audit data and compute user adoption analytics.

        Results are cached for CACHE_TTL_MIN minutes.
        """
        now = datetime.datetime.now(datetime.timezone.utc)

        if self._cache and self._cached_at:
            age = (now - self._cached_at).total_seconds() / 60
            if age < self.CACHE_TTL_MIN:
                return {**self._cache, "cache_age_minutes": round(age, 1)}

        with ThreadPoolExecutor(max_workers=5) as pool:
            f_act  = pool.submit(self._fetch_user_activity)
            f_dly  = pool.submit(self._fetch_daily_active)
            f_svc  = pool.submit(self._fetch_service_breakdown)
            f_top  = pool.submit(self._fetch_top_actions)
            f_usvc = pool.submit(self._fetch_user_top_services)

        def _safe_result(future, default=None):
            try:
                return future.result()
            except Exception:
                return default if default is not None else []

        user_activity     = _safe_result(f_act)
        daily_active      = _safe_result(f_dly)
        service_breakdown = _safe_result(f_svc)
        top_actions       = _safe_result(f_top)
        user_top_svc      = _safe_result(f_usvc)

        result = self._compute_results(
            user_activity, daily_active, service_breakdown,
            top_actions, user_top_svc,
        )
        result["cache_age_minutes"] = 0

        self._cache     = result
        self._cached_at = now
        return result
