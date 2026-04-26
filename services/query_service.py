"""
Query Cost Attribution analytics from system.query.history.

Provides executive-level visibility into:
  - Top expensive queries by total execution time and bytes scanned
  - Per-user query cost attribution
  - Query volume trends
  - Slow query identification
  - Warehouse utilisation by query volume
"""

from __future__ import annotations

import datetime
from typing import Optional

from databricks.sdk import WorkspaceClient

from core.config import get_settings
from core.sql_executor import execute_sync, safe_execute_sync
from core.subscriptions import resolve_subscription


class QueryService:
    LOOKBACK_DAYS = 30
    CACHE_TTL_MIN = 30

    def __init__(self, client: WorkspaceClient, account_client=None) -> None:
        self._client = client
        self._wh_id  = get_settings().databricks_warehouse_id
        self._cache: Optional[dict]                  = None
        self._cached_at: Optional[datetime.datetime] = None

        # Build workspace Azure map
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
                        "subscription_name": resolve_subscription(sub) if sub else "",
                        "resource_group":    rg,
                    }
            except Exception:
                pass

    def _execute(self, sql: str) -> list[dict]:
        return execute_sync(self._client, self._wh_id, sql, label="Query")

    def _safe_execute(self, sql: str) -> list[dict]:
        return safe_execute_sync(self._client, self._wh_id, sql, label="Query")

    # ── data queries ──────────────────────────────────────────────────────────

    def _fetch_user_query_stats(self) -> list[dict]:
        """Per-user query statistics with 30/60/90 day breakdowns."""
        return self._execute("""
            SELECT
                executed_by                                       AS user_email,
                COUNT(*)                                          AS query_count,
                SUM(CASE WHEN start_time >= timestamp(date_add(current_date(), -30)) THEN 1 ELSE 0 END) AS queries_30d,
                SUM(CASE WHEN start_time >= timestamp(date_add(current_date(), -60)) THEN 1 ELSE 0 END) AS queries_60d,
                ROUND(SUM(total_duration_ms) / 1000.0, 1)        AS total_duration_sec,
                ROUND(AVG(total_duration_ms) / 1000.0, 2)        AS avg_duration_sec,
                ROUND(SUM(read_bytes) / (1024.0*1024*1024), 2)   AS total_gb_scanned,
                ROUND(SUM(CASE WHEN start_time >= timestamp(date_add(current_date(), -30))
                    THEN read_bytes ELSE 0 END) / (1024.0*1024*1024), 2) AS gb_30d,
                ROUND(SUM(CASE WHEN start_time >= timestamp(date_add(current_date(), -60))
                    THEN read_bytes ELSE 0 END) / (1024.0*1024*1024), 2) AS gb_60d,
                COUNT(DISTINCT compute.warehouse_id)              AS warehouses_used,
                SUM(CASE WHEN error_message IS NULL THEN 1 ELSE 0 END) AS success_count,
                SUM(CASE WHEN error_message IS NOT NULL THEN 1 ELSE 0 END) AS failed_count
            FROM system.query.history
            WHERE start_time >= timestamp(date_add(current_date(), -90))
              AND executed_by IS NOT NULL
              AND TRIM(executed_by) != ''
              AND statement_type != 'OTHER'
              AND executed_by = executed_as
            GROUP BY executed_by
            ORDER BY total_gb_scanned DESC
            LIMIT 200
        """)

    def _fetch_daily_query_trend(self) -> list[dict]:
        """Daily query volume and scan stats."""
        return self._execute(f"""
            SELECT
                CAST(DATE(start_time) AS STRING)                  AS date,
                COUNT(*)                                          AS query_count,
                COUNT(DISTINCT executed_by)                       AS unique_users,
                ROUND(SUM(read_bytes) / (1024.0*1024*1024), 2)   AS gb_scanned,
                ROUND(AVG(total_duration_ms) / 1000.0, 2)        AS avg_duration_sec
            FROM system.query.history
            WHERE start_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
              AND statement_type != 'OTHER'
            GROUP BY DATE(start_time)
            ORDER BY 1
        """)

    def _fetch_top_expensive_queries(self) -> list[dict]:
        """Top 50 most expensive queries by bytes scanned."""
        return self._execute(f"""
            SELECT
                statement_id,
                executed_by                                       AS user_email,
                compute.warehouse_id                              AS warehouse_id,
                statement_type,
                SUBSTRING(statement_text, 1, 200)                 AS query_preview,
                CASE WHEN error_message IS NULL THEN 'FINISHED' ELSE 'FAILED' END AS status,
                ROUND(total_duration_ms / 1000.0, 2)             AS duration_sec,
                ROUND(read_bytes / (1024.0*1024*1024), 3)        AS gb_scanned,
                ROUND(read_rows / 1000000.0, 2)                  AS m_rows_read,
                CAST(start_time AS STRING)                        AS started_at
            FROM system.query.history
            WHERE start_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
              AND statement_type != 'OTHER'
              AND read_bytes > 0
            ORDER BY read_bytes DESC
            LIMIT 50
        """)

    def _fetch_warehouse_query_stats(self) -> list[dict]:
        """Query volume per warehouse with 30/60/90 breakdowns."""
        return self._execute("""
            SELECT
                compute.warehouse_id                              AS warehouse_id,
                COUNT(*)                                          AS query_count,
                SUM(CASE WHEN start_time >= timestamp(date_add(current_date(), -30)) THEN 1 ELSE 0 END) AS queries_30d,
                SUM(CASE WHEN start_time >= timestamp(date_add(current_date(), -60)) THEN 1 ELSE 0 END) AS queries_60d,
                COUNT(DISTINCT executed_by)                       AS unique_users,
                ROUND(SUM(total_duration_ms) / 1000.0, 1)        AS total_duration_sec,
                ROUND(SUM(read_bytes) / (1024.0*1024*1024), 2)   AS total_gb_scanned,
                ROUND(SUM(CASE WHEN start_time >= timestamp(date_add(current_date(), -30))
                    THEN read_bytes ELSE 0 END) / (1024.0*1024*1024), 2) AS gb_30d,
                ROUND(SUM(CASE WHEN start_time >= timestamp(date_add(current_date(), -60))
                    THEN read_bytes ELSE 0 END) / (1024.0*1024*1024), 2) AS gb_60d,
                ROUND(AVG(total_duration_ms) / 1000.0, 2)        AS avg_duration_sec,
                SUM(CASE WHEN error_message IS NOT NULL THEN 1 ELSE 0 END) AS failed_count
            FROM system.query.history
            WHERE start_time >= timestamp(date_add(current_date(), -90))
              AND compute.warehouse_id IS NOT NULL
              AND statement_type != 'OTHER'
            GROUP BY 1
            ORDER BY total_gb_scanned DESC
        """)

    def _fetch_warehouse_billing(self) -> dict[str, dict]:
        """Warehouse billing cost with 30/60/90 breakdowns from billing.usage."""
        rows = self._execute("""
            SELECT
                usage_metadata.warehouse_id                       AS warehouse_id,
                ROUND(SUM(CASE WHEN usage_start_time >= timestamp(date_add(current_date(), -30))
                    THEN usage_quantity ELSE 0 END), 2)           AS dbus_30d,
                ROUND(SUM(CASE WHEN usage_start_time >= timestamp(date_add(current_date(), -30))
                    THEN usage_quantity * COALESCE(p.pricing.default, 0) ELSE 0 END), 2) AS cost_30d,
                ROUND(SUM(CASE WHEN usage_start_time >= timestamp(date_add(current_date(), -60))
                    THEN usage_quantity ELSE 0 END), 2)           AS dbus_60d,
                ROUND(SUM(CASE WHEN usage_start_time >= timestamp(date_add(current_date(), -60))
                    THEN usage_quantity * COALESCE(p.pricing.default, 0) ELSE 0 END), 2) AS cost_60d,
                ROUND(SUM(usage_quantity), 2)                     AS dbus_90d,
                ROUND(SUM(usage_quantity * COALESCE(p.pricing.default, 0)), 2) AS cost_90d
            FROM system.billing.usage u
            LEFT JOIN system.billing.list_prices p
                ON u.sku_name          = p.sku_name
               AND u.cloud             = p.cloud
               AND u.usage_start_time >= p.price_start_time
               AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
            WHERE usage_start_time >= timestamp(date_add(current_date(), -90))
              AND usage_metadata.warehouse_id IS NOT NULL
            GROUP BY 1
        """)
        result = {}
        for r in rows:
            wid = r.get("warehouse_id", "")
            if wid:
                result[wid] = {k: float(r.get(k) or 0) for k in
                    ("dbus_30d","cost_30d","dbus_60d","cost_60d","dbus_90d","cost_90d")}
        return result

    def _fetch_warehouse_workspace_map(self) -> dict[str, str]:
        """Map warehouse_id → workspace_id from system.compute.warehouses."""
        rows = self._execute("""
            WITH latest AS (
                SELECT warehouse_id, workspace_id,
                    ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY change_time DESC) AS rn
                FROM system.compute.warehouses
            )
            SELECT warehouse_id, workspace_id FROM latest WHERE rn = 1
        """)
        return {r["warehouse_id"]: r["workspace_id"] for r in rows if r.get("warehouse_id")}

    def _fetch_user_warehouse_duration(self) -> list[dict]:
        """Per-user per-warehouse interactive query duration with 30/60/90d breakdowns.
        Only counts queries the user directly ran (executed_by = executed_as).
        Duration-based because warehouse billing is DBU-hours (time-based)."""
        return self._execute("""
            SELECT
                executed_by                    AS user_email,
                compute.warehouse_id           AS warehouse_id,
                SUM(total_duration_ms)         AS user_dur_ms,
                SUM(CASE WHEN start_time >= timestamp(date_add(current_date(), -30))
                    THEN total_duration_ms ELSE 0 END) AS user_dur_ms_30d,
                SUM(CASE WHEN start_time >= timestamp(date_add(current_date(), -60))
                    THEN total_duration_ms ELSE 0 END) AS user_dur_ms_60d
            FROM system.query.history
            WHERE start_time >= timestamp(date_add(current_date(), -90))
              AND executed_by IS NOT NULL
              AND compute.warehouse_id IS NOT NULL
              AND statement_type != 'OTHER'
              AND executed_by = executed_as
            GROUP BY 1, 2
        """)

    def _fetch_warehouse_total_duration(self) -> dict[str, dict]:
        """Total query duration per warehouse from ALL queries (interactive + automated).
        Used as denominator for cost attribution."""
        rows = self._execute("""
            SELECT
                compute.warehouse_id           AS warehouse_id,
                SUM(total_duration_ms)         AS total_dur_ms,
                SUM(CASE WHEN start_time >= timestamp(date_add(current_date(), -30))
                    THEN total_duration_ms ELSE 0 END) AS total_dur_ms_30d,
                SUM(CASE WHEN start_time >= timestamp(date_add(current_date(), -60))
                    THEN total_duration_ms ELSE 0 END) AS total_dur_ms_60d
            FROM system.query.history
            WHERE start_time >= timestamp(date_add(current_date(), -90))
              AND compute.warehouse_id IS NOT NULL
              AND statement_type != 'OTHER'
            GROUP BY 1
        """)
        return {
            r["warehouse_id"]: {
                "total_dur_ms":     int(r.get("total_dur_ms") or 0),
                "total_dur_ms_30d": int(r.get("total_dur_ms_30d") or 0),
                "total_dur_ms_60d": int(r.get("total_dur_ms_60d") or 0),
            }
            for r in rows if r.get("warehouse_id")
        }

    def _fetch_slow_queries(self) -> list[dict]:
        """Queries exceeding 60 seconds execution time."""
        return self._execute(f"""
            SELECT
                statement_id,
                executed_by                                       AS user_email,
                compute.warehouse_id                              AS warehouse_id,
                statement_type,
                SUBSTRING(statement_text, 1, 200)                 AS query_preview,
                CASE WHEN error_message IS NULL THEN 'FINISHED' ELSE 'FAILED' END AS status,
                ROUND(total_duration_ms / 1000.0, 2)             AS duration_sec,
                ROUND(read_bytes / (1024.0*1024*1024), 3)        AS gb_scanned,
                CAST(start_time AS STRING)                        AS started_at
            FROM system.query.history
            WHERE start_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
              AND total_duration_ms > 60000
              AND statement_type != 'OTHER'
            ORDER BY total_duration_ms DESC
            LIMIT 50
        """)

    def _fetch_warehouse_events(self) -> list[dict]:
        """Warehouse start/stop/scaling events from system.compute.warehouse_events (7d)."""
        return self._safe_execute("""
            SELECT
                warehouse_id,
                event_type,
                cluster_count,
                CAST(event_time AS STRING) AS event_time
            FROM system.compute.warehouse_events
            WHERE event_time >= timestamp(date_add(current_date(), -7))
            ORDER BY event_time DESC
            LIMIT 500
        """)

    # ── analysis ──────────────────────────────────────────────────────────────

    def _analyse(
        self,
        user_stats: list[dict],
        daily_trend: list[dict],
        top_queries: list[dict],
        wh_stats: list[dict],
        slow_queries: list[dict],
        wh_billing: dict[str, dict] | None = None,
        wh_ws_map: dict[str, str] | None = None,
        user_wh_dur: list[dict] | None = None,
        wh_total_dur_map: dict[str, dict] | None = None,
        wh_events: list[dict] | None = None,
    ) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)
        bill = wh_billing or {}
        ws_map = wh_ws_map or {}

        total_queries  = sum(int(r.get("query_count") or 0) for r in user_stats)
        total_gb       = sum(float(r.get("total_gb_scanned") or 0) for r in user_stats)
        total_failed   = sum(int(r.get("failed_count") or 0) for r in user_stats)
        unique_users   = len(user_stats)

        def _int(v):
            try: return int(v) if v else 0
            except: return 0

        def _float(v):
            try: return float(v) if v else 0.0
            except: return 0.0

        # ── Build per-user estimated cost from warehouse query duration share ─
        # Duration-based because warehouse billing is DBU-hours (time-based).
        # Numerator: user's interactive query duration (executed_by = executed_as)
        # Denominator: ALL query duration on that warehouse (interactive + automated)
        # This excludes idle warehouse time and automated workload costs.
        wh_dur_all = wh_total_dur_map or {}
        user_cost_30d: dict[str, float] = {}
        user_cost_60d: dict[str, float] = {}
        user_cost_map: dict[str, float] = {}  # 90d
        # Track total warehouse bytes for per-query cost estimation
        wh_total_bytes: dict[str, int] = {}
        if user_wh_dur:
            for r in user_wh_dur:
                email = r.get("user_email", "")
                wid   = r.get("warehouse_id", "")
                wb    = bill.get(wid, {})
                totals = wh_dur_all.get(wid, {})
                # 90d
                ud90 = int(r.get("user_dur_ms") or 0)
                t90  = totals.get("total_dur_ms", 0)
                user_cost_map[email] = user_cost_map.get(email, 0) + (ud90 / t90 * wb.get("cost_90d", 0) if t90 > 0 else 0)
                # 60d
                ud60 = int(r.get("user_dur_ms_60d") or 0)
                t60  = totals.get("total_dur_ms_60d", 0)
                user_cost_60d[email] = user_cost_60d.get(email, 0) + (ud60 / t60 * wb.get("cost_60d", 0) if t60 > 0 else 0)
                # 30d
                ud30 = int(r.get("user_dur_ms_30d") or 0)
                t30  = totals.get("total_dur_ms_30d", 0)
                user_cost_30d[email] = user_cost_30d.get(email, 0) + (ud30 / t30 * wb.get("cost_30d", 0) if t30 > 0 else 0)

        # ── Per-query estimated cost helper (duration-based) ──────────────
        def _query_est_cost(wh_id: str, query_dur_sec: float) -> float:
            totals = wh_dur_all.get(wh_id, {})
            total_ms = totals.get("total_dur_ms", 0)
            wh_cost = bill.get(wh_id, {}).get("cost_90d", 0)
            query_ms = query_dur_sec * 1000
            return (query_ms / total_ms * wh_cost) if total_ms > 0 and wh_cost > 0 else 0

        # ── Warehouse Azure info helper ───────────────────────────────────
        def _wh_azure(wh_id: str) -> dict:
            ws_id = ws_map.get(wh_id, "")
            return self._ws_azure.get(ws_id, {})

        return {
            "generated_at":    now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lookback_days":   self.LOOKBACK_DAYS,
            "summary": {
                "total_queries":  total_queries,
                "total_gb_scanned": round(total_gb, 2),
                "unique_users":   unique_users,
                "total_failed":   total_failed,
                "slow_queries":   len(slow_queries),
            },
            "daily_trend": [
                {
                    "date":            r.get("date", ""),
                    "query_count":     _int(r.get("query_count")),
                    "unique_users":    _int(r.get("unique_users")),
                    "gb_scanned":      _float(r.get("gb_scanned")),
                    "avg_duration_sec": _float(r.get("avg_duration_sec")),
                }
                for r in daily_trend
            ],
            "user_stats": [
                {
                    "user_email":       r.get("user_email", ""),
                    "query_count":      _int(r.get("query_count")),
                    "queries_30d":      _int(r.get("queries_30d")),
                    "queries_60d":      _int(r.get("queries_60d")),
                    "queries_90d":      _int(r.get("query_count")),
                    "total_duration_sec": _float(r.get("total_duration_sec")),
                    "avg_duration_sec":  _float(r.get("avg_duration_sec")),
                    "total_gb_scanned":  _float(r.get("total_gb_scanned")),
                    "gb_30d":            _float(r.get("gb_30d")),
                    "gb_60d":            _float(r.get("gb_60d")),
                    "gb_90d":            _float(r.get("total_gb_scanned")),
                    "cost_30d":          round(user_cost_30d.get(r.get("user_email", ""), 0), 2),
                    "cost_60d":          round(user_cost_60d.get(r.get("user_email", ""), 0), 2),
                    "cost_90d":          round(user_cost_map.get(r.get("user_email", ""), 0), 2),
                    "warehouses_used":   _int(r.get("warehouses_used")),
                    "success_count":     _int(r.get("success_count")),
                    "failed_count":      _int(r.get("failed_count")),
                }
                for r in user_stats
            ],
            "top_expensive_queries": [
                {
                    "statement_id":   r.get("statement_id", ""),
                    "user_email":     r.get("user_email", ""),
                    "warehouse_id":   r.get("warehouse_id", ""),
                    **_wh_azure(r.get("warehouse_id", "")),
                    "statement_type": r.get("statement_type", ""),
                    "query_preview":  r.get("query_preview", ""),
                    "status":         r.get("status", ""),
                    "duration_sec":   _float(r.get("duration_sec")),
                    "gb_scanned":     _float(r.get("gb_scanned")),
                    "m_rows_read":    _float(r.get("m_rows_read")),
                    "estimated_cost": round(_query_est_cost(
                        r.get("warehouse_id", ""),
                        _float(r.get("duration_sec")),
                    ), 2),
                    "started_at":     r.get("started_at", ""),
                }
                for r in top_queries
            ],
            "warehouse_stats": [
                {
                    "warehouse_id":      r.get("warehouse_id", ""),
                    **_wh_azure(r.get("warehouse_id", "")),
                    "query_count":       _int(r.get("query_count")),
                    "queries_30d":       _int(r.get("queries_30d")),
                    "queries_60d":       _int(r.get("queries_60d")),
                    "queries_90d":       _int(r.get("query_count")),
                    "unique_users":      _int(r.get("unique_users")),
                    "total_duration_sec": _float(r.get("total_duration_sec")),
                    "total_gb_scanned":  _float(r.get("total_gb_scanned")),
                    "gb_30d":            _float(r.get("gb_30d")),
                    "gb_60d":            _float(r.get("gb_60d")),
                    "gb_90d":            _float(r.get("total_gb_scanned")),
                    "avg_duration_sec":  _float(r.get("avg_duration_sec")),
                    **(wh_billing or {}).get(r.get("warehouse_id", ""), {
                        "dbus_30d": 0, "cost_30d": 0, "dbus_60d": 0,
                        "cost_60d": 0, "dbus_90d": 0, "cost_90d": 0,
                    }),
                    "failed_count":      _int(r.get("failed_count")),
                }
                for r in wh_stats
            ],
            "slow_queries": [
                {
                    "statement_id":   r.get("statement_id", ""),
                    "user_email":     r.get("user_email", ""),
                    "warehouse_id":   r.get("warehouse_id", ""),
                    **_wh_azure(r.get("warehouse_id", "")),
                    "statement_type": r.get("statement_type", ""),
                    "query_preview":  r.get("query_preview", ""),
                    "status":         r.get("status", ""),
                    "duration_sec":   _float(r.get("duration_sec")),
                    "gb_scanned":     _float(r.get("gb_scanned")),
                    "estimated_cost": round(_query_est_cost(
                        r.get("warehouse_id", ""),
                        _float(r.get("duration_sec")),
                    ), 2),
                    "started_at":     r.get("started_at", ""),
                }
                for r in slow_queries
            ],
            "warehouse_events": [
                {
                    "warehouse_id":  r.get("warehouse_id", ""),
                    **_wh_azure(r.get("warehouse_id", "")),
                    "event_type":    r.get("event_type", ""),
                    "cluster_count": _int(r.get("cluster_count")),
                    "event_time":    r.get("event_time", ""),
                }
                for r in (wh_events or [])
            ],
        }

    # ── main entry point ──────────────────────────────────────────────────────

    def get_query_insights(self) -> dict:
        """Fetch query history and compute attribution analytics.

        Results are cached for CACHE_TTL_MIN minutes.
        """
        now = datetime.datetime.now(datetime.timezone.utc)

        if self._cache and self._cached_at:
            age = (now - self._cached_at).total_seconds() / 60
            if age < self.CACHE_TTL_MIN:
                return {**self._cache, "cache_age_minutes": round(age, 1)}

        user_stats       = self._fetch_user_query_stats()
        daily_trend      = self._fetch_daily_query_trend()
        top_queries      = self._fetch_top_expensive_queries()
        wh_stats         = self._fetch_warehouse_query_stats()
        slow_queries     = self._fetch_slow_queries()
        wh_billing       = self._fetch_warehouse_billing()
        wh_ws_map        = self._fetch_warehouse_workspace_map()
        user_wh_dur      = self._fetch_user_warehouse_duration()
        wh_total_dur     = self._fetch_warehouse_total_duration()
        wh_events        = self._fetch_warehouse_events()

        result = self._analyse(
            user_stats, daily_trend, top_queries, wh_stats, slow_queries,
            wh_billing, wh_ws_map, user_wh_dur, wh_total_dur, wh_events,
        )
        result["cache_age_minutes"] = 0

        self._cache     = result
        self._cached_at = now
        return result
