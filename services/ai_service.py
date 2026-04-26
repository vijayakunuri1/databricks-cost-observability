"""
AI/LLM Usage & Cost analytics from system.ai_gateway and system.ai tables.

Provides executive-level visibility into:
  - Token consumption and cost by model, endpoint, user
  - AI Gateway request volume and latency
  - Daily usage trends
  - Top consumers and models
"""

from __future__ import annotations

import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from databricks.sdk import WorkspaceClient

from core.config import get_settings
from core.sql_executor import execute_sync, safe_execute_sync


class AIService:
    LOOKBACK_DAYS = 30
    CACHE_TTL_MIN = 30

    def __init__(self, client: WorkspaceClient) -> None:
        self._client = client
        self._wh_id  = get_settings().databricks_warehouse_id
        self._cache: Optional[dict]                  = None
        self._cached_at: Optional[datetime.datetime] = None

    def _execute(self, sql: str) -> list[dict]:
        return execute_sync(self._client, self._wh_id, sql, label="AI")

    def _safe_execute(self, sql: str) -> list[dict]:
        """Execute a query, returning [] if the table doesn't exist."""
        return safe_execute_sync(self._client, self._wh_id, sql, label="AI")

    # ── data queries ──────────────────────────────────────────────────────────

    def _fetch_gateway_usage(self) -> list[dict]:
        """AI Gateway request/token usage by route and model."""
        return self._safe_execute(f"""
            SELECT
                route_name,
                route_name                                        AS model,
                CAST(DATE(event_time) AS STRING)                  AS date,
                COUNT(*)                                          AS request_count,
                SUM(total_token_count)                            AS total_tokens,
                SUM(input_token_count)                            AS input_tokens,
                SUM(output_token_count)                           AS output_tokens,
                ROUND(AVG(execution_duration_ms), 1)              AS avg_latency_ms,
                ROUND(SUM(total_token_count) / 1000000.0, 3)     AS m_tokens
            FROM system.ai_gateway.usage
            WHERE event_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
            GROUP BY route_name, DATE(event_time)
            ORDER BY date, request_count DESC
        """)

    def _fetch_gateway_by_user(self) -> list[dict]:
        """AI Gateway usage aggregated by user."""
        return self._safe_execute(f"""
            SELECT
                requester                                         AS user_email,
                COUNT(*)                                          AS request_count,
                SUM(total_token_count)                            AS total_tokens,
                SUM(input_token_count)                            AS input_tokens,
                SUM(output_token_count)                           AS output_tokens,
                ROUND(AVG(execution_duration_ms), 1)              AS avg_latency_ms,
                COUNT(DISTINCT route_name)                        AS routes_used,
                1                                                 AS models_used
            FROM system.ai_gateway.usage
            WHERE event_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
              AND requester IS NOT NULL
            GROUP BY requester
            ORDER BY total_tokens DESC
            LIMIT 100
        """)

    def _fetch_model_serving_billing(self) -> list[dict]:
        """Model serving DBU/cost from billing.usage for Foundation Model / External Model SKUs."""
        return self._safe_execute(f"""
            SELECT
                u.sku_name,
                u.workspace_id,
                CAST(DATE(u.usage_start_time) AS STRING)          AS date,
                ROUND(SUM(u.usage_quantity), 4)                   AS total_dbus,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 4) AS total_cost
            FROM system.billing.usage u
            LEFT JOIN system.billing.list_prices p
                ON u.sku_name          = p.sku_name
               AND u.cloud             = p.cloud
               AND u.usage_start_time >= p.price_start_time
               AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
            WHERE u.usage_start_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
              AND (
                u.sku_name LIKE '%FOUNDATION_MODEL%'
                OR u.sku_name LIKE '%EXTERNAL_MODEL%'
                OR u.sku_name LIKE '%MODEL_SERVING%'
                OR u.sku_name LIKE '%FEATURE_SERVING%'
                OR u.sku_name LIKE '%VECTOR_SEARCH%'
              )
            GROUP BY u.sku_name, u.workspace_id, DATE(u.usage_start_time)
            ORDER BY date, total_cost DESC
        """)

    def _fetch_latency_percentiles(self) -> list[dict]:
        """p50/p90/p99 latency per model route over the lookback window."""
        return self._safe_execute(f"""
            SELECT
                route_name,
                COUNT(*)                                             AS request_count,
                ROUND(PERCENTILE(execution_duration_ms, 0.50), 1)   AS p50_ms,
                ROUND(PERCENTILE(execution_duration_ms, 0.90), 1)   AS p90_ms,
                ROUND(PERCENTILE(execution_duration_ms, 0.99), 1)   AS p99_ms,
                ROUND(AVG(execution_duration_ms), 1)                 AS avg_ms,
                ROUND(MAX(execution_duration_ms), 1)                 AS max_ms,
                ROUND(100.0 * SUM(CASE WHEN output_token_count IS NULL OR output_token_count = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS error_rate_pct
            FROM system.ai_gateway.usage
            WHERE event_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
            GROUP BY route_name
            ORDER BY request_count DESC
        """)

    def _fetch_latency_daily(self) -> list[dict]:
        """Daily p50/p90 latency trend across all routes."""
        return self._safe_execute(f"""
            SELECT
                CAST(DATE(event_time) AS STRING)                     AS date,
                ROUND(PERCENTILE(execution_duration_ms, 0.50), 1)   AS p50_ms,
                ROUND(PERCENTILE(execution_duration_ms, 0.90), 1)   AS p90_ms,
                COUNT(*)                                             AS request_count
            FROM system.ai_gateway.usage
            WHERE event_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
            GROUP BY DATE(event_time)
            ORDER BY date
        """)

    def _fetch_serving_billing_by_user(self) -> list[dict]:
        """AI/ML serving cost broken down by user from billing identity metadata."""
        return self._safe_execute(f"""
            SELECT
                COALESCE(
                    u.identity_metadata.run_as,
                    u.identity_metadata.created_by,
                    'unknown'
                )                                                 AS user_email,
                u.sku_name,
                ROUND(SUM(u.usage_quantity), 4)                   AS total_dbus,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 4) AS total_cost,
                COUNT(DISTINCT DATE(u.usage_start_time))          AS active_days,
                COUNT(DISTINCT u.workspace_id)                    AS workspaces
            FROM system.billing.usage u
            LEFT JOIN system.billing.list_prices p
                ON u.sku_name          = p.sku_name
               AND u.cloud             = p.cloud
               AND u.usage_start_time >= p.price_start_time
               AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
            WHERE u.usage_start_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
              AND (
                u.sku_name LIKE '%FOUNDATION_MODEL%'
                OR u.sku_name LIKE '%EXTERNAL_MODEL%'
                OR u.sku_name LIKE '%MODEL_SERVING%'
                OR u.sku_name LIKE '%FEATURE_SERVING%'
                OR u.sku_name LIKE '%VECTOR_SEARCH%'
              )
            GROUP BY user_email, u.sku_name
            ORDER BY total_cost DESC
            LIMIT 200
        """)

    # ── analysis ──────────────────────────────────────────────────────────────

    def _analyse(
        self,
        gateway_usage: list[dict],
        gateway_users: list[dict],
        serving_billing: list[dict],
        billing_users: list[dict],
        latency_percentiles: list[dict],
        latency_daily: list[dict],
    ) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)

        def _int(v):
            try: return int(v) if v else 0
            except: return 0

        def _float(v):
            try: return float(v) if v else 0.0
            except: return 0.0

        # Aggregate gateway daily trend
        daily_map: dict[str, dict] = {}
        for r in gateway_usage:
            d = r.get("date", "")
            if d not in daily_map:
                daily_map[d] = {"date": d, "requests": 0, "total_tokens": 0, "m_tokens": 0}
            daily_map[d]["requests"]     += _int(r.get("request_count"))
            daily_map[d]["total_tokens"] += _int(r.get("total_tokens"))
            daily_map[d]["m_tokens"]     += _float(r.get("m_tokens"))

        daily_trend = sorted(daily_map.values(), key=lambda x: x["date"])

        # Model breakdown
        model_map: dict[str, dict] = {}
        for r in gateway_usage:
            m = r.get("model") or r.get("route_name") or "unknown"
            if m not in model_map:
                model_map[m] = {"model": m, "requests": 0, "total_tokens": 0, "input_tokens": 0, "output_tokens": 0, "avg_latency_ms": 0, "_latency_sum": 0, "_latency_n": 0}
            model_map[m]["requests"]      += _int(r.get("request_count"))
            model_map[m]["total_tokens"]  += _int(r.get("total_tokens"))
            model_map[m]["input_tokens"]  += _int(r.get("input_tokens"))
            model_map[m]["output_tokens"] += _int(r.get("output_tokens"))
            model_map[m]["_latency_sum"]  += _float(r.get("avg_latency_ms")) * _int(r.get("request_count"))
            model_map[m]["_latency_n"]    += _int(r.get("request_count"))

        models = []
        for m in model_map.values():
            m["avg_latency_ms"] = round(m["_latency_sum"] / m["_latency_n"], 1) if m["_latency_n"] else 0
            del m["_latency_sum"]
            del m["_latency_n"]
            models.append(m)
        models.sort(key=lambda x: -x["total_tokens"])

        # Serving billing daily trend
        billing_daily: dict[str, dict] = {}
        for r in serving_billing:
            d = r.get("date", "")
            if d not in billing_daily:
                billing_daily[d] = {"date": d, "dbus": 0, "cost": 0}
            billing_daily[d]["dbus"] += _float(r.get("total_dbus"))
            billing_daily[d]["cost"] += _float(r.get("total_cost"))

        billing_trend = sorted(billing_daily.values(), key=lambda x: x["date"])

        # Serving SKU breakdown
        sku_map: dict[str, dict] = {}
        for r in serving_billing:
            s = r.get("sku_name", "unknown")
            if s not in sku_map:
                sku_map[s] = {"sku_name": s, "total_dbus": 0, "total_cost": 0}
            sku_map[s]["total_dbus"] += _float(r.get("total_dbus"))
            sku_map[s]["total_cost"] += _float(r.get("total_cost"))

        skus = sorted(sku_map.values(), key=lambda x: -x["total_cost"])

        # Summary
        total_requests = sum(d["requests"] for d in daily_trend)
        total_tokens   = sum(d["total_tokens"] for d in daily_trend)
        total_serving_cost = sum(s["total_cost"] for s in skus)

        has_gateway = len(gateway_usage) > 0
        has_serving = len(serving_billing) > 0

        # Latency percentiles per model (clean up types)
        latency_by_model = [
            {
                "route_name":     r.get("route_name", ""),
                "request_count":  int(r.get("request_count") or 0),
                "p50_ms":         _float(r.get("p50_ms")),
                "p90_ms":         _float(r.get("p90_ms")),
                "p99_ms":         _float(r.get("p99_ms")),
                "avg_ms":         _float(r.get("avg_ms")),
                "max_ms":         _float(r.get("max_ms")),
                "error_rate_pct": _float(r.get("error_rate_pct")),
            }
            for r in latency_percentiles
        ]

        latency_trend = [
            {
                "date":          r.get("date", ""),
                "p50_ms":        _float(r.get("p50_ms")),
                "p90_ms":        _float(r.get("p90_ms")),
                "request_count": int(r.get("request_count") or 0),
            }
            for r in latency_daily
        ]

        return {
            "generated_at":     now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lookback_days":    self.LOOKBACK_DAYS,
            "has_gateway_data": has_gateway,
            "has_serving_data": has_serving,
            "summary": {
                "total_requests":     total_requests,
                "total_tokens":       total_tokens,
                "total_m_tokens":     round(total_tokens / 1_000_000, 2) if total_tokens else 0,
                "total_serving_cost": round(total_serving_cost, 2),
                "models_in_use":      len(models),
                "gateway_users":      len(gateway_users),
            },
            "daily_trend":      daily_trend,
            "models":           models,
            "billing_trend":    billing_trend,
            "serving_skus":     skus,
            "latency_by_model": latency_by_model,
            "latency_trend":    latency_trend,
            "gateway_users": [
                {
                    "user_email":     r.get("user_email", ""),
                    "request_count":  _int(r.get("request_count")),
                    "total_tokens":   _int(r.get("total_tokens")),
                    "input_tokens":   _int(r.get("input_tokens")),
                    "output_tokens":  _int(r.get("output_tokens")),
                    "avg_latency_ms": _float(r.get("avg_latency_ms")),
                    "routes_used":    _int(r.get("routes_used")),
                    "models_used":    _int(r.get("models_used")),
                }
                for r in gateway_users
            ],
            "billing_users": [
                {
                    "user_email":   r.get("user_email", ""),
                    "sku_name":     r.get("sku_name", ""),
                    "total_dbus":   _float(r.get("total_dbus")),
                    "total_cost":   _float(r.get("total_cost")),
                    "active_days":  _int(r.get("active_days")),
                    "workspaces":   _int(r.get("workspaces")),
                }
                for r in billing_users
            ],
        }

    # ── main entry point ──────────────────────────────────────────────────────

    def get_ai_insights(self) -> dict:
        """Fetch AI/LLM usage data and compute analytics.

        Results are cached for CACHE_TTL_MIN minutes.
        """
        now = datetime.datetime.now(datetime.timezone.utc)

        if self._cache and self._cached_at:
            age = (now - self._cached_at).total_seconds() / 60
            if age < self.CACHE_TTL_MIN:
                return {**self._cache, "cache_age_minutes": round(age, 1)}

        with ThreadPoolExecutor(max_workers=6) as pool:
            f_gw_use   = pool.submit(self._fetch_gateway_usage)
            f_gw_usr   = pool.submit(self._fetch_gateway_by_user)
            f_sv_bill  = pool.submit(self._fetch_model_serving_billing)
            f_bill_usr = pool.submit(self._fetch_serving_billing_by_user)
            f_lat_mod  = pool.submit(self._fetch_latency_percentiles)
            f_lat_day  = pool.submit(self._fetch_latency_daily)

        def _safe_result(future, default=None):
            try:
                return future.result()
            except Exception:
                return default if default is not None else []

        gateway_usage       = _safe_result(f_gw_use)
        gateway_users       = _safe_result(f_gw_usr)
        serving_billing     = _safe_result(f_sv_bill)
        billing_users       = _safe_result(f_bill_usr)
        latency_percentiles = _safe_result(f_lat_mod)
        latency_daily       = _safe_result(f_lat_day)

        result = self._analyse(gateway_usage, gateway_users, serving_billing, billing_users,
                               latency_percentiles, latency_daily)
        result["cache_age_minutes"] = 0

        self._cache     = result
        self._cached_at = now
        return result
