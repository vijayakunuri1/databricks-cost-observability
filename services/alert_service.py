"""
Cost spike detection — compares recent daily spend against a 30-day rolling baseline.
Detects: spend spikes, single-day extremes, new SKUs, fast-growing SKUs.
"""

from __future__ import annotations

import datetime
import logging
from typing import Optional

from databricks.sdk import WorkspaceClient

from core.config import get_settings
from core.sql_executor import safe_execute_sync

_log = logging.getLogger(__name__)

DEFAULT_THRESHOLD_PCT = 20.0


class AlertService:
    CACHE_TTL_MIN = 60

    def __init__(self, client: WorkspaceClient) -> None:
        self._client = client
        self._wh_id  = get_settings().databricks_warehouse_id
        self._cache: Optional[dict]                  = None
        self._cached_at: Optional[datetime.datetime] = None

    def _safe(self, sql: str) -> list[dict]:
        return safe_execute_sync(self._client, self._wh_id, sql, label="Alert")

    def _fetch_daily_spend(self) -> list[dict]:
        return self._safe("""
            SELECT
                CAST(DATE(u.usage_start_time) AS STRING)                              AS date,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2)     AS daily_cost,
                ROUND(SUM(u.usage_quantity), 2)                                       AS daily_dbus,
                COUNT(DISTINCT u.workspace_id)                                        AS active_workspaces
            FROM system.billing.usage u
            LEFT JOIN system.billing.list_prices p
                ON  u.sku_name          = p.sku_name
                AND u.cloud             = p.cloud
                AND u.usage_start_time >= p.price_start_time
                AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
            WHERE u.usage_start_time >= timestamp(date_add(current_date(), -37))
            GROUP BY DATE(u.usage_start_time)
            ORDER BY date
        """)

    def _fetch_sku_week_over_week(self) -> list[dict]:
        return self._safe("""
            SELECT
                u.sku_name,
                u.billing_origin_product,
                ROUND(SUM(CASE
                    WHEN u.usage_start_time >= timestamp(date_add(current_date(), -7))
                    THEN u.usage_quantity * COALESCE(p.pricing.default, 0) ELSE 0 END), 2) AS cost_7d,
                ROUND(SUM(CASE
                    WHEN u.usage_start_time >= timestamp(date_add(current_date(), -14))
                     AND u.usage_start_time <  timestamp(date_add(current_date(), -7))
                    THEN u.usage_quantity * COALESCE(p.pricing.default, 0) ELSE 0 END), 2) AS cost_prior_7d
            FROM system.billing.usage u
            LEFT JOIN system.billing.list_prices p
                ON  u.sku_name          = p.sku_name
                AND u.cloud             = p.cloud
                AND u.usage_start_time >= p.price_start_time
                AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
            WHERE u.usage_start_time >= timestamp(date_add(current_date(), -14))
            GROUP BY u.sku_name, u.billing_origin_product
            ORDER BY cost_7d DESC
        """)

    def detect_spikes(self, threshold_pct: float = DEFAULT_THRESHOLD_PCT) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)

        if self._cache and self._cached_at:
            age = (now - self._cached_at).total_seconds() / 60
            if age < self.CACHE_TTL_MIN:
                return {**self._cache, "cache_age_minutes": round(age, 1)}

        daily_rows = self._fetch_daily_spend()
        sku_rows   = self._fetch_sku_week_over_week()

        if not daily_rows:
            empty = {
                "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "has_alerts": False, "alert_count": 0, "alerts": [],
                "threshold_pct": threshold_pct, "summary": {}, "daily_spend": [],
                "cache_age_minutes": 0,
            }
            return empty

        costs = [(r["date"], float(r.get("daily_cost") or 0)) for r in daily_rows]

        # Baseline: everything older than 7 days (stable history)
        baseline = [c for _, c in costs[:-7]] if len(costs) > 7 else [c for _, c in costs]
        baseline_avg = sum(baseline) / len(baseline) if baseline else 0

        recent = costs[-7:] if len(costs) >= 7 else costs
        recent_avg = sum(c for _, c in recent) / len(recent) if recent else 0

        alerts: list[dict] = []

        # ── Trend spike: 7-day avg vs 30-day baseline ─────────────────────────
        if baseline_avg > 0:
            pct = (recent_avg - baseline_avg) / baseline_avg * 100
            if pct > threshold_pct:
                severity = "HIGH" if pct > threshold_pct * 2 else "MEDIUM"
                alerts.append({
                    "type":         "SPEND_SPIKE",
                    "severity":     severity,
                    "title":        f"Spend up {pct:.1f}% above baseline",
                    "detail":       f"7-day avg ${recent_avg:,.2f}/day vs ${baseline_avg:,.2f}/day baseline",
                    "pct_change":   round(pct, 1),
                    "recent_avg":   round(recent_avg, 2),
                    "baseline_avg": round(baseline_avg, 2),
                })

        # ── Single-day extreme: any day >2× baseline ──────────────────────────
        for date, cost in recent:
            if baseline_avg > 0 and cost > baseline_avg * 2:
                alerts.append({
                    "type":         "DAILY_SPIKE",
                    "severity":     "HIGH",
                    "title":        f"Extreme spike on {date}: ${cost:,.2f}",
                    "detail":       f"{cost / baseline_avg:.1f}× the daily baseline avg",
                    "date":         date,
                    "cost":         round(cost, 2),
                    "baseline_avg": round(baseline_avg, 2),
                })

        # ── New or fast-growing SKU ────────────────────────────────────────────
        for r in sku_rows:
            prior  = float(r.get("cost_prior_7d") or 0)
            recent_cost = float(r.get("cost_7d") or 0)
            sku    = r.get("sku_name", "")
            if prior == 0 and recent_cost > 10:
                alerts.append({
                    "type":     "NEW_SKU",
                    "severity": "MEDIUM",
                    "title":    f"New SKU: {sku}",
                    "detail":   f"${recent_cost:,.2f} in last 7 days — not present the prior week",
                    "sku_name": sku,
                    "cost_7d":  round(recent_cost, 2),
                })
            elif prior > 0 and recent_cost > 0:
                sku_pct = (recent_cost - prior) / prior * 100
                if sku_pct > threshold_pct * 1.5:
                    alerts.append({
                        "type":          "SKU_GROWTH",
                        "severity":      "MEDIUM",
                        "title":         f"{sku} up {sku_pct:.0f}% week-over-week",
                        "detail":        f"${recent_cost:,.2f} this week vs ${prior:,.2f} last week",
                        "sku_name":      sku,
                        "cost_7d":       round(recent_cost, 2),
                        "cost_prior_7d": round(prior, 2),
                        "pct_change":    round(sku_pct, 1),
                    })

        total_30d = sum(c for _, c in costs[-30:])
        latest_date, latest_cost = costs[-1] if costs else ("", 0)

        result = {
            "generated_at":  now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "has_alerts":    len(alerts) > 0,
            "alert_count":   len(alerts),
            "alerts":        alerts,
            "threshold_pct": threshold_pct,
            "summary": {
                "total_30d":          round(total_30d, 2),
                "baseline_avg_daily": round(baseline_avg, 2),
                "recent_avg_daily":   round(recent_avg, 2),
                "latest_date":        latest_date,
                "latest_cost":        round(latest_cost, 2),
                "pct_vs_baseline":    round(
                    (recent_avg - baseline_avg) / baseline_avg * 100, 1
                ) if baseline_avg > 0 else 0,
            },
            "daily_spend": [{"date": d, "cost": c} for d, c in costs],
            "cache_age_minutes": 0,
        }
        self._cache     = result
        self._cached_at = now
        return result
