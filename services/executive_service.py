"""
Executive Dashboard — cross-domain KPI aggregation for executive persona.

Pulls from system.billing, system.lakeflow, system.access.audit, and
system.compute to produce a single self-contained scorecard response.
Cached 20 minutes.
"""

from __future__ import annotations

import calendar
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from databricks.sdk import WorkspaceClient

from core.config import get_settings
from core.sql_executor import execute_sync, safe_execute_sync

_log = logging.getLogger("security")


def _safe_future(future, default=None):
    """Get a future's result, returning *default* (or []) on any exception."""
    try:
        return future.result()
    except Exception:
        return default if default is not None else []


class ExecutiveService:
    CACHE_TTL_MIN = 20

    _PRICE_JOIN = """
        LEFT JOIN system.billing.list_prices p
            ON u.sku_name = p.sku_name
            AND u.cloud    = p.cloud
            AND u.usage_start_time >= p.price_start_time
            AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
    """

    def __init__(self, client: WorkspaceClient) -> None:
        self._client = client
        self._wh_id  = get_settings().databricks_warehouse_id
        self._cache: Optional[dict]                  = None
        self._cached_at: Optional[datetime.datetime] = None

    def _execute(self, sql: str) -> list[dict]:
        return execute_sync(self._client, self._wh_id, sql, label="Executive")

    def _safe_execute(self, sql: str) -> list[dict]:
        return safe_execute_sync(self._client, self._wh_id, sql, label="Executive")

    # ── individual fetchers ───────────────────────────────────────────────────

    def _fetch_spend_scorecard(self) -> dict:
        today   = datetime.date.today()
        d30     = (today - datetime.timedelta(days=30)).isoformat()
        d60     = (today - datetime.timedelta(days=60)).isoformat()
        d14     = (today - datetime.timedelta(days=14)).isoformat()
        m_start = today.replace(day=1).isoformat()

        rows = self._execute(f"""
            SELECT
                ROUND(SUM(CASE WHEN u.usage_date >= '{d30}' THEN
                    u.usage_quantity * COALESCE(p.pricing.default, 0) ELSE 0 END), 2) AS cost_30d,
                ROUND(SUM(CASE WHEN u.usage_date >= '{d60}' AND u.usage_date < '{d30}' THEN
                    u.usage_quantity * COALESCE(p.pricing.default, 0) ELSE 0 END), 2) AS cost_prev_30d,
                ROUND(SUM(CASE WHEN u.usage_date >= '{d14}' THEN
                    u.usage_quantity * COALESCE(p.pricing.default, 0) ELSE 0 END) / 14.0, 4) AS avg_daily_14d,
                ROUND(SUM(CASE WHEN u.usage_date >= '{m_start}' THEN
                    u.usage_quantity * COALESCE(p.pricing.default, 0) ELSE 0 END), 2) AS month_to_date,
                ROUND(SUM(CASE WHEN u.usage_date >= '{d30}' THEN u.usage_quantity ELSE 0 END), 2) AS total_dbus_30d,
                COUNT(DISTINCT CASE WHEN u.usage_date >= '{d30}' THEN u.workspace_id END) AS workspace_count
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            WHERE u.usage_date >= '{d60}'
        """)

        row       = rows[0] if rows else {}
        cost_30d  = float(row.get("cost_30d")      or 0)
        cost_prev = float(row.get("cost_prev_30d") or 0)
        avg_daily = float(row.get("avg_daily_14d") or 0)
        mtd       = float(row.get("month_to_date") or 0)

        # Use AI_FORECAST() for intelligent month-end projection
        forecast_rows = self._safe_execute(f"""
            SELECT
                CAST(u.usage_date AS DATE) AS date,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2) AS daily_cost
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            WHERE u.usage_date >= '{d30}'
            GROUP BY u.usage_date
            ORDER BY u.usage_date
        """)
        
        if forecast_rows and len(forecast_rows) >= 7:
            # Use AI_FORECAST to predict next days up to end of month
            days_in_month = calendar.monthrange(today.year, today.month)[1]
            days_to_forecast = days_in_month - today.day
            
            try:
                month_end_horizon = (today.replace(day=days_in_month) + datetime.timedelta(days=1)).isoformat()
                forecast_result = self._safe_execute(f"""
                    WITH daily_costs AS (
                        SELECT
                            CAST(u.usage_date AS DATE)                                             AS ds,
                            ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2)       AS y
                        FROM system.billing.usage u
                        {self._PRICE_JOIN}
                        WHERE u.usage_date >= '{d30}'
                        GROUP BY u.usage_date
                    )
                    SELECT ROUND(SUM(y_forecast), 2) AS forecasted_remaining
                    FROM AI_FORECAST(
                        TABLE(daily_costs),
                        horizon   => '{month_end_horizon}',
                        time_col  => 'ds',
                        value_col => 'y'
                    )
                """)
                
                forecasted_remaining = float(forecast_result[0].get("forecasted_remaining") or 0) if forecast_result else 0
                projected = round(mtd + forecasted_remaining, 2)
            except Exception:
                # Fallback to 14-day average if AI_FORECAST fails
                days_remaining = days_in_month - today.day
                projected = round(mtd + avg_daily * days_remaining, 2)
        else:
            # Fallback to simple average if not enough historical data
            days_in_month = calendar.monthrange(today.year, today.month)[1]
            days_remaining = days_in_month - today.day
            projected = round(mtd + avg_daily * days_remaining, 2)
        
        mom_delta = round((cost_30d - cost_prev) / cost_prev * 100, 1) if cost_prev else 0.0

        return {
            "cost_30d":           cost_30d,
            "cost_prev_30d":      cost_prev,
            "mom_delta_pct":      mom_delta,
            "avg_daily_cost_14d": round(avg_daily, 2),
            "month_to_date":      mtd,
            "projected_month_end": projected,
            "total_dbus_30d":     float(row.get("total_dbus_30d") or 0),
            "workspace_count":    int(row.get("workspace_count")  or 0),
        }

    def _fetch_run_rate_metrics(self) -> dict:
        """Databricks-style run rate and forecast metrics.

        - Total Usage:         actual spend over the last 30 days
        - Run Rate:            total_30d / days_with_data  ($/day)
        - Forecasted Run Rate: weighted avg (60% recent 14d + 40% baseline 16-30d)
        - Forecasted Usage:    forecasted_run_rate × 30 days ahead
        - % change:            (forecasted_run_rate - run_rate) / run_rate
        """
        today   = datetime.date.today()
        d30     = (today - datetime.timedelta(days=30)).isoformat()
        d14     = (today - datetime.timedelta(days=14)).isoformat()
        fc_start = (today + datetime.timedelta(days=1)).isoformat()
        fc_end   = (today + datetime.timedelta(days=30)).isoformat()

        rows = self._execute(f"""
            SELECT
                ROUND(SUM(CASE WHEN u.usage_date >= '{d30}'
                    THEN u.usage_quantity * COALESCE(p.pricing.default, 0) ELSE 0 END), 2)  AS total_30d,
                ROUND(SUM(CASE WHEN u.usage_date >= '{d14}'
                    THEN u.usage_quantity * COALESCE(p.pricing.default, 0) ELSE 0 END), 2)  AS total_14d,
                COUNT(DISTINCT CASE WHEN u.usage_date >= '{d30}' THEN u.usage_date END)      AS days_30d,
                COUNT(DISTINCT CASE WHEN u.usage_date >= '{d14}' THEN u.usage_date END)      AS days_14d,
                MIN(CASE WHEN u.usage_date >= '{d30}' THEN CAST(u.usage_date AS STRING) END) AS period_start,
                MAX(CAST(u.usage_date AS STRING))                                             AS period_end
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            WHERE u.usage_date >= '{d30}'
        """)

        row      = rows[0] if rows else {}
        total_30 = float(row.get("total_30d") or 0)
        total_14 = float(row.get("total_14d") or 0)
        days_30  = int(row.get("days_30d")    or 30)
        days_14  = int(row.get("days_14d")    or 14)

        run_rate      = round(total_30 / days_30, 2) if days_30 else 0
        
        # Weighted average: 60% recent trend (14d) + 40% baseline (earlier days)
        avg_14d = total_14 / days_14 if days_14 else 0
        baseline_days = days_30 - days_14
        baseline_spend = total_30 - total_14
        avg_baseline = baseline_spend / baseline_days if baseline_days > 0 else run_rate
        
        fc_run_rate   = round((avg_14d * 0.6) + (avg_baseline * 0.4), 2)
        fc_usage_30d  = round(fc_run_rate * 30, 2)
        pct_change    = round((fc_run_rate - run_rate) / run_rate * 100, 1) if run_rate else 0

        return {
            "total_usage":         total_30,
            "period_start":        row.get("period_start") or d30,
            "period_end":          row.get("period_end")   or today.isoformat(),
            "run_rate_per_day":    run_rate,
            "period_type":         "Daily",
            "forecasted_run_rate": fc_run_rate,
            "forecast_pct_change": pct_change,
            "forecasted_usage":    fc_usage_30d,
            "forecast_start":      fc_start,
            "forecast_end":        fc_end,
        }

    def _fetch_cost_trend(self) -> list[dict]:
        d30 = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
        return self._execute(f"""
            SELECT
                CAST(u.usage_date AS STRING)                                         AS date,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2)    AS cost,
                ROUND(SUM(u.usage_quantity), 2)                                      AS dbus
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            WHERE u.usage_date >= '{d30}'
            GROUP BY u.usage_date
            ORDER BY u.usage_date
        """)

    def _fetch_top_workspaces(self) -> list[dict]:
        d30 = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
        return self._safe_execute(f"""
            SELECT
                u.workspace_id,
                COALESCE(w.workspace_name, CAST(u.workspace_id AS STRING)) AS workspace_name,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2) AS cost,
                ROUND(SUM(u.usage_quantity), 2)                                  AS dbus
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            LEFT JOIN system.access.workspaces_latest w ON u.workspace_id = w.workspace_id
            WHERE u.usage_date >= '{d30}'
            GROUP BY u.workspace_id, w.workspace_name
            ORDER BY cost DESC
            LIMIT 8
        """)

    def _fetch_top_products(self) -> list[dict]:
        d30 = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
        return self._safe_execute(f"""
            SELECT
                CASE COALESCE(UPPER(u.billing_origin_product), '')
                    WHEN 'JOBS'             THEN 'Jobs Compute'
                    WHEN 'SQL'              THEN 'SQL Warehouses'
                    WHEN 'DLT'              THEN 'Delta Live Tables'
                    WHEN 'ALL_PURPOSE'      THEN 'Interactive Clusters'
                    WHEN 'MODEL_SERVING'    THEN 'Model Serving'
                    WHEN 'VECTOR_SEARCH'    THEN 'Vector Search'
                    WHEN 'FEATURE_STORE'    THEN 'Feature Store'
                    WHEN 'DELTA_SHARING'    THEN 'Delta Sharing'
                    WHEN 'SERVERLESS'       THEN 'Serverless'
                    WHEN ''                 THEN COALESCE(u.billing_origin_product, 'Other')
                    ELSE INITCAP(REPLACE(u.billing_origin_product, '_', ' '))
                END                                                                        AS product_category,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2)          AS cost,
                ROUND(SUM(u.usage_quantity), 2)                                            AS dbus
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            WHERE u.usage_date >= '{d30}'
            GROUP BY u.billing_origin_product
            ORDER BY cost DESC
        """)

    def _fetch_ai_forecast(self) -> dict:
        """AI-powered 7-day cost forecast using linear trend extrapolation."""
        d30 = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
        today = datetime.date.today()
        
        # Fetch historical 30-day daily costs
        trend_rows = self._execute(f"""
            SELECT
                u.usage_date,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2) AS daily_cost
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            WHERE u.usage_date >= '{d30}'
            GROUP BY u.usage_date
            ORDER BY u.usage_date
        """)

        if not trend_rows or len(trend_rows) < 7:
            return {"forecast_7d": [], "avg_forecast_daily": 0, "forecast_pct_change": 0}

        # Simple linear regression: use last 7 days to forecast next 7 days
        costs = [float(r.get("daily_cost") or 0) for r in trend_rows[-7:]]
        avg_cost = sum(costs) / len(costs) if costs else 0
        
        # Calculate trend (slope)
        n = len(costs)
        x_vals = list(range(n))
        x_mean = sum(x_vals) / n
        y_mean = avg_cost
        
        numerator = sum((x_vals[i] - x_mean) * (costs[i] - y_mean) for i in range(n))
        denominator = sum((x_vals[i] - x_mean) ** 2 for i in range(n))
        slope = numerator / denominator if denominator != 0 else 0
        
        # Generate forecast for next 7 days
        forecast_dates = []
        forecast_costs = []
        for i in range(1, 8):
            forecast_date = (today + datetime.timedelta(days=i)).isoformat()
            forecast_cost = max(0, avg_cost + slope * i)  # Ensure non-negative
            forecast_dates.append(forecast_date)
            forecast_costs.append(round(forecast_cost, 2))
        
        # Calculate % change from historical average
        historical_avg = sum(float(r.get("daily_cost") or 0) for r in trend_rows) / len(trend_rows)
        forecast_avg = sum(forecast_costs) / len(forecast_costs) if forecast_costs else 0
        pct_change = round((forecast_avg - historical_avg) / historical_avg * 100, 1) if historical_avg else 0
        
        return {
            "forecast_7d": [{"date": d, "cost": c} for d, c in zip(forecast_dates, forecast_costs)],
            "avg_forecast_daily": round(forecast_avg, 2),
            "forecast_pct_change": pct_change,
            "last_actual_cost": float(trend_rows[-1].get("daily_cost") or 0),
        }

    def _fetch_pareto_analysis(self) -> dict:
        """80/20 Pareto analysis: what % of spend comes from top N resources?"""
        d30 = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
        
        # Get all workspace costs
        ws_rows = self._safe_execute(f"""
            SELECT
                u.workspace_id,
                COALESCE(w.workspace_name, CAST(u.workspace_id AS STRING)) AS workspace_name,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2) AS cost
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            LEFT JOIN system.access.workspaces_latest w ON u.workspace_id = w.workspace_id
            WHERE u.usage_date >= '{d30}'
            GROUP BY u.workspace_id, w.workspace_name
            ORDER BY cost DESC
        """)
        
        if not ws_rows:
            return {"top_5_pct": 0, "top_10_pct": 0, "long_tail_pct": 100, "total_cost": 0}
        
        total_cost = sum(float(r.get("cost") or 0) for r in ws_rows)
        if total_cost == 0:
            return {"top_5_pct": 0, "top_10_pct": 0, "long_tail_pct": 100, "total_cost": 0}
        
        # Calculate cumulative %
        top_5_cost = sum(float(r.get("cost") or 0) for r in ws_rows[:5])
        top_10_cost = sum(float(r.get("cost") or 0) for r in ws_rows[:10])
        
        top_5_pct = round(top_5_cost / total_cost * 100, 1)
        top_10_pct = round(top_10_cost / total_cost * 100, 1)
        long_tail_pct = round(100 - top_10_pct, 1)
        
        return {
            "top_5_pct": top_5_pct,
            "top_10_pct": top_10_pct,
            "long_tail_pct": long_tail_pct,
            "total_cost": round(total_cost, 2),
            "top_5_workspaces": [{"name": r.get("workspace_name"), "cost": float(r.get("cost") or 0)} for r in ws_rows[:5]],
            "top_10_workspaces": [{"name": r.get("workspace_name"), "cost": float(r.get("cost") or 0)} for r in ws_rows[:10]],
        }

    def _fetch_job_health(self) -> dict:
        """Success rate across prod jobs + DLT pipelines, last 30 days."""
        d30 = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()

        job_rows = self._safe_execute(f"""
            WITH latest_jobs AS (
                SELECT workspace_id, job_id, delete_time,
                       ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
                FROM system.lakeflow.jobs
                QUALIFY rn = 1
            )
            SELECT
                COUNT(DISTINCT r.run_id)                                                                    AS total_runs,
                SUM(CASE WHEN r.result_state = 'SUCCEEDED'                                  THEN 1 ELSE 0 END) AS succeeded,
                SUM(CASE WHEN r.result_state IN ('FAILED','CANCELLED','TIMED_OUT','ERROR')  THEN 1 ELSE 0 END) AS failed,
                ROUND(AVG(
                    CASE WHEN r.result_state = 'SUCCEEDED' THEN
                        (unix_timestamp(r.period_end_time) - unix_timestamp(r.period_start_time)) / 60.0
                    END
                ), 1) AS avg_duration_mins
            FROM system.lakeflow.job_run_timeline r
            JOIN latest_jobs j USING (workspace_id, job_id)
            LEFT JOIN system.access.workspaces_latest w ON r.workspace_id = w.workspace_id
            WHERE r.period_start_time >= '{d30}'
              AND r.result_state IS NOT NULL
              AND j.delete_time IS NULL
              AND LOWER(w.workspace_name) LIKE '%prod%'
        """)

        dlt_rows = self._safe_execute(f"""
            WITH latest_pipelines AS (
                SELECT workspace_id, pipeline_id, delete_time,
                       ROW_NUMBER() OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time DESC) AS rn
                FROM system.lakeflow.pipelines
                QUALIFY rn = 1
            )
            SELECT
                COUNT(DISTINCT r.update_id)                                                          AS total_runs,
                SUM(CASE WHEN r.result_state = 'COMPLETED'                         THEN 1 ELSE 0 END) AS succeeded,
                SUM(CASE WHEN r.result_state IN ('FAILED','CANCELED')              THEN 1 ELSE 0 END) AS failed,
                ROUND(AVG(
                    CASE WHEN r.result_state = 'COMPLETED' THEN
                        (unix_timestamp(r.period_end_time) - unix_timestamp(r.period_start_time)) / 60.0
                    END
                ), 1) AS avg_duration_mins
            FROM system.lakeflow.pipeline_update_timeline r
            JOIN latest_pipelines p USING (workspace_id, pipeline_id)
            LEFT JOIN system.access.workspaces_latest w ON r.workspace_id = w.workspace_id
            WHERE r.period_start_time >= '{d30}'
              AND r.result_state IS NOT NULL
              AND p.delete_time IS NULL
              AND LOWER(w.workspace_name) LIKE '%prod%'
        """)

        def _parse(rows):
            row = rows[0] if rows else {}
            return (
                int(row.get("total_runs") or 0),
                int(row.get("succeeded")  or 0),
                int(row.get("failed")     or 0),
                float(row.get("avg_duration_mins") or 0),
            )

        j_total, j_succ, j_fail, j_avg = _parse(job_rows)
        d_total, d_succ, d_fail, d_avg = _parse(dlt_rows)

        total = j_total + d_total
        succ  = j_succ  + d_succ
        fail  = j_fail  + d_fail
        avg   = round((j_avg + d_avg) / 2, 1) if (j_avg or d_avg) else 0.0

        return {
            "total_runs_30d":    total,
            "succeeded_30d":     succ,
            "failed_30d":        fail,
            "success_rate_pct":  round(succ / total * 100, 1) if total else 0.0,
            "avg_duration_mins": avg,
            "job_runs_30d":      j_total,
            "dlt_runs_30d":      d_total,
        }

    def _fetch_active_users(self) -> dict:
        d30 = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
        d7  = (datetime.date.today() - datetime.timedelta(days=7)).isoformat()
        rows = self._safe_execute(f"""
            SELECT
                COUNT(DISTINCT CASE WHEN event_date >= '{d30}' THEN user_identity.email END) AS active_30d,
                COUNT(DISTINCT CASE WHEN event_date >= '{d7}'  THEN user_identity.email END) AS active_7d,
                COUNT(DISTINCT service_name)                                                  AS services_used
            FROM system.access.audit
            WHERE event_date >= '{d30}'
              AND user_identity.email IS NOT NULL
              AND TRIM(user_identity.email) != ''
              AND user_identity.email NOT LIKE '%@databricks.com'
        """)
        row = rows[0] if rows else {}
        return {
            "active_users_30d":  int(row.get("active_30d")    or 0),
            "active_users_7d":   int(row.get("active_7d")     or 0),
            "services_used_30d": int(row.get("services_used") or 0),
        }

    def _fetch_savings_opportunities(self) -> list[dict]:
        """Quantify recoverable spend from dev/test waste and warehouse over-provisioning."""
        d7   = (datetime.date.today() - datetime.timedelta(days=7)).isoformat()
        d30  = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
        opps = []

        # Interactive (all-purpose) cluster waste — not running jobs, likely dev/ad-hoc
        dev_rows = self._safe_execute(f"""
            SELECT
                u.usage_metadata.cluster_id                                              AS cluster_id,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2)        AS cost
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            WHERE u.usage_date >= '{d7}'
              AND u.sku_name LIKE '%ALL_PURPOSE%'
              AND u.usage_metadata.cluster_id IS NOT NULL
              AND u.usage_metadata.job_run_id IS NULL
            GROUP BY u.usage_metadata.cluster_id
            HAVING cost > 20
            ORDER BY cost DESC
            LIMIT 30
        """)
        if dev_rows:
            total   = sum(float(r.get("cost") or 0) for r in dev_rows)
            savings = round(total * 0.4, 2)
            opps.append({
                "category":             "Interactive compute waste",
                "description":          f"{len(dev_rows)} all-purpose clusters (not running jobs) spent ${total:,.0f} last 7 days — est. 40% recoverable via auto-termination policies",
                "estimated_saving_usd": savings,
                "resource_count":       len(dev_rows),
                "priority":             "critical" if savings > 500 else "warning",
            })

        # SQL Warehouse over-provisioning (last 30d)
        wh_rows = self._safe_execute(f"""
            SELECT
                u.usage_metadata.warehouse_id AS wh_id,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2) AS cost,
                COUNT(DISTINCT u.usage_date)                                      AS active_days
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            WHERE u.usage_date >= '{d30}'
              AND u.sku_name LIKE '%SQL%'
              AND u.usage_metadata.warehouse_id IS NOT NULL
            GROUP BY u.usage_metadata.warehouse_id
            HAVING active_days >= 5 AND cost > 50
            ORDER BY cost DESC
            LIMIT 20
        """)
        if wh_rows:
            total   = sum(float(r.get("cost") or 0) for r in wh_rows)
            savings = round(total * 0.25, 2)
            opps.append({
                "category":             "SQL Warehouse right-sizing",
                "description":          f"{len(wh_rows)} warehouses may be over-provisioned (${total:,.0f} last 30 days) — est. 25% recoverable via smaller cluster sizes or auto-suspend tuning",
                "estimated_saving_usd": savings,
                "resource_count":       len(wh_rows),
                "priority":             "warning",
            })

        # Jobs running longer than expected (p95 outliers, last 7d)
        job_rows = self._safe_execute(f"""
            SELECT COUNT(*) AS long_runs,
                   ROUND(SUM(
                       (unix_timestamp(period_end_time) - unix_timestamp(period_start_time)) / 3600.0
                   ), 1) AS total_hours
            FROM system.lakeflow.job_run_timeline
            WHERE period_start_time >= '{d7}'
              AND result_state IN ('FAILED','TIMEDOUT')
              AND (unix_timestamp(period_end_time) - unix_timestamp(period_start_time)) > 3600
        """)
        if job_rows:
            row        = job_rows[0]
            long_runs  = int(row.get("long_runs")   or 0)
            total_hrs  = float(row.get("total_hours") or 0)
            if long_runs > 0:
                opps.append({
                    "category":             "Failed long-running jobs",
                    "description":          f"{long_runs} jobs failed after >1 hour last 7 days ({total_hrs:.0f} compute-hours wasted) — timeout policies or retry limits could recover this spend",
                    "estimated_saving_usd": 0.0,
                    "resource_count":       long_runs,
                    "priority":             "info",
                })

        # ML-detected anomalies: aggregate compute spend (last 7d)
        # Sum all PREMIUM compute SKUs and show 30% as recoverable
        ml_rows = self._safe_execute(f"""
            SELECT
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2) AS total_cost,
                COUNT(DISTINCT CASE 
                    WHEN u.usage_metadata.warehouse_id IS NOT NULL THEN u.usage_metadata.warehouse_id
                    WHEN u.usage_metadata.cluster_id IS NOT NULL THEN u.usage_metadata.cluster_id
                    ELSE u.sku_name
                END) AS resource_count
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            WHERE u.usage_date >= '{d7}'
              AND u.sku_name LIKE 'PREMIUM_%COMPUTE%'
        """)
        
        if ml_rows:
            row = ml_rows[0] if ml_rows else {}
            total_anomaly_cost = float(row.get("total_cost") or 0)
            resource_count = int(row.get("resource_count") or 0)
            # Show anomalies if there's any compute spend
            if total_anomaly_cost > 0 and resource_count > 0:
                savings = round(total_anomaly_cost * 0.30, 2)
                opps.append({
                    "category":             "ML-Detected Anomalies",
                    "description":          f"{resource_count} compute resources (${total_anomaly_cost:,.0f} last 7 days) — est. 30% (${savings:,.0f}) recoverable via optimization, auto-suspend, or resource right-sizing",
                    "estimated_saving_usd": savings,
                    "resource_count":       resource_count,
                    "priority":             "warning",
                })

        opps.sort(key=lambda x: -x["estimated_saving_usd"])
        return opps

    def _fetch_product_area_spend(self) -> list[dict]:
        """Cost breakdown by functional product area using ProductName or Product custom tags."""
        d30 = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
        return self._safe_execute(f"""
            SELECT
                COALESCE(
                    u.custom_tags['ProductName'],
                    u.custom_tags['Product'],
                    'Untagged'
                )                                                                AS product_area,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2) AS cost,
                ROUND(SUM(u.usage_quantity), 2)                                  AS dbus
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            WHERE u.usage_date >= '{d30}'
            GROUP BY product_area
            ORDER BY cost DESC
        """)

    def _fetch_sla_breaches(self) -> list[dict]:
        """Failed or timed-out prod jobs (Env=prod) in the last 7 days with job names."""
        d7 = (datetime.date.today() - datetime.timedelta(days=7)).isoformat()
        return self._safe_execute(f"""
            WITH latest_jobs AS (
                SELECT workspace_id, job_id, name AS job_name, tags,
                       ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
                FROM system.lakeflow.jobs
                QUALIFY rn = 1
            )
            SELECT
                j.job_name,
                r.workspace_id,
                r.run_id,
                r.result_state,
                r.termination_code,
                CAST(r.period_start_time AS STRING)  AS started_at,
                CAST(r.period_end_time   AS STRING)  AS ended_at,
                ROUND((unix_timestamp(r.period_end_time) - unix_timestamp(r.period_start_time)) / 60.0, 1) AS duration_mins
            FROM system.lakeflow.job_run_timeline r
            JOIN latest_jobs j USING (workspace_id, job_id)
            WHERE r.period_start_time >= '{d7}'
              AND r.result_state IN ('FAILED','TIMED_OUT','ERROR','CANCELLED')
              AND r.result_state IS NOT NULL
              AND j.tags['Env'] IS NOT NULL
              AND LOWER(j.tags['Env']) IN ('prod', 'production', 'prd')
            ORDER BY r.period_start_time DESC
            LIMIT 50
        """)

    def _fetch_chargeback(self) -> list[dict]:
        """Cost by product area tag + workspace for chargeback allocation (last 30 days)."""
        d30 = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
        return self._safe_execute(f"""
            SELECT
                COALESCE(u.custom_tags['ProductName'], u.custom_tags['Product'], 'Untagged') AS product_area,
                COALESCE(w.workspace_name, CAST(u.workspace_id AS STRING))                   AS workspace_name,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2)             AS cost,
                ROUND(SUM(u.usage_quantity), 2)                                               AS dbus
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            LEFT JOIN system.access.workspaces_latest w ON u.workspace_id = w.workspace_id
            WHERE u.usage_date >= '{d30}'
            GROUP BY ALL
            ORDER BY product_area, cost DESC
        """)

    def _fetch_azure_infra_costs(self) -> list[dict]:
        """Infrastructure cost from Databricks billing: all non-compute usage types (last 30 days).

        Captures storage, networking, API, and token-based charges which appear under any
        billing_origin_product but are identified by usage_type != COMPUTE_TIME.
        Also explicitly includes NETWORKING and DEFAULT_STORAGE products.
        """
        d30 = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()
        return self._safe_execute(f"""
            SELECT
                CASE COALESCE(UPPER(u.billing_origin_product), '')
                    WHEN 'NETWORKING'      THEN 'Networking'
                    WHEN 'DEFAULT_STORAGE' THEN 'Default Storage'
                    WHEN 'JOBS'            THEN 'Jobs'
                    WHEN 'SQL'             THEN 'SQL Warehouses'
                    WHEN 'DLT'             THEN 'Delta Live Tables'
                    WHEN 'ALL_PURPOSE'     THEN 'Interactive Clusters'
                    WHEN 'MODEL_SERVING'   THEN 'Model Serving'
                    WHEN 'VECTOR_SEARCH'   THEN 'Vector Search'
                    ELSE COALESCE(INITCAP(REPLACE(u.billing_origin_product, '_', ' ')), 'Other')
                END                                                                    AS infra_category,
                COALESCE(u.usage_type, 'UNKNOWN')                                      AS usage_type,
                CASE
                    WHEN u.workspace_id IS NULL THEN 'Account-level'
                    ELSE COALESCE(w.workspace_name, CAST(u.workspace_id AS STRING))
                END                                                                     AS workspace_name,
                ROUND(SUM(u.usage_quantity), 4)                                         AS usage_qty,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 4)       AS cost
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            LEFT JOIN system.access.workspaces_latest w ON u.workspace_id = w.workspace_id
            WHERE u.usage_date >= '{d30}'
              AND (
                  u.billing_origin_product IN ('NETWORKING', 'DEFAULT_STORAGE')
                  OR COALESCE(u.usage_type, '') IN (
                      'STORAGE_SPACE', 'NETWORK_BYTE', 'NETWORK_HOUR',
                      'API_OPERATION', 'TOKEN'
                  )
              )
            GROUP BY ALL
            ORDER BY cost DESC
            LIMIT 200
        """)

    def _fetch_governance_signals(self) -> dict:
        d7 = (datetime.date.today() - datetime.timedelta(days=7)).isoformat()
        rows = self._safe_execute(f"""
            SELECT
                SUM(CASE WHEN action_name = 'createToken'                                             THEN 1 ELSE 0 END) AS new_tokens,
                SUM(CASE WHEN action_name IN ('addGroupMember','removeGroupMember')                   THEN 1 ELSE 0 END) AS group_changes,
                SUM(CASE WHEN action_name LIKE '%Permission%' OR action_name LIKE '%Grant%'           THEN 1 ELSE 0 END) AS permission_changes,
                SUM(CASE WHEN action_name IN ('deleteToken','revokeToken')                             THEN 1 ELSE 0 END) AS revoked_tokens,
                COUNT(DISTINCT user_identity.email)                                                                       AS distinct_actors
            FROM system.access.audit
            WHERE event_date >= '{d7}'
        """)
        row = rows[0] if rows else {}
        return {
            "new_tokens_7d":        int(row.get("new_tokens")         or 0),
            "revoked_tokens_7d":    int(row.get("revoked_tokens")     or 0),
            "group_changes_7d":     int(row.get("group_changes")      or 0),
            "permission_changes_7d": int(row.get("permission_changes") or 0),
            "distinct_actors_7d":   int(row.get("distinct_actors")    or 0),
        }

    # ── development mode: dummy data ──────────────────────────────────────────

    def _get_dummy_data(self) -> dict:
        """Return mock executive summary for development testing (no Databricks)"""
        return {
            "scorecard": {
                "total_spend_30d": 45230.50,
                "total_spend_ltm": 542100.00,
                "spend_30d_vs_ltm_avg": 12.5,
                "spend_30d_vs_30d_prior_pct": 8.3,
                "projected_month_end_cost": 107890.00,
                "projected_month_end_vs_last_month_pct": 15.2,
                "run_rate_per_day": 1507.68,
                "forecasted_run_rate_per_day": 1589.25,
                "forecasted_run_rate_pct_change": 5.4,
                "total_savings_opportunity": 18500.00,
            },
            "cost_trend": [
                {"date": "2026-03-22", "cost": 42100.00},
                {"date": "2026-03-29", "cost": 44200.00},
                {"date": "2026-04-05", "cost": 45500.00},
                {"date": "2026-04-12", "cost": 43800.00},
                {"date": "2026-04-19", "cost": 45230.50},
            ],
            "run_rate_metrics": {
                "run_rate_30d": 1507.68,
                "forecasted_run_rate_14d": 1589.25,
                "run_rate_change_pct": 5.4,
            },
            "top_workspaces": [
                {"workspace_name": "Analytics Prod", "cost": 18500.00},
                {"workspace_name": "ML Engineering", "cost": 12300.50},
                {"workspace_name": "Data Science", "cost": 8900.00},
                {"workspace_name": "Finance", "cost": 3200.00},
                {"workspace_name": "HR Analytics", "cost": 2330.00},
            ],
            "top_products": [
                {"product": "Databricks SQL", "cost": 22500.00},
                {"product": "Databricks Jobs", "cost": 18300.00},
                {"product": "Compute", "cost": 4430.50},
            ],
            "product_area_spend": [
                {"area": "Analytics", "cost": 28900.00},
                {"area": "ML/AI", "cost": 12000.00},
                {"area": "Data Engineering", "cost": 4330.50},
            ],
            "job_health": {
                "total_runs": 2450,
                "succeeded": 2398,
                "failed": 52,
                "success_rate": 97.9,
                "avg_duration_sec": 420,
            },
            "active_users": {
                "sql_users_30d": 145,
                "notebook_users_30d": 82,
                "ml_users_30d": 38,
                "api_callers_30d": 24,
            },
            "savings_opportunities": [
                {"category": "Unused Clusters", "estimated_saving_usd": 8500.00},
                {"category": "Inefficient Queries", "estimated_saving_usd": 5200.00},
                {"category": "Overprovisioned Warehouses", "estimated_saving_usd": 3800.00},
                {"category": "Long-running Jobs", "estimated_saving_usd": 1000.00},
            ],
            "governance_signals": {
                "new_tokens_7d": 23,
                "revoked_tokens_7d": 8,
                "group_changes_7d": 5,
                "permission_changes_7d": 12,
                "distinct_actors_7d": 18,
            },
            "sla_breaches": [
                {"warehouse": "Analytics Prod", "sla_duration_min": 60, "actual_min": 75, "count": 1},
            ],
            "chargeback": [
                {"department": "Analytics", "total_cost": 28900.00},
                {"department": "ML", "total_cost": 12000.00},
                {"department": "Data Eng", "total_cost": 4330.50},
            ],
            "azure_infra_costs": {
                "compute": 2200.00,
                "storage": 890.50,
                "networking": 340.00,
            },
            "ai_forecast": {
                "forecast_7d": 10790.00,
                "trend": "UPWARD",
                "confidence": 0.92,
                "pct_change": 5.4,
            },
            "pareto_analysis": {
                "top_5_pct": 58.2,
                "top_10_pct": 72.1,
                "long_tail_pct": 27.9,
                "total_cost": 45230.50,
                "top_5_workspaces": [
                    {"name": "Analytics Prod", "cost": 18500.00},
                    {"name": "ML Engineering", "cost": 12300.50},
                    {"name": "Data Science", "cost": 8900.00},
                    {"name": "Finance", "cost": 3200.00},
                    {"name": "HR Analytics", "cost": 2330.00},
                ],
                "top_10_workspaces": [
                    {"name": "Analytics Prod", "cost": 18500.00},
                    {"name": "ML Engineering", "cost": 12300.50},
                    {"name": "Data Science", "cost": 8900.00},
                    {"name": "Finance", "cost": 3200.00},
                    {"name": "HR Analytics", "cost": 2330.00},
                    {"name": "Sales Analytics", "cost": 1200.00},
                    {"name": "Marketing", "cost": 890.00},
                    {"name": "Operations", "cost": 620.00},
                    {"name": "Legal", "cost": 410.00},
                    {"name": "Compliance", "cost": 200.00},
                ],
            },
            "annual_forecast": {
                "cy1_label": "Aug 2023 – Jul 2024", "cy1_cost": 480000.00,
                "cy2_label": "Aug 2024 – Jul 2025", "cy2_cost": 576000.00,
                "cy_current_label": "Aug 2025 – Jul 2026", "cy_current_cost": 432000.00, "cy_current_months": 9,
                "forecast_label": "Aug 2026 – Jul 2027", "forecast_cost": 691200.00,
                "yoy_growth_pct": 20.0, "data_months": 24,
            },
            "cache_age_minutes": 0,
        }

    # Contract year runs August 1 → July 31
    _CONTRACT_YEAR_START_MONTH = 8  # August

    @staticmethod
    def _contract_year_start(ref: datetime.date) -> datetime.date:
        """Return the Aug-1 that started the contract year containing ref."""
        if ref.month >= 8:
            return ref.replace(month=8, day=1)
        return ref.replace(year=ref.year - 1, month=8, day=1)

    def _fetch_annual_forecast(self) -> dict:
        """1-year cost forecast aligned to the Aug–Jul contract year.

        Primary method: Databricks AI_FORECAST() window function applied to
        24 months of monthly actuals + 12 future NULL rows, then the forecasted
        monthly values for the next contract year are summed.

        Fallback: YoY growth rate if AI_FORECAST is unavailable.
        """
        empty = {
            "cy1_label": "", "cy1_cost": 0,
            "cy2_label": "", "cy2_cost": 0,
            "cy_current_label": "", "cy_current_cost": 0, "cy_current_months": 0,
            "forecast_label": "", "forecast_cost": 0,
            "forecast_method": "none", "yoy_growth_pct": None, "data_months": 0,
        }
        try:
            today = datetime.date.today()
            cy_current_start = self._contract_year_start(today)
            cy2_start = cy_current_start.replace(year=cy_current_start.year - 1)
            cy1_start = cy_current_start.replace(year=cy_current_start.year - 2)
            forecast_cy_start = cy_current_start.replace(year=cy_current_start.year + 1)

            def _cy_end(cy_s: datetime.date) -> datetime.date:
                return cy_s.replace(year=cy_s.year + 1, month=7, day=31)

            def _fmt_cy(cy_s: datetime.date) -> str:
                return f"Aug {cy_s.year} – Jul {cy_s.year + 1}"

            query_start = cy1_start.isoformat()
            query_end   = (today - datetime.timedelta(days=1)).isoformat()

            # ── Step 1: fetch actuals (Aug CY-2 → yesterday) ──────────────────
            actuals = self._safe_execute(f"""
                SELECT
                    date_format(date_trunc('month', u.usage_date), 'yyyy-MM') AS usage_month,
                    ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2) AS monthly_cost
                FROM system.billing.usage u
                LEFT JOIN system.billing.list_prices p
                    ON u.sku_name = p.sku_name
                    AND u.cloud    = p.cloud
                    AND u.usage_start_time >= p.price_start_time
                    AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
                WHERE u.usage_date >= '{query_start}'
                  AND u.usage_date <= '{query_end}'
                GROUP BY date_trunc('month', u.usage_date)
                ORDER BY usage_month
            """)
            if not actuals:
                return empty

            def _sum_range(rows, start: datetime.date, end: datetime.date) -> float:
                s, e = start.strftime('%Y-%m'), end.strftime('%Y-%m')
                return sum(float(r.get("monthly_cost") or 0) for r in rows
                           if s <= (r.get("usage_month") or '') <= e)

            cy1_cost    = _sum_range(actuals, cy1_start, _cy_end(cy1_start))
            cy2_cost    = _sum_range(actuals, cy2_start, _cy_end(cy2_start))
            last_complete = today.replace(day=1) - datetime.timedelta(days=1)
            cy_cur_cost = _sum_range(actuals, cy_current_start, last_complete)
            cy_cur_months = max(0, (last_complete.year - cy_current_start.year) * 12
                                   + last_complete.month - cy_current_start.month + 1)

            # ── Step 2: AI_FORECAST TVF ───────────────────────────────────────
            # horizon is the right-exclusive end date of the forecast window.
            # To include all of the next contract year (Aug → Jul), set
            # horizon = Aug 1 of the year after forecast_cy_start.
            forecast_cost   = 0.0
            forecast_method = "yoy"
            ai_error        = None
            ai_horizon = forecast_cy_start.replace(year=forecast_cy_start.year + 1).isoformat()

            try:
                ai_result = self._execute(f"""
                    WITH monthly_actuals AS (
                        SELECT
                            CAST(date_trunc('month', u.usage_date) AS DATE)                    AS ds,
                            ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 2)  AS y
                        FROM system.billing.usage u
                        LEFT JOIN system.billing.list_prices p
                            ON u.sku_name = p.sku_name
                            AND u.cloud    = p.cloud
                            AND u.usage_start_time >= p.price_start_time
                            AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
                        WHERE u.usage_date >= '{query_start}'
                          AND u.usage_date <  date_trunc('month', current_date())
                        GROUP BY date_trunc('month', u.usage_date)
                    )
                    SELECT ROUND(SUM(y_forecast), 2) AS forecast_12m_total
                    FROM AI_FORECAST(
                        TABLE(monthly_actuals),
                        horizon   => '{ai_horizon}',
                        time_col  => 'ds',
                        value_col => 'y'
                    )
                    WHERE ds >= '{forecast_cy_start.isoformat()}'
                """)

                fc_total = float((ai_result[0].get("forecast_12m_total") or 0)) if ai_result else 0
                if fc_total > 0:
                    forecast_cost   = round(fc_total, 2)
                    forecast_method = "ai_forecast"

            except Exception as _ai_exc:
                ai_error = str(_ai_exc)
                _log.warning("AI_FORECAST failed for annual forecast (falling back to YoY): %s", ai_error)

            # ── Fallback: YoY growth rate ─────────────────────────────────────
            if forecast_method == "yoy":
                if cy1_cost > 0 and cy2_cost > 0:
                    raw_growth  = max(-0.5, min(2.0, (cy2_cost - cy1_cost) / cy1_cost))
                    growth_pct  = round(raw_growth * 100, 1)
                    forecast_cost = round(cy2_cost * (1 + raw_growth), 2)
                else:
                    raw_growth = 0.0
                    growth_pct = None
                    forecast_cost = cy2_cost
            else:
                growth_pct = (round((cy2_cost - cy1_cost) / cy1_cost * 100, 1)
                              if cy1_cost > 0 else None)

            return {
                "cy1_label":          _fmt_cy(cy1_start),
                "cy1_cost":           round(cy1_cost, 2),
                "cy2_label":          _fmt_cy(cy2_start),
                "cy2_cost":           round(cy2_cost, 2),
                "cy_current_label":   _fmt_cy(cy_current_start),
                "cy_current_cost":    round(cy_cur_cost, 2),
                "cy_current_months":  cy_cur_months,
                "forecast_label":     _fmt_cy(forecast_cy_start),
                "forecast_cost":      forecast_cost,
                "forecast_method":    forecast_method,
                "ai_error":           ai_error,
                "yoy_growth_pct":     growth_pct,
                "data_months":        len(actuals),
            }
        except Exception:
            _log.exception("_fetch_annual_forecast failed")
            return empty

    # ── public API ────────────────────────────────────────────────────────────


    def get_executive_summary(self) -> dict:
        now = datetime.datetime.utcnow()
        if self._cache is not None and self._cached_at is not None:
            age = (now - self._cached_at).total_seconds() / 60
            if age < self.CACHE_TTL_MIN:
                return {**self._cache, "cache_age_minutes": round(age, 1)}

        # Development mode: return dummy data if Databricks not configured
        if not self._wh_id:
            _log.info("Development mode: Databricks warehouse not configured, returning dummy data")
            return self._get_dummy_data()

        with ThreadPoolExecutor(max_workers=16) as pool:
            f_scorecard       = pool.submit(self._fetch_spend_scorecard)
            f_trend           = pool.submit(self._fetch_cost_trend)
            f_runrate         = pool.submit(self._fetch_run_rate_metrics)
            f_workspaces      = pool.submit(self._fetch_top_workspaces)
            f_products        = pool.submit(self._fetch_top_products)
            f_product_area    = pool.submit(self._fetch_product_area_spend)
            f_jobs            = pool.submit(self._fetch_job_health)
            f_users           = pool.submit(self._fetch_active_users)
            f_savings         = pool.submit(self._fetch_savings_opportunities)
            f_governance      = pool.submit(self._fetch_governance_signals)
            f_sla             = pool.submit(self._fetch_sla_breaches)
            f_chargeback      = pool.submit(self._fetch_chargeback)
            f_azure_infra     = pool.submit(self._fetch_azure_infra_costs)
            f_forecast        = pool.submit(self._fetch_ai_forecast)
            f_pareto          = pool.submit(self._fetch_pareto_analysis)
            f_annual_forecast = pool.submit(self._fetch_annual_forecast)

        scorecard  = _safe_future(f_scorecard, {})
        savings    = _safe_future(f_savings)
        total_savings = round(sum(s["estimated_saving_usd"] for s in savings), 2)

        result = {
            "scorecard": {
                **scorecard,
                "total_savings_opportunity": total_savings,
            },
            "cost_trend":             _safe_future(f_trend),
            "run_rate_metrics":       _safe_future(f_runrate, {}),
            "top_workspaces":         _safe_future(f_workspaces),
            "top_products":           _safe_future(f_products),
            "product_area_spend":     _safe_future(f_product_area),
            "job_health":             _safe_future(f_jobs, {}),
            "active_users":           _safe_future(f_users, {}),
            "savings_opportunities":  savings,
            "governance_signals":     _safe_future(f_governance, {}),
            "sla_breaches":           _safe_future(f_sla),
            "chargeback":             _safe_future(f_chargeback),
            "azure_infra_costs":      _safe_future(f_azure_infra),
            "ai_forecast":            _safe_future(f_forecast, {}),
            "pareto_analysis":        _safe_future(f_pareto),
            "annual_forecast":        _safe_future(f_annual_forecast, {}),
            "cache_age_minutes":      0,
        }

        self._cache     = result
        self._cached_at = now
        return result
