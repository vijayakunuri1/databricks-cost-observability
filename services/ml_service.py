"""
ML-based anomaly detection for Databricks cost observability.

Three detection layers
  Layer 1 – Per-resource baseline  : Z-score on duration, DBUs, cost vs 90-day history
  Layer 2 – Isolation Forest       : multivariate session features (rate, timing, type)
  Layer 3 – Cost drift             : spend spikes and upward trends per workspace × product
  +          Idle detection        : resources that are running but have had zero billing
                                     activity for >= IDLE_THRESH_HOURS hours
"""

from __future__ import annotations

import math
import datetime
from typing import Optional

import numpy as np
from sklearn.ensemble import IsolationForest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

from core.config import get_settings
from core.sql_executor import execute_sync, safe_execute_sync
from core.subscriptions import resolve_subscription


# ── helpers ───────────────────────────────────────────────────────────────────

_TYPE_ENC: dict[str, int] = {
    "cluster": 0, "warehouse": 1, "job": 2, "pipeline": 3, "other": 4,
}

_RTYPE_MAP: dict[str, str] = {
    "SQL Warehouse": "warehouse",
    "Cluster":       "cluster",
    "Job Run":       "job",
    "DLT Pipeline":  "pipeline",
}


def _f(val, default: float = 0.0) -> float:
    """Safe float coercion."""
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default


def _parse_started_at(ts_str: str) -> Optional[datetime.datetime]:
    """Parse 'YYYY-MM-DD HH:MM UTC' (from anomaly results) or 'YYYY-MM-DD HH:MM:SS'.

    NOTE: slice lengths are the *date-string* lengths (19 / 16), NOT the
    format-string lengths — format codes like %Y are 2 chars but represent 4.
    """
    if not ts_str:
        return None
    try:
        s = ts_str.strip().replace(" UTC", "").replace("T", " ")
        for fmt, date_len in (("%Y-%m-%d %H:%M:%S", 19), ("%Y-%m-%d %H:%M", 16)):
            try:
                return datetime.datetime.strptime(s[:date_len], fmt).replace(
                    tzinfo=datetime.timezone.utc
                )
            except ValueError:
                pass
    except Exception:
        pass
    return None


def _parse_sql_ts(ts_str: str) -> Optional[datetime.datetime]:
    """Parse a Spark/SQL timestamp string to UTC datetime."""
    if not ts_str:
        return None
    try:
        s = ts_str.strip().replace("T", " ").split(".")[0].split("+")[0][:19]
        return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=datetime.timezone.utc
        )
    except Exception:
        return None


# ── service ───────────────────────────────────────────────────────────────────

class MLAnomalyService:
    """Trains lightweight ML models on billing history and scores currently
    running Databricks resources for anomalous cost / waste behaviour."""

    # ── tuneable thresholds ───────────────────────────────────────────────────
    HISTORY_DAYS      = 90    # billing look-back for training
    IDLE_THRESH_HOURS = 1.5   # running but no billing for this long → idle
    MIN_BASELINE_SESS = 5     # min historical sessions before baseline scoring
    IF_MIN_SAMPLES    = 30    # min training rows for Isolation Forest
    IF_CONTAM         = 0.05  # expected anomaly fraction for IF
    DRIFT_DAYS        = 30    # look-back for drift detection
    DRIFT_SPIKE_MULT  = 2.0   # today > N × 7d rolling avg → spike
    DRIFT_TREND_PCT   = 0.30  # recent 7d avg > 30% above prior 7d → trend
    Z_CRITICAL        = 3.0
    Z_WARNING         = 2.0
    CACHE_TTL_MIN     = 30    # minutes before re-training

    _PRICE_JOIN = """
        LEFT JOIN system.billing.list_prices p
            ON u.sku_name            = p.sku_name
           AND u.cloud               = p.cloud
           AND u.usage_start_time   >= p.price_start_time
           AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
    """

    def __init__(
        self,
        client: WorkspaceClient,
        account_client=None,          # Optional[AccountClient] — avoids hard import
    ) -> None:
        self._client       = client
        self._wh_id        = get_settings().databricks_warehouse_id
        self._workspace_id = str(client.get_workspace_id())
        self._cache: Optional[dict]                  = None
        self._cached_at: Optional[datetime.datetime] = None

        # ── Build workspace_id → Azure context map ────────────────────────────
        # Primary source: AccountClient.workspaces.list() returns all workspaces
        # with azure_subscription_id, azure_resource_group, workspace_name.
        self._ws_azure: dict[str, dict] = {}
        if account_client is not None:
            try:
                for ws in account_client.workspaces.list():
                    wid = str(getattr(ws, "workspace_id", "") or "")
                    if not wid:
                        continue
                    # Azure info is nested under ws.azure_workspace_info
                    az  = getattr(ws, "azure_workspace_info", None)
                    sub = (getattr(az, "subscription_id", "") or "") if az else ""
                    rg  = (getattr(az, "resource_group",  "") or "") if az else ""
                    self._ws_azure[wid] = {
                        "workspace_name": getattr(ws, "workspace_name", "") or "",
                        "subscription":   sub,
                        "resource_group": rg,
                    }
            except Exception:
                pass  # account API unavailable — fall back to SDK config

        # Fallback: parse the current workspace's azure_workspace_resource_id
        # /subscriptions/{sub}/resourceGroups/{rg}/providers/...
        if self._workspace_id not in self._ws_azure or not self._ws_azure[self._workspace_id].get("subscription"):
            cfg = client.config
            sub = getattr(cfg, "azure_subscription_id", "") or ""
            rg  = getattr(cfg, "azure_resource_group", "") or ""
            rid = getattr(cfg, "azure_workspace_resource_id", "") or ""
            if rid and (not sub or not rg):
                parts = rid.split("/")
                lower = [p.lower() for p in parts]
                if "subscriptions" in lower:
                    i = lower.index("subscriptions")
                    sub = sub or (parts[i + 1] if i + 1 < len(parts) else "")
                if "resourcegroups" in lower:
                    i = lower.index("resourcegroups")
                    rg = rg or (parts[i + 1] if i + 1 < len(parts) else "")
            if sub or rg:
                existing = self._ws_azure.setdefault(self._workspace_id, {})
                existing.setdefault("subscription", sub)
                existing.setdefault("resource_group", rg)

    # ── SQL execution ─────────────────────────────────────────────────────────

    def _execute(self, sql: str) -> list[dict]:
        return execute_sync(self._client, self._wh_id, sql, label="ML")

    def _safe_execute(self, sql: str) -> list[dict]:
        return safe_execute_sync(self._client, self._wh_id, sql, label="ML")

    # ── data fetching ─────────────────────────────────────────────────────────

    def _fetch_history(self) -> list[dict]:
        """Per-resource daily billing aggregates — last HISTORY_DAYS days.

        One row = one resource × one calendar day.  The fields returned are:
          resource_id, resource_type, usage_date, workspace_id,
          total_dbus, total_cost, duration_hours, start_hour, day_of_week
        """
        return self._execute(f"""
            WITH base AS (
                SELECT
                    COALESCE(
                        u.usage_metadata.cluster_id,
                        u.usage_metadata.warehouse_id,
                        CAST(u.usage_metadata.job_run_id AS STRING),
                        u.usage_metadata.dlt_pipeline_id
                    )                                               AS resource_id,
                    CASE
                        WHEN u.sku_name LIKE '%ALL_PURPOSE%'        THEN 'cluster'
                        WHEN u.sku_name LIKE '%SQL%'                THEN 'warehouse'
                        WHEN u.sku_name LIKE '%JOBS%'               THEN 'job'
                        WHEN u.sku_name LIKE '%DLT%'
                          OR u.sku_name LIKE '%DELTA_LIVE%'         THEN 'pipeline'
                        ELSE 'other'
                    END                                             AS resource_type,
                    DATE(u.usage_start_time)                        AS usage_date,
                    HOUR(u.usage_start_time)                        AS hr,
                    DAYOFWEEK(u.usage_start_time)                   AS dow,
                    u.usage_quantity                                 AS dbus,
                    u.usage_quantity * COALESCE(p.pricing.default, 0) AS cost_usd,
                    unix_timestamp(u.usage_start_time)               AS ts_start,
                    unix_timestamp(u.usage_end_time)                 AS ts_end,
                    u.workspace_id
                FROM system.billing.usage u
                {self._PRICE_JOIN}
                WHERE u.usage_start_time >= timestamp(date_add(current_date(), -{self.HISTORY_DAYS}))
                  AND (
                    u.usage_metadata.cluster_id    IS NOT NULL OR
                    u.usage_metadata.warehouse_id  IS NOT NULL OR
                    u.usage_metadata.job_run_id    IS NOT NULL OR
                    u.usage_metadata.dlt_pipeline_id IS NOT NULL
                  )
            )
            SELECT
                resource_id,
                resource_type,
                CAST(usage_date AS STRING)                       AS usage_date,
                workspace_id,
                ROUND(SUM(dbus), 4)                              AS total_dbus,
                ROUND(SUM(cost_usd), 4)                          AS total_cost,
                ROUND((MAX(ts_end) - MIN(ts_start)) / 3600.0, 3) AS duration_hours,
                MIN(hr)                                          AS start_hour,
                MIN(dow)                                         AS day_of_week
            FROM base
            WHERE resource_id IS NOT NULL
            GROUP BY 1, 2, 3, 4
            LIMIT 80000
        """)

    def _fetch_daily_cost(self) -> list[dict]:
        """Daily cost by workspace × product × resource — last DRIFT_DAYS days."""
        return self._execute(f"""
            SELECT
                u.workspace_id,
                CAST(DATE(u.usage_start_time) AS STRING)            AS usage_date,
                CASE
                    WHEN u.sku_name LIKE '%ALL_PURPOSE%'             THEN 'All Purpose Compute'
                    WHEN u.sku_name LIKE '%SQL%'                     THEN 'SQL Warehouse'
                    WHEN u.sku_name LIKE '%JOBS%'                    THEN 'Jobs Compute'
                    WHEN u.sku_name LIKE '%DLT%'
                      OR u.sku_name LIKE '%DELTA_LIVE%'              THEN 'Delta Live Tables'
                    ELSE 'Other'
                END                                                  AS product,
                COALESCE(
                    u.usage_metadata.cluster_id,
                    u.usage_metadata.warehouse_id,
                    CAST(u.usage_metadata.job_run_id AS STRING),
                    u.usage_metadata.dlt_pipeline_id
                )                                                    AS resource_id,
                COALESCE(
                    u.custom_tags['ProductName'],
                    u.custom_tags['productname'],
                    u.custom_tags['Product'],
                    u.custom_tags['product']
                )                                                    AS product_tag,
                COALESCE(
                    u.custom_tags['Environment'],
                    u.custom_tags['environment'],
                    u.custom_tags['Env'],
                    u.custom_tags['env'],
                    u.custom_tags['Environments'],
                    u.custom_tags['environments']
                )                                                    AS env_tag,
                COALESCE(
                    u.custom_tags['SubscriptionId'],
                    u.custom_tags['subscriptionid'],
                    u.custom_tags['SubscriptionID'],
                    u.custom_tags['Subscription'],
                    u.custom_tags['subscription'],
                    u.custom_tags['AzureSubscriptionId'],
                    u.custom_tags['SubscriptionName'],
                    u.custom_tags['subscriptionname']
                )                                                    AS subscription_tag,
                COALESCE(
                    u.custom_tags['ResourceGroup'],
                    u.custom_tags['resourcegroup'],
                    u.custom_tags['ResourceGroupName'],
                    u.custom_tags['resource_group'],
                    u.custom_tags['resourcegroupname'],
                    u.custom_tags['RG'],
                    u.custom_tags['rg']
                )                                                    AS resource_group_tag,
                ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.default, 0)), 4) AS daily_cost,
                ROUND(SUM(u.usage_quantity), 4)                      AS daily_dbus
            FROM system.billing.usage u
            {self._PRICE_JOIN}
            WHERE u.usage_start_time >= timestamp(date_add(current_date(), -{self.DRIFT_DAYS}))
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
            ORDER BY 1, 3, 4, 2
        """)

    def _fetch_last_billing(self) -> dict[str, str]:
        """Last billing end-time per resource over the past 2 days.

        Returns a dict of  resource_id → last_billing_end  (SQL timestamp string).
        Used by idle detection to tell when each resource last had compute activity.
        """
        rows = self._execute("""
            SELECT
                COALESCE(
                    usage_metadata.cluster_id,
                    usage_metadata.warehouse_id,
                    CAST(usage_metadata.job_run_id AS STRING),
                    usage_metadata.dlt_pipeline_id
                )                                   AS resource_id,
                CAST(MAX(usage_end_time) AS STRING) AS last_billing_end
            FROM system.billing.usage
            WHERE usage_start_time >= timestamp(date_add(current_date(), -2))
              AND (
                usage_metadata.cluster_id    IS NOT NULL OR
                usage_metadata.warehouse_id  IS NOT NULL OR
                usage_metadata.job_run_id    IS NOT NULL OR
                usage_metadata.dlt_pipeline_id IS NOT NULL
              )
            GROUP BY 1
            HAVING resource_id IS NOT NULL
        """)
        return {
            r["resource_id"]: r["last_billing_end"]
            for r in rows
            if r.get("resource_id")
        }

    # ── Layer 1: per-resource baseline ────────────────────────────────────────

    def _compute_baseline(self, history: list[dict]) -> dict[str, dict]:
        """Build a statistical profile (median + std-dev) per resource."""
        buckets: dict[str, list] = {}
        for row in history:
            rid = row.get("resource_id")
            if not rid:
                continue
            buckets.setdefault(rid, []).append((
                _f(row.get("total_dbus")),
                _f(row.get("total_cost")),
                _f(row.get("duration_hours")),
                row.get("resource_type", "other"),
            ))

        baseline: dict[str, dict] = {}
        for rid, sessions in buckets.items():
            if len(sessions) < self.MIN_BASELINE_SESS:
                continue
            dbus_arr = np.array([s[0] for s in sessions], dtype=np.float32)
            cost_arr = np.array([s[1] for s in sessions], dtype=np.float32)
            dur_arr  = np.array([s[2] for s in sessions], dtype=np.float32)
            baseline[rid] = {
                "session_count": len(sessions),
                "resource_type": sessions[0][3],
                "p50_dbus":      float(np.median(dbus_arr)),
                "std_dbus":      max(float(np.std(dbus_arr)),  0.001),
                "p50_cost":      float(np.median(cost_arr)),
                "std_cost":      max(float(np.std(cost_arr)),  0.001),
                "p50_duration":  float(np.median(dur_arr)),
                "std_duration":  max(float(np.std(dur_arr)),   0.001),
            }
        return baseline

    @staticmethod
    def _z(value: float, median: float, std: float) -> float:
        """One-sided Z-score: returns 0 when value is below median."""
        return max((value - median) / std, 0.0)

    # ── Layer 2: Isolation Forest ─────────────────────────────────────────────

    @staticmethod
    def _build_features(
        dbus: float, cost: float, dur: float,
        hour: float, dow: float, rtype: str,
    ) -> list[float]:
        """Eight-dimensional feature vector for one session."""
        dur = max(dur, 0.01)
        return [
            math.log1p(dbus / dur),                   # log DBUs/hr
            math.log1p(cost),                          # log cost
            math.log1p(dur),                           # log duration
            math.sin(2 * math.pi * hour / 24),         # hour-of-day circular
            math.cos(2 * math.pi * hour / 24),
            math.sin(2 * math.pi * dow / 7),           # day-of-week circular
            math.cos(2 * math.pi * dow / 7),
            float(_TYPE_ENC.get(rtype, 4)),            # resource type ordinal
        ]

    def _train_if(self, history: list[dict]):
        """Train an IsolationForest on all historical daily sessions.

        Returns the fitted model, or None if there is not enough data.
        """
        feature_matrix = []
        for row in history:
            try:
                fv = self._build_features(
                    _f(row.get("total_dbus")),
                    _f(row.get("total_cost")),
                    _f(row.get("duration_hours")),
                    _f(row.get("start_hour"),  12.0),
                    _f(row.get("day_of_week"),  3.0),
                    row.get("resource_type", "other"),
                )
                feature_matrix.append(fv)
            except Exception:
                pass

        if len(feature_matrix) < self.IF_MIN_SAMPLES:
            return None

        model = IsolationForest(
            n_estimators=100,
            contamination=self.IF_CONTAM,
            random_state=42,
            n_jobs=-1,
        )
        model.fit(np.array(feature_matrix, dtype=np.float32))
        return model

    def _if_score(
        self, model, dbus: float, cost: float, dur: float,
        hour: float, dow: float, rtype: str,
    ) -> Optional[float]:
        """Return anomaly score 0–1 (1 = most anomalous), or None if unavailable."""
        if model is None:
            return None
        try:
            fv  = self._build_features(dbus, cost, dur, hour, dow, rtype)
            raw = model.decision_function(np.array([fv], dtype=np.float32))[0]
            # decision_function: more negative = more anomalous (range ≈ -0.5 … +0.5)
            return float(np.clip((-raw + 0.3) / 0.6, 0.0, 1.0))
        except Exception:
            return None

    # ── Layer 3: cost drift ───────────────────────────────────────────────────

    def _drift_alerts(self, daily_cost: list[dict]) -> list[dict]:
        """Detect spend spikes and upward trends per workspace × product × resource."""
        grouped: dict[tuple, list] = {}
        for row in daily_cost:
            key = (
                row.get("workspace_id", ""),
                row.get("product", ""),
                row.get("resource_id") or "",
                row.get("product_tag") or "",
                row.get("env_tag") or "",
                row.get("subscription_tag") or "",
                row.get("resource_group_tag") or "",
            )
            grouped.setdefault(key, []).append((
                row.get("usage_date", ""),
                _f(row.get("daily_cost")),
                _f(row.get("daily_dbus")),
            ))

        today_str = datetime.date.today().isoformat()
        alerts: list[dict] = []

        for (ws, prod, rid, ptag, etag, sub, rg), days in grouped.items():
            days.sort(key=lambda x: x[0])
            costs = [d[1] for d in days]
            if len(costs) < 7:
                continue

            recent7    = costs[-7:]
            prior7     = costs[-14:-7] if len(costs) >= 14 else costs[:-7]
            today_cost = costs[-1] if days[-1][0] == today_str else None
            avg_r      = sum(recent7) / len(recent7) if recent7 else 0.0
            avg_p      = sum(prior7)  / len(prior7)  if prior7  else 0.0

            if today_cost and avg_r > 0.01 and today_cost > avg_r * self.DRIFT_SPIKE_MULT:
                pct = round((today_cost - avg_r) / avg_r * 100, 1)
                alerts.append({
                    "workspace_id":        ws,
                    "product":             prod,
                    "resource_id":         rid or None,
                    "product_tag":         ptag or None,
                    "env_tag":             etag or None,
                    "subscription":        sub or None,
                    "resource_group":      rg  or None,
                    "alert_type":          "cost_spike",
                    "description":         f"Today's cost is {pct}% above 7-day rolling avg",
                    "today_cost":          round(today_cost, 2),
                    "rolling_7d_avg_cost": round(avg_r, 2),
                    "prior_7d_avg_cost":   round(avg_p, 2) if avg_p else None,
                    "pct_change":          pct,
                    "severity":            "critical" if pct > 100 else "warning",
                })
            elif avg_p > 0.01 and avg_r > avg_p * (1 + self.DRIFT_TREND_PCT):
                pct = round((avg_r - avg_p) / avg_p * 100, 1)
                alerts.append({
                    "workspace_id":        ws,
                    "product":             prod,
                    "resource_id":         rid or None,
                    "product_tag":         ptag or None,
                    "env_tag":             etag or None,
                    "subscription":        sub or None,
                    "resource_group":      rg  or None,
                    "alert_type":          "cost_trend",
                    "description":         f"7-day spend trending +{pct}% vs prior week",
                    "today_cost":          round(today_cost, 2) if today_cost else None,
                    "rolling_7d_avg_cost": round(avg_r, 2),
                    "prior_7d_avg_cost":   round(avg_p, 2),
                    "pct_change":          pct,
                    "severity":            "warning",
                })

        return sorted(alerts, key=lambda x: x["pct_change"], reverse=True)

    # ── Idle detection ────────────────────────────────────────────────────────

    def _detect_idle(
        self, running: list[dict], last_billing: dict[str, str],
    ) -> list[dict]:
        """Flag resources that are running but have had no billing recently."""
        now  = datetime.datetime.now(datetime.timezone.utc)
        idle = []

        for res in running:
            rid = res.get("resource_id") or ""
            if not rid:
                continue

            lb_str     = last_billing.get(rid)
            lb_dt      = _parse_sql_ts(lb_str) if lb_str else None
            idle_hours = (
                (now - lb_dt).total_seconds() / 3600
                if lb_dt
                else _f(res.get("duration_hours"))   # no billing in 48h → use full duration
            )

            if idle_hours < self.IDLE_THRESH_HOURS:
                continue

            idle.append({
                "resource_id":    rid,
                "service_type":   res.get("service_type", ""),
                "name":           res.get("name", ""),
                "workspace_name": res.get("workspace_name", ""),
                "environment":    res.get("environment", ""),
                "started_at":     res.get("started_at", ""),
                "duration_hours": res.get("duration_hours"),
                "last_billed":    lb_str or "No billing in 48h",
                "idle_hours":     round(idle_hours, 1),
                "severity":       "critical" if idle_hours >= 4.0 else "warning",
            })

        return sorted(idle, key=lambda x: x["idle_hours"], reverse=True)

    # ── resource scoring (Layers 1 & 2) ──────────────────────────────────────

    def _score_running(
        self,
        running: list[dict],
        baseline: dict[str, dict],
        if_model,
    ) -> list[dict]:
        """Apply baseline Z-score and Isolation Forest to each running resource."""
        results = []

        for res in running:
            rid   = res.get("resource_id") or ""
            dbus  = _f(res.get("dbus"))
            cost  = _f(res.get("cost_usd"))
            dur   = _f(res.get("duration_hours"))
            stype = res.get("service_type", "")
            rtype = _RTYPE_MAP.get(stype, "other")

            start_dt = _parse_started_at(res.get("started_at", ""))
            hour     = float(start_dt.hour)         if start_dt else 12.0
            dow      = float(start_dt.isoweekday()) if start_dt else 3.0

            detections = []

            # ── Layer 1: baseline Z-score ─────────────────────────────────────
            b = baseline.get(rid)
            if b:
                z_dur  = self._z(dur,  b["p50_duration"], b["std_duration"])
                z_cost = self._z(cost, b["p50_cost"],     b["std_cost"])
                z_dbus = self._z(dbus, b["p50_dbus"],     b["std_dbus"])
                worst  = max(z_dur, z_cost, z_dbus)

                if worst >= self.Z_WARNING:
                    reasons = []
                    if z_dur  >= self.Z_WARNING:
                        reasons.append(
                            f"Duration {dur:.1f}h is {z_dur:.1f}σ above baseline "
                            f"(median {b['p50_duration']:.1f}h)"
                        )
                    if z_cost >= self.Z_WARNING:
                        reasons.append(
                            f"Cost ${cost:.2f} is {z_cost:.1f}σ above baseline "
                            f"(median ${b['p50_cost']:.2f})"
                        )
                    if z_dbus >= self.Z_WARNING:
                        reasons.append(
                            f"DBUs {dbus:.2f} is {z_dbus:.1f}σ above baseline "
                            f"(median {b['p50_dbus']:.2f})"
                        )
                    detections.append({
                        "layer":       "baseline",
                        "reasons":     reasons,
                        "score":       round(min(worst / 5.0, 1.0), 3),
                        "max_z_score": round(worst, 2),
                    })

            # ── Layer 2: Isolation Forest ─────────────────────────────────────
            ifs = self._if_score(if_model, dbus, cost, dur, hour, dow, rtype)
            if ifs is not None and ifs >= 0.5:
                detections.append({
                    "layer":   "isolation_forest",
                    "reasons": [
                        "Unusual combination of cost rate, duration, and timing "
                        "relative to all resources in the training window"
                    ],
                    "score":   round(ifs, 3),
                })

            if not detections:
                continue

            ml_score = max(d["score"] for d in detections)
            severity = (
                "critical" if ml_score >= 0.7 else
                "warning"  if ml_score >= 0.4 else
                "info"
            )

            results.append({
                "resource_id":    rid,
                "service_type":   stype,
                "name":           res.get("name", ""),
                "workspace_id":   self._workspace_id,
                "workspace_name": res.get("workspace_name", ""),
                "environment":    res.get("environment", ""),
                "env_tier":       res.get("env_tier", ""),
                "started_at":     res.get("started_at", ""),
                "duration_hours": res.get("duration_hours"),
                "dbus":           res.get("dbus"),
                "cost_usd":       res.get("cost_usd"),
                "ml_score":       round(ml_score, 3),
                "severity":       severity,
                "detections":     detections,
                "baseline":       {
                    "session_count":         b["session_count"],
                    "median_duration_hours": round(b["p50_duration"], 2),
                    "median_daily_dbus":     round(b["p50_dbus"],     3),
                    "median_daily_cost_usd": round(b["p50_cost"],     3),
                } if b else None,
            })

        return sorted(results, key=lambda x: x["ml_score"], reverse=True)

    # ── main entry point ──────────────────────────────────────────────────────

    def get_ml_anomalies(self, running: list[dict]) -> dict:
        """Run all ML detection layers and return a structured result dict.

        The *running* list comes from CostService.get_anomalies() and contains
        currently active resources with their session info.

        Results are cached for CACHE_TTL_MIN minutes.  On a cache hit the
        heavy training queries are skipped and only the lightweight scoring
        pass is re-run against the fresh *running* list.
        """
        now = datetime.datetime.now(datetime.timezone.utc)

        # ── cache hit ─────────────────────────────────────────────────────────
        if self._cache and self._cached_at:
            age_min = (now - self._cached_at).total_seconds() / 60
            if age_min < self.CACHE_TTL_MIN:
                return {
                    **{k: v for k, v in self._cache.items() if not k.startswith("_")},
                    "generated_at":      now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "running_resources": len(running),
                    "cache_age_minutes": round(age_min, 1),
                    "all_running":       running,
                    "anomalies":         self._score_running(
                        running,
                        self._cache["_baseline"],
                        self._cache["_if_model"],
                    ),
                    "idle_resources":    self._detect_idle(
                        running,
                        self._cache["_last_billing"],
                    ),
                }

        # ── full computation ──────────────────────────────────────────────────
        try:
            history = self._fetch_history()
        except Exception:
            history = []
        baseline     = self._compute_baseline(history)
        if_model     = self._train_if(history)
        try:
            daily_cost = self._fetch_daily_cost()
        except Exception:
            daily_cost = []
        try:
            last_billing = self._fetch_last_billing()
        except Exception:
            last_billing = {}

        drift     = self._drift_alerts(daily_cost)
        idle      = self._detect_idle(running, last_billing)
        anomalies = self._score_running(running, baseline, if_model)

        # Build workspace_id → {name, environment} map from scored resources
        # so drift alerts (which only have workspace_id) can show names on hover.
        ws_info: dict[str, dict] = {}
        for a in anomalies:
            wid = a.get("workspace_id", "")
            if wid and wid not in ws_info:
                ws_info[wid] = {
                    "workspace_name": a.get("workspace_name", ""),
                    "environment":    a.get("environment", ""),
                }
        for alert in drift:
            wid  = alert.get("workspace_id", "")
            info = ws_info.get(wid, {})
            if info.get("workspace_name"):
                alert["workspace_name"] = info["workspace_name"]
            if info.get("environment"):
                alert["environment"] = info["environment"]
            # Fill subscription / RG from the account-level workspace map
            # (covers ALL workspaces in the billing data, not just the current one).
            az = self._ws_azure.get(wid, {})
            if not alert.get("subscription") and az.get("subscription"):
                alert["subscription"] = az["subscription"]
            if not alert.get("resource_group") and az.get("resource_group"):
                alert["resource_group"] = az["resource_group"]
            # Also use account API workspace name if not already resolved
            if not alert.get("workspace_name") and az.get("workspace_name"):
                alert["workspace_name"] = az["workspace_name"]
            # Resolve subscription ID to display name
            sub = alert.get("subscription") or ""
            if sub:
                alert["subscription_name"] = resolve_subscription(sub)

        result: dict = {
            "generated_at":              now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "training_window_days":      self.HISTORY_DAYS,
            "training_records":          len(history),
            "resources_with_baseline":   len(baseline),
            "isolation_forest_trained":  if_model is not None,
            "running_resources":         len(running),
            "cache_age_minutes":         0,
            "all_running":               running,
            "anomalies":                 anomalies,
            "drift_alerts":              drift,
            "idle_resources":            idle,
        }

        # Store training artefacts in cache under private keys
        self._cache = {
            **result,
            "_baseline":     baseline,
            "_if_model":     if_model,
            "_last_billing": last_billing,
        }
        self._cached_at = now

        return result
