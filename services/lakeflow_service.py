"""
Job SLA & Pipeline Health from system.lakeflow tables.

Provides executive-level visibility into:
  - Job success/failure rates and SLA compliance
  - Pipeline run durations and trends
  - Top failing jobs and longest-running pipelines
  - Job ownership and scheduling patterns
"""

from __future__ import annotations

import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from databricks.sdk import WorkspaceClient

from core.config import get_settings
from core.sql_executor import execute_sync, safe_execute_sync


class LakeflowService:
    LOOKBACK_DAYS = 30
    CACHE_TTL_MIN = 20

    def __init__(self, client: WorkspaceClient) -> None:
        self._client = client
        self._wh_id  = get_settings().databricks_warehouse_id
        self._cache: Optional[dict]                  = None
        self._cached_at: Optional[datetime.datetime] = None

    def _execute(self, sql: str) -> list[dict]:
        return execute_sync(self._client, self._wh_id, sql, label="Lakeflow")

    def _safe_execute(self, sql: str) -> list[dict]:
        return safe_execute_sync(self._client, self._wh_id, sql, label="Lakeflow")

    # ── queries ───────────────────────────────────────────────────────────────

    def _fetch_job_runs(self) -> list[dict]:
        """Job run summary for prod workspaces: success/failure counts, durations."""
        return self._safe_execute(f"""
            SELECT
                r.job_id,
                r.workspace_id,
                r.run_type,
                COUNT(*)                                                                                  AS total_runs,
                SUM(CASE WHEN r.result_state = 'SUCCEEDED' THEN 1 ELSE 0 END)                            AS success_count,
                SUM(CASE WHEN r.result_state IN ('FAILED','TIMED_OUT','CANCELLED','ERROR') THEN 1 ELSE 0 END) AS failed_count,
                ROUND(AVG(CASE WHEN r.result_state = 'SUCCEEDED'
                    THEN (UNIX_TIMESTAMP(r.period_end_time) - UNIX_TIMESTAMP(r.period_start_time)) / 60.0
                    ELSE NULL END), 1)                                                                    AS avg_duration_min,
                ROUND(MAX(CASE WHEN r.result_state = 'SUCCEEDED'
                    THEN (UNIX_TIMESTAMP(r.period_end_time) - UNIX_TIMESTAMP(r.period_start_time)) / 60.0
                    ELSE NULL END), 1)                                                                    AS max_duration_min,
                MAX(CAST(r.period_end_time AS STRING))                                                    AS last_run_at,
                MAX(r.result_state)                                                                       AS last_result
            FROM system.lakeflow.job_run_timeline r
            JOIN system.access.workspaces_latest w ON r.workspace_id = w.workspace_id
            WHERE r.period_start_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
              AND r.result_state IS NOT NULL
              AND LOWER(w.workspace_name) LIKE '%prod%'
            GROUP BY r.job_id, r.workspace_id, r.run_type
            ORDER BY failed_count DESC, total_runs DESC
            LIMIT 500
        """)

    def _fetch_daily_job_stats(self) -> list[dict]:
        """Daily job run success/failure trend for prod workspaces."""
        return self._safe_execute(f"""
            SELECT
                CAST(DATE(r.period_start_time) AS STRING)                                             AS date,
                COUNT(*)                                                                               AS total_runs,
                SUM(CASE WHEN r.result_state = 'SUCCEEDED' THEN 1 ELSE 0 END)                         AS success_count,
                SUM(CASE WHEN r.result_state IN ('FAILED','TIMED_OUT','CANCELLED','ERROR') THEN 1 ELSE 0 END) AS failed_count
            FROM system.lakeflow.job_run_timeline r
            JOIN system.access.workspaces_latest w ON r.workspace_id = w.workspace_id
            WHERE r.period_start_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
              AND r.result_state IS NOT NULL
              AND LOWER(w.workspace_name) LIKE '%prod%'
            GROUP BY DATE(r.period_start_time)
            ORDER BY 1
        """)

    def _fetch_top_failures(self) -> list[dict]:
        """Top 50 most recent prod job failures with error details."""
        return self._safe_execute(f"""
            SELECT
                r.job_id,
                r.run_id,
                r.workspace_id,
                r.result_state,
                CAST(r.period_start_time AS STRING)                                                   AS started_at,
                ROUND((UNIX_TIMESTAMP(r.period_end_time) - UNIX_TIMESTAMP(r.period_start_time)) / 60.0, 1) AS duration_min
            FROM system.lakeflow.job_run_timeline r
            JOIN system.access.workspaces_latest w ON r.workspace_id = w.workspace_id
            WHERE r.period_start_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
              AND r.result_state IN ('FAILED', 'TIMED_OUT', 'CANCELLED', 'ERROR')
              AND LOWER(w.workspace_name) LIKE '%prod%'
            ORDER BY r.period_start_time DESC
            LIMIT 50
        """)

    def _fetch_pipeline_runs(self) -> list[dict]:
        """DLT pipeline run summary for prod workspaces from pipeline_update_timeline."""
        return self._safe_execute(f"""
            SELECT
                r.pipeline_id,
                r.workspace_id,
                COUNT(DISTINCT r.update_id)                                                               AS total_runs,
                SUM(CASE WHEN r.result_state = 'COMPLETED' THEN 1 ELSE 0 END)                            AS success_count,
                SUM(CASE WHEN r.result_state IN ('FAILED','CANCELED') THEN 1 ELSE 0 END)                  AS failed_count,
                ROUND(AVG(CASE WHEN r.result_state = 'COMPLETED'
                    THEN (UNIX_TIMESTAMP(r.period_end_time) - UNIX_TIMESTAMP(r.period_start_time)) / 60.0
                    ELSE NULL END), 1)                                                                    AS avg_duration_min,
                MAX(CAST(r.period_end_time AS STRING))                                                    AS last_run_at
            FROM system.lakeflow.pipeline_update_timeline r
            JOIN system.access.workspaces_latest w ON r.workspace_id = w.workspace_id
            WHERE r.period_start_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
              AND r.period_end_time IS NOT NULL
              AND r.result_state IS NOT NULL
              AND LOWER(w.workspace_name) LIKE '%prod%'
            GROUP BY r.pipeline_id, r.workspace_id
            ORDER BY failed_count DESC
            LIMIT 200
        """)

    def _fetch_combined_sla(self) -> dict:
        """Combined job + DLT success rate for prod workspaces — both 7d and 30d windows.

        Uses identical logic to executive_service._fetch_job_health():
        SCD2 dedup, delete_time IS NULL, LEFT JOIN workspaces_latest LIKE '%prod%',
        COUNT(DISTINCT run_id / update_id).
        """
        d7  = (datetime.date.today() - datetime.timedelta(days=7)).isoformat()
        d30 = (datetime.date.today() - datetime.timedelta(days=30)).isoformat()

        def _run_jobs(since: str) -> tuple:
            rows = self._safe_execute(f"""
                WITH latest_jobs AS (
                    SELECT workspace_id, job_id, delete_time,
                           ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
                    FROM system.lakeflow.jobs
                    QUALIFY rn = 1
                )
                SELECT
                    COUNT(DISTINCT r.run_id)                                                                       AS total_runs,
                    SUM(CASE WHEN r.result_state = 'SUCCEEDED'                                    THEN 1 ELSE 0 END) AS succeeded,
                    SUM(CASE WHEN r.result_state IN ('FAILED','TIMED_OUT','CANCELLED','ERROR')    THEN 1 ELSE 0 END) AS failed
                FROM system.lakeflow.job_run_timeline r
                JOIN latest_jobs j USING (workspace_id, job_id)
                LEFT JOIN system.access.workspaces_latest w ON r.workspace_id = w.workspace_id
                WHERE r.period_start_time >= '{since}'
                  AND r.result_state IS NOT NULL
                  AND j.delete_time IS NULL
                  AND LOWER(w.workspace_name) LIKE '%prod%'
            """)
            r = rows[0] if rows else {}
            return int(r.get("total_runs") or 0), int(r.get("succeeded") or 0), int(r.get("failed") or 0)

        def _run_dlt(since: str) -> tuple:
            rows = self._safe_execute(f"""
                WITH latest_pipelines AS (
                    SELECT workspace_id, pipeline_id, delete_time,
                           ROW_NUMBER() OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time DESC) AS rn
                    FROM system.lakeflow.pipelines
                    QUALIFY rn = 1
                )
                SELECT
                    COUNT(DISTINCT r.update_id)                                                                    AS total_runs,
                    SUM(CASE WHEN r.result_state = 'COMPLETED'                        THEN 1 ELSE 0 END)           AS succeeded,
                    SUM(CASE WHEN r.result_state IN ('FAILED','CANCELED')             THEN 1 ELSE 0 END)           AS failed
                FROM system.lakeflow.pipeline_update_timeline r
                JOIN latest_pipelines p USING (workspace_id, pipeline_id)
                LEFT JOIN system.access.workspaces_latest w ON r.workspace_id = w.workspace_id
                WHERE r.period_start_time >= '{since}'
                  AND r.result_state IS NOT NULL
                  AND p.delete_time IS NULL
                  AND LOWER(w.workspace_name) LIKE '%prod%'
            """)
            r = rows[0] if rows else {}
            return int(r.get("total_runs") or 0), int(r.get("succeeded") or 0), int(r.get("failed") or 0)

        j7t,  j7s,  j7f  = _run_jobs(d7)
        d7t,  d7s,  d7f  = _run_dlt(d7)
        j30t, j30s, j30f = _run_jobs(d30)
        d30t, d30s, d30f = _run_dlt(d30)

        t7  = j7t  + d7t;   s7  = j7s  + d7s;   f7  = j7f  + d7f
        t30 = j30t + d30t;  s30 = j30s + d30s;  f30 = j30f + d30f

        return {
            "total_runs_7d":   t7,  "succeeded_7d":  s7,  "failed_7d":  f7,
            "sla_pct_7d":      round(s7  / t7  * 100, 1) if t7  else 0.0,
            "job_runs_7d":     j7t, "dlt_runs_7d":  d7t,
            "total_runs_30d":  t30, "succeeded_30d": s30, "failed_30d": f30,
            "sla_pct_30d":     round(s30 / t30 * 100, 1) if t30 else 0.0,
            "job_runs_30d":    j30t, "dlt_runs_30d": d30t,
        }

    def _fetch_workspace_names(self) -> list[dict]:
        """Workspace id-to-name map from system.access.workspaces_latest."""
        return self._safe_execute("""
            SELECT workspace_id, workspace_name
            FROM system.access.workspaces_latest
        """)

    def _fetch_jobs_inventory(self) -> list[dict]:
        """Prod job inventory (SCD2 latest version) with ownership and schedule."""
        return self._safe_execute("""
            WITH latest_jobs AS (
                SELECT job_id, name, creator_user_name, run_as_user_name,
                       workspace_id, created_time, schedule,
                       ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
                FROM system.lakeflow.jobs
                WHERE delete_time IS NULL
                QUALIFY rn = 1
            )
            SELECT
                j.job_id,
                j.name                          AS job_name,
                j.creator_user_name             AS creator,
                j.run_as_user_name              AS run_as,
                j.workspace_id,
                CAST(j.created_time AS STRING)  AS created_at,
                j.schedule.quartz_cron_expression AS cron_schedule,
                j.schedule.pause_status         AS schedule_status
            FROM latest_jobs j
            JOIN system.access.workspaces_latest w ON j.workspace_id = w.workspace_id
            WHERE LOWER(w.workspace_name) LIKE '%prod%'
            LIMIT 500
        """)

    def _fetch_job_tasks_summary(self) -> list[dict]:
        """Task count per job for prod workspaces from system.lakeflow.job_tasks."""
        return self._safe_execute("""
            SELECT
                t.job_id,
                COUNT(DISTINCT t.task_key) AS task_count
            FROM system.lakeflow.job_tasks t
            JOIN system.access.workspaces_latest w ON t.workspace_id = w.workspace_id
            WHERE t.delete_time IS NULL
              AND LOWER(w.workspace_name) LIKE '%prod%'
            GROUP BY t.job_id
        """)

    def _fetch_task_bottlenecks(self) -> list[dict]:
        """Slowest tasks in prod workspaces from system.lakeflow.job_task_run_timeline."""
        return self._safe_execute(f"""
            SELECT
                r.job_id,
                r.task_key,
                COUNT(*) AS run_count,
                SUM(CASE WHEN r.result_state = 'SUCCEEDED' THEN 1 ELSE 0 END)                            AS success_count,
                SUM(CASE WHEN r.result_state IN ('FAILED','TIMED_OUT','CANCELLED','ERROR') THEN 1 ELSE 0 END) AS failed_count,
                ROUND(AVG(
                    (UNIX_TIMESTAMP(r.period_end_time) - UNIX_TIMESTAMP(r.period_start_time)) / 60.0
                ), 1) AS avg_duration_min,
                ROUND(MAX(
                    (UNIX_TIMESTAMP(r.period_end_time) - UNIX_TIMESTAMP(r.period_start_time)) / 60.0
                ), 1) AS max_duration_min
            FROM system.lakeflow.job_task_run_timeline r
            JOIN system.access.workspaces_latest w ON r.workspace_id = w.workspace_id
            WHERE r.period_start_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
              AND r.period_end_time IS NOT NULL
              AND LOWER(w.workspace_name) LIKE '%prod%'
            GROUP BY r.job_id, r.task_key
            ORDER BY avg_duration_min DESC
            LIMIT 200
        """)

    def _fetch_pipelines_inventory(self) -> list[dict]:
        """Prod DLT pipeline inventory (SCD2 latest version) from system.lakeflow.pipelines."""
        return self._safe_execute("""
            WITH latest_pipelines AS (
                SELECT pipeline_id, name, workspace_id, creator_user_name, run_as_user_name,
                       channel, edition, created_time,
                       ROW_NUMBER() OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time DESC) AS rn
                FROM system.lakeflow.pipelines
                WHERE delete_time IS NULL
                QUALIFY rn = 1
            )
            SELECT
                p.pipeline_id,
                p.name              AS pipeline_name,
                p.workspace_id,
                p.creator_user_name AS creator,
                p.run_as_user_name  AS run_as,
                p.channel,
                p.edition,
                CAST(p.created_time AS STRING) AS created_at
            FROM latest_pipelines p
            JOIN system.access.workspaces_latest w ON p.workspace_id = w.workspace_id
            WHERE LOWER(w.workspace_name) LIKE '%prod%'
            LIMIT 300
        """)

    def _fetch_pipeline_update_timeline(self) -> list[dict]:
        """Pipeline update durations for prod workspaces from pipeline_update_timeline."""
        return self._safe_execute(f"""
            SELECT
                r.pipeline_id,
                r.update_id,
                r.workspace_id,
                r.result_state                                                                        AS state,
                ROUND(
                    (UNIX_TIMESTAMP(r.period_end_time) - UNIX_TIMESTAMP(r.period_start_time)) / 60.0
                , 1)                                                                                  AS duration_min,
                CAST(r.period_start_time AS STRING)                                                   AS started_at,
                CAST(r.period_end_time   AS STRING)                                                   AS ended_at
            FROM system.lakeflow.pipeline_update_timeline r
            JOIN system.access.workspaces_latest w ON r.workspace_id = w.workspace_id
            WHERE r.period_start_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
              AND r.period_end_time IS NOT NULL
              AND r.result_state IS NOT NULL
              AND LOWER(w.workspace_name) LIKE '%prod%'
            ORDER BY r.period_start_time DESC
            LIMIT 300
        """)

    # ── analysis ──────────────────────────────────────────────────────────────

    def _analyse(self, job_runs, daily_stats, top_failures, pipeline_runs,
                 jobs_inv=None, task_summary=None, task_bottlenecks=None,
                 pipe_inv=None, pipe_updates=None, ws_names=None) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)

        def _int(v):
            try: return int(v) if v else 0
            except: return 0
        def _float(v):
            try: return float(v) if v else 0.0
            except: return 0.0

        # ── Workspace id → name map ──────────────────────────────────────────
        ws_map: dict[str, str] = {}
        for r in (ws_names or []):
            wid = str(r.get("workspace_id", ""))
            if wid:
                ws_map[wid] = r.get("workspace_name", "")

        total_runs = sum(_int(r.get("total_runs")) for r in job_runs)
        total_success = sum(_int(r.get("success_count")) for r in job_runs)
        total_failed = sum(_int(r.get("failed_count")) for r in job_runs)
        sla_pct = round((total_success / total_runs * 100), 1) if total_runs > 0 else 0

        jobs = []
        for r in job_runs:
            total = _int(r.get("total_runs"))
            success = _int(r.get("success_count"))
            failed = _int(r.get("failed_count"))
            rate = round((success / total * 100), 1) if total > 0 else 0
            jobs.append({
                "job_id":          r.get("job_id", ""),
                "job_name":        r.get("job_name", ""),
                "workspace_id":    r.get("workspace_id", ""),
                "workspace_name":  ws_map.get(str(r.get("workspace_id", "")), ""),
                "run_type":        r.get("run_type", ""),
                "total_runs":      total,
                "success_count":   success,
                "failed_count":    failed,
                "success_rate":    rate,
                "avg_duration_min": _float(r.get("avg_duration_min")),
                "max_duration_min": _float(r.get("max_duration_min")),
                "last_run_at":     r.get("last_run_at", ""),
                "last_result":     r.get("last_result", ""),
            })

        # ── Jobs inventory enrichment ─────────────────────────────────────────
        inv_map = {}
        for r in (jobs_inv or []):
            jid = r.get("job_id", "")
            if jid:
                inv_map[jid] = {
                    "job_name":        r.get("job_name", ""),
                    "creator":         r.get("creator", ""),
                    "run_as":          r.get("run_as", ""),
                    "cron_schedule":   r.get("cron_schedule", ""),
                    "schedule_status": r.get("schedule_status", ""),
                    "created_at":      r.get("created_at", ""),
                    "workspace_id":    r.get("workspace_id", ""),
                }

        failures = [{
            "job_id":       r.get("job_id", ""),
            "job_name":     inv_map.get(r.get("job_id", ""), {}).get("job_name", ""),
            "run_id":       r.get("run_id", ""),
            "workspace_id": r.get("workspace_id", ""),
            "workspace_name": ws_map.get(str(r.get("workspace_id", "")), ""),
            "result_state": r.get("result_state", ""),
            "started_at":   r.get("started_at", ""),
            "duration_min": _float(r.get("duration_min")),
        } for r in top_failures]

        # ── Pipeline inventory map (for name enrichment) ────────────────────
        pipe_map: dict = {}
        for r in (pipe_inv or []):
            pid = r.get("pipeline_id", "")
            if pid:
                pipe_map[pid] = {
                    "pipeline_name": r.get("pipeline_name", ""),
                    "creator":       r.get("creator", ""),
                    "run_as":        r.get("run_as", ""),
                    "channel":       r.get("channel", ""),
                    "edition":       r.get("edition", ""),
                    "workspace_id":  r.get("workspace_id", ""),
                    "created_at":    r.get("created_at", ""),
                }

        pipelines = []
        for r in pipeline_runs:
            total = _int(r.get("total_runs"))
            success = _int(r.get("success_count"))
            failed = _int(r.get("failed_count"))
            pid = r.get("pipeline_id", "")
            pinv = pipe_map.get(pid, {})
            wid = str(r.get("workspace_id", ""))
            pipelines.append({
                "pipeline_id":     pid,
                "pipeline_name":   pinv.get("pipeline_name", ""),
                "workspace_id":    wid,
                "workspace_name":  ws_map.get(wid, ""),
                "total_runs":      total,
                "success_count":   success,
                "failed_count":    failed,
                "success_rate":    round((success / total * 100), 1) if total > 0 else 0,
                "avg_duration_min": _float(r.get("avg_duration_min")),
                "last_run_at":     r.get("last_run_at", ""),
            })

        daily = [{
            "date":          r.get("date", ""),
            "total_runs":    _int(r.get("total_runs")),
            "success_count": _int(r.get("success_count")),
            "failed_count":  _int(r.get("failed_count")),
        } for r in daily_stats]

        # Task count per job
        task_cnt_map = {}
        for r in (task_summary or []):
            jid = r.get("job_id", "")
            if jid:
                task_cnt_map[jid] = _int(r.get("task_count"))

        # Enrich jobs with inventory + task count
        for j in jobs:
            inv = inv_map.get(j["job_id"], {})
            j["job_name"]        = inv.get("job_name", j.get("job_name", ""))
            j["creator"]         = inv.get("creator", "")
            j["run_as"]          = inv.get("run_as", "")
            j["cron_schedule"]   = inv.get("cron_schedule", "")
            j["schedule_status"] = inv.get("schedule_status", "")
            j["created_at"]      = inv.get("created_at", "")
            j["task_count"]      = task_cnt_map.get(j["job_id"], 0)

        # Task bottlenecks (top slowest tasks)
        bottlenecks = []
        for r in (task_bottlenecks or []):
            jid = r.get("job_id", "")
            jinv = inv_map.get(jid, {})
            wid = jinv.get("workspace_id", "")
            bottlenecks.append({
                "job_id":          jid,
                "job_name":        jinv.get("job_name", ""),
                "workspace_name":  ws_map.get(str(wid), ""),
                "task_key":        r.get("task_key", ""),
                "run_count":       _int(r.get("run_count")),
                "success_count":   _int(r.get("success_count")),
                "failed_count":    _int(r.get("failed_count")),
                "avg_duration_min": _float(r.get("avg_duration_min")),
                "max_duration_min": _float(r.get("max_duration_min")),
            })

        # ── Pipeline inventory ────────────────────────────────────────────────
        pipe_inv_list = [{
            "pipeline_id":   r.get("pipeline_id", ""),
            "pipeline_name": r.get("pipeline_name", ""),
            "workspace_id":  r.get("workspace_id", ""),
            "workspace_name": ws_map.get(str(r.get("workspace_id", "")), ""),
            "creator":       r.get("creator", ""),
            "run_as":        r.get("run_as", ""),
            "channel":       r.get("channel", ""),
            "edition":       r.get("edition", ""),
            "created_at":    r.get("created_at", ""),
        } for r in (pipe_inv or [])]

        # Pipeline update durations
        updates_list = []
        for r in (pipe_updates or []):
            pid = r.get("pipeline_id", "")
            pinv = pipe_map.get(pid, {})
            wid = str(r.get("workspace_id", pinv.get("workspace_id", "")))
            updates_list.append({
                "pipeline_id":    pid,
                "pipeline_name":  pinv.get("pipeline_name", ""),
                "workspace_name": ws_map.get(wid, ""),
                "update_id":      r.get("update_id", ""),
                "state":          r.get("state", ""),
                "duration_min":   _float(r.get("duration_min")),
                "started_at":     r.get("started_at", ""),
            })

        return {
            "generated_at":   now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lookback_days":  self.LOOKBACK_DAYS,
            "summary": {
                "total_jobs":    len(jobs),
                "total_runs":    total_runs,
                "total_success": total_success,
                "total_failed":  total_failed,
                "sla_pct":       sla_pct,
                "pipelines":     len(pipelines),
                "task_bottlenecks": len(bottlenecks),
                "pipeline_inventory": len(pipe_inv_list),
            },
            "daily_trend":         daily,
            "jobs":                jobs,
            "top_failures":        failures,
            "pipelines":           pipelines,
            "task_bottlenecks":    bottlenecks,
            "pipeline_inventory":  pipe_inv_list,
            "pipeline_updates":    updates_list,
        }

    def get_job_insights(self) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)
        if self._cache and self._cached_at:
            age = (now - self._cached_at).total_seconds() / 60
            if age < self.CACHE_TTL_MIN:
                return {**self._cache, "cache_age_minutes": round(age, 1)}

        with ThreadPoolExecutor(max_workers=11) as pool:
            f_runs    = pool.submit(self._fetch_job_runs)
            f_daily   = pool.submit(self._fetch_daily_job_stats)
            f_fail    = pool.submit(self._fetch_top_failures)
            f_pipe    = pool.submit(self._fetch_pipeline_runs)
            f_jinv    = pool.submit(self._fetch_jobs_inventory)
            f_task    = pool.submit(self._fetch_job_tasks_summary)
            f_bots    = pool.submit(self._fetch_task_bottlenecks)
            f_pinv    = pool.submit(self._fetch_pipelines_inventory)
            f_pupd    = pool.submit(self._fetch_pipeline_update_timeline)
            f_ws      = pool.submit(self._fetch_workspace_names)
            f_sla7    = pool.submit(self._fetch_combined_sla)

        def _safe_result(future, default=None):
            try:
                return future.result()
            except Exception:
                return default if default is not None else []

        job_runs      = _safe_result(f_runs)
        daily_stats   = _safe_result(f_daily)
        top_failures  = _safe_result(f_fail)
        pipeline_runs = _safe_result(f_pipe)
        jobs_inv      = _safe_result(f_jinv)
        task_summary  = _safe_result(f_task)
        task_bots     = _safe_result(f_bots)
        pipe_inv      = _safe_result(f_pinv)
        pipe_updates  = _safe_result(f_pupd)
        ws_names      = _safe_result(f_ws, {})
        sla_7d        = _safe_result(f_sla7)

        result = self._analyse(
            job_runs, daily_stats, top_failures, pipeline_runs,
            jobs_inv, task_summary, task_bots, pipe_inv, pipe_updates, ws_names,
        )
        result["sla_7d"] = sla_7d
        result["cache_age_minutes"] = 0
        self._cache = result
        self._cached_at = now
        return result
