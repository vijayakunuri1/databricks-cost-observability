"""
Data Lineage, Model Serving Health, and MLflow Experiments.

Queries system.access (lineage), system.serving, and system.mlflow.
Falls back to SDK APIs when system tables are unavailable.
"""

from __future__ import annotations

import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from databricks.sdk import WorkspaceClient

from core.config import get_settings
from core.sql_executor import execute_sync, safe_execute_sync

_log = logging.getLogger(__name__)


class LineageService:
    CACHE_TTL_MIN = 30

    def __init__(self, client: WorkspaceClient) -> None:
        self._client = client
        self._wh_id  = get_settings().databricks_warehouse_id
        self._cache: Optional[dict]                  = None
        self._cached_at: Optional[datetime.datetime] = None

    def _execute(self, sql: str) -> list[dict]:
        return execute_sync(self._client, self._wh_id, sql, label="Lineage")

    def _safe_execute(self, sql: str) -> list[dict]:
        return safe_execute_sync(self._client, self._wh_id, sql, label="Lineage")

    # ── Data Lineage ──────────────────────────────────────────────────────────

    def _fetch_table_lineage(self) -> list[dict]:
        return self._safe_execute("""
            SELECT
                source_table_catalog,
                source_table_schema,
                source_table_name,
                target_table_catalog,
                target_table_schema,
                target_table_name,
                CAST(event_time AS STRING) AS event_time
            FROM system.access.table_lineage
            WHERE event_time >= timestamp(date_add(current_date(), -30))
            ORDER BY event_time DESC
            LIMIT 5000
        """)

    def _fetch_column_lineage(self) -> list[dict]:
        return self._safe_execute("""
            SELECT
                source_table_catalog,
                source_table_schema,
                source_table_name,
                source_column_name,
                target_table_catalog,
                target_table_schema,
                target_table_name,
                target_column_name,
                CAST(event_time AS STRING) AS event_time
            FROM system.access.column_lineage
            WHERE event_time >= timestamp(date_add(current_date(), -30))
            ORDER BY event_time DESC
            LIMIT 5000
        """)

    # ── Model Serving ─────────────────────────────────────────────────────────

    def _fetch_serving_endpoints(self) -> list[dict]:
        rows = self._safe_execute("""
            SELECT
                endpoint_name,
                served_entity_name AS entity_name,
                entity_type,
                workspace_id,
                CAST(change_time AS STRING) AS updated_at
            FROM system.serving.served_entities
            ORDER BY endpoint_name
            LIMIT 200
        """)
        if rows:
            return rows
        # SDK fallback
        try:
            out = []
            for ep in self._client.serving_endpoints.list():
                name = getattr(ep, "name", "")
                state = getattr(ep, "state", None)
                state_str = str(getattr(state, "ready", "")) if state else ""
                created = getattr(ep, "creation_timestamp", 0) or 0
                updated = getattr(ep, "last_updated_timestamp", 0) or 0
                out.append({
                    "endpoint_name": name,
                    "entity_name": "",
                    "entity_type": "",
                    "workspace_id": "",
                    "updated_at": datetime.datetime.fromtimestamp(
                        updated / 1000, tz=datetime.timezone.utc
                    ).strftime("%Y-%m-%d %H:%M") if updated else "",
                    "_state": state_str,
                    "_created": datetime.datetime.fromtimestamp(
                        created / 1000, tz=datetime.timezone.utc
                    ).strftime("%Y-%m-%d %H:%M") if created else "",
                })
            return out
        except Exception as e:
            _log.debug("SDK serving_endpoints.list failed: %s", e)
            return []

    def _fetch_serving_usage(self) -> list[dict]:
        return self._safe_execute("""
            SELECT
                served_entity_name   AS endpoint_name,
                CAST(DATE(request_time) AS STRING) AS date,
                COUNT(*)             AS request_count,
                ROUND(AVG(total_token_count), 1) AS avg_tokens,
                SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS error_count
            FROM system.serving.endpoint_usage
            WHERE request_time >= timestamp(date_add(current_date(), -30))
            GROUP BY served_entity_name, DATE(request_time)
            ORDER BY date, request_count DESC
            LIMIT 2000
        """)

    # ── MLflow ────────────────────────────────────────────────────────────────

    def _fetch_mlflow_experiments(self) -> list[dict]:
        rows = self._safe_execute("""
            SELECT
                experiment_id,
                name              AS experiment_name,
                lifecycle_stage,
                CAST(creation_time AS STRING) AS created_at,
                CAST(last_update_time AS STRING) AS updated_at
            FROM system.mlflow.experiments_latest
            ORDER BY last_update_time DESC
            LIMIT 200
        """)
        if rows:
            return rows
        # SDK fallback
        try:
            import mlflow
            out = []
            for exp in mlflow.search_experiments(max_results=200):
                out.append({
                    "experiment_id":   exp.experiment_id,
                    "experiment_name": exp.name,
                    "lifecycle_stage": exp.lifecycle_stage,
                    "created_at":      datetime.datetime.fromtimestamp(
                        (exp.creation_time or 0) / 1000, tz=datetime.timezone.utc
                    ).strftime("%Y-%m-%d %H:%M") if exp.creation_time else "",
                    "updated_at":      datetime.datetime.fromtimestamp(
                        (exp.last_update_time or 0) / 1000, tz=datetime.timezone.utc
                    ).strftime("%Y-%m-%d %H:%M") if exp.last_update_time else "",
                })
            return out
        except Exception as e:
            _log.debug("MLflow experiments SDK fallback failed: %s", e)
            return []

    def _fetch_mlflow_runs(self) -> list[dict]:
        return self._safe_execute("""
            SELECT
                experiment_id,
                run_id,
                status,
                CAST(start_time AS STRING) AS started_at,
                CAST(end_time AS STRING)   AS ended_at,
                user_id AS user_email
            FROM system.mlflow.runs_latest
            WHERE start_time >= timestamp(date_add(current_date(), -30))
            ORDER BY start_time DESC
            LIMIT 2000
        """)

    def _fetch_mlflow_models(self) -> list[dict]:
        rows = self._safe_execute("""
            SELECT
                name AS model_name,
                CAST(creation_timestamp AS STRING) AS created_at,
                CAST(last_updated_timestamp AS STRING) AS updated_at,
                user_id AS owner
            FROM system.mlflow.registered_models_latest
            ORDER BY last_updated_timestamp DESC
            LIMIT 200
        """)
        if rows:
            return rows
        # SDK fallback — Unity Catalog models
        try:
            out = []
            for m in self._client.registered_models.list(max_results=200):
                name = getattr(m, "name", "") or getattr(m, "full_name", "")
                owner = getattr(m, "owner", "")
                created = getattr(m, "creation_timestamp", 0) or 0
                updated = getattr(m, "last_updated_timestamp", 0) or getattr(m, "updated_at", 0) or 0
                out.append({
                    "model_name": name,
                    "created_at": datetime.datetime.fromtimestamp(
                        created / 1000, tz=datetime.timezone.utc
                    ).strftime("%Y-%m-%d %H:%M") if created else "",
                    "updated_at": datetime.datetime.fromtimestamp(
                        updated / 1000, tz=datetime.timezone.utc
                    ).strftime("%Y-%m-%d %H:%M") if updated else "",
                    "owner": owner,
                })
            return out
        except Exception as e:
            _log.debug("SDK registered_models.list failed: %s", e)
            return []

    # ── analysis ──────────────────────────────────────────────────────────────

    def _analyse(self, table_lineage, column_lineage, endpoints, serving_usage,
                 experiments, ml_runs, ml_models) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)

        def _int(v):
            try: return int(v) if v else 0
            except: return 0

        # Lineage graph edges
        lineage_edges = []
        seen_edges = set()
        for r in table_lineage:
            src = f"{r.get('source_table_catalog','')}.{r.get('source_table_schema','')}.{r.get('source_table_name','')}"
            tgt = f"{r.get('target_table_catalog','')}.{r.get('target_table_schema','')}.{r.get('target_table_name','')}"
            key = f"{src}→{tgt}"
            if key not in seen_edges:
                seen_edges.add(key)
                lineage_edges.append({"source": src, "target": tgt})

        # Unique tables in lineage
        lineage_tables = set()
        for e in lineage_edges:
            lineage_tables.add(e["source"])
            lineage_tables.add(e["target"])

        # Serving endpoint summary — merge entities + usage
        ep_map: dict = {}
        for r in serving_usage:
            name = r.get("endpoint_name", "")
            ep_map.setdefault(name, {"requests": 0, "errors": 0, "days": 0, "tokens_sum": 0, "tokens_n": 0})
            ep_map[name]["requests"] += _int(r.get("request_count"))
            ep_map[name]["errors"]   += _int(r.get("error_count"))
            ep_map[name]["days"]     += 1
            tok = float(r.get("avg_tokens") or 0)
            if tok > 0:
                ep_map[name]["tokens_sum"] += tok * _int(r.get("request_count"))
                ep_map[name]["tokens_n"]   += _int(r.get("request_count"))

        # Include endpoints that have no usage yet
        for r in endpoints:
            name = r.get("endpoint_name", "")
            if name and name not in ep_map:
                ep_map[name] = {"requests": 0, "errors": 0, "days": 0, "tokens_sum": 0, "tokens_n": 0,
                                "updated_at": r.get("updated_at", ""),
                                "state": r.get("_state", "")}

        serving_summary = sorted([
            {
                "endpoint_name": k,
                "total_requests": v["requests"],
                "total_errors":   v["errors"],
                "error_rate":     round(v["errors"] / v["requests"] * 100, 1) if v["requests"] > 0 else 0,
                "active_days":    v["days"],
                "avg_tokens":     round(v["tokens_sum"] / v["tokens_n"], 1) if v.get("tokens_n", 0) > 0 else 0,
            }
            for k, v in ep_map.items()
        ], key=lambda x: -x["total_requests"])

        # MLflow summary
        run_count = len(ml_runs)
        run_success = sum(1 for r in ml_runs if r.get("status") == "FINISHED")
        run_failed  = sum(1 for r in ml_runs if r.get("status") == "FAILED")

        return {
            "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "summary": {
                "lineage_tables":    len(lineage_tables),
                "lineage_edges":     len(lineage_edges),
                "column_lineage":    len(column_lineage),
                "serving_endpoints": len(ep_map),
                "serving_requests":  sum(v["requests"] for v in ep_map.values()),
                "ml_experiments":    len(experiments),
                "ml_runs":           run_count,
                "ml_models":         len(ml_models),
            },
            "lineage_edges":    lineage_edges[:500],
            "serving_endpoints": serving_summary,
            "serving_daily":    [{
                "endpoint_name": r.get("endpoint_name", ""),
                "date":          r.get("date", ""),
                "requests":      _int(r.get("request_count")),
                "errors":        _int(r.get("error_count")),
                "avg_tokens":    float(r.get("avg_tokens") or 0),
            } for r in serving_usage],
            "ml_experiments":   [{
                "id":     r.get("experiment_id", ""),
                "name":   r.get("experiment_name", ""),
                "stage":  r.get("lifecycle_stage", ""),
                "created": r.get("created_at", ""),
                "updated": r.get("updated_at", ""),
            } for r in experiments],
            "ml_runs_summary": {
                "total": run_count,
                "success": run_success,
                "failed":  run_failed,
                "success_rate": round(run_success / run_count * 100, 1) if run_count > 0 else 0,
            },
            "ml_models": [{
                "name":    r.get("model_name", ""),
                "owner":   r.get("owner", ""),
                "created": r.get("created_at", ""),
                "updated": r.get("updated_at", ""),
            } for r in ml_models],
        }

    def get_lineage_insights(self) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)
        if self._cache and self._cached_at:
            age = (now - self._cached_at).total_seconds() / 60
            if age < self.CACHE_TTL_MIN:
                return {**self._cache, "cache_age_minutes": round(age, 1)}

        with ThreadPoolExecutor(max_workers=7) as pool:
            f_tlin  = pool.submit(self._fetch_table_lineage)
            f_clin  = pool.submit(self._fetch_column_lineage)
            f_ep    = pool.submit(self._fetch_serving_endpoints)
            f_su    = pool.submit(self._fetch_serving_usage)
            f_exp   = pool.submit(self._fetch_mlflow_experiments)
            f_runs  = pool.submit(self._fetch_mlflow_runs)
            f_mdls  = pool.submit(self._fetch_mlflow_models)

        def _safe_result(future, default=None):
            try:
                return future.result()
            except Exception:
                return default if default is not None else []

        table_lineage  = _safe_result(f_tlin)
        column_lineage = _safe_result(f_clin)
        endpoints      = _safe_result(f_ep)
        serving_usage  = _safe_result(f_su)
        experiments    = _safe_result(f_exp)
        ml_runs        = _safe_result(f_runs)
        ml_models      = _safe_result(f_mdls)

        result = self._analyse(
            table_lineage, column_lineage, endpoints, serving_usage,
            experiments, ml_runs, ml_models,
        )
        result["cache_age_minutes"] = 0
        self._cache = result
        self._cached_at = now
        return result
