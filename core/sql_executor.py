"""
Shared async SQL executor with retry + exponential backoff.

All services delegate SQL execution here so that:
  1. Queries run in a thread-pool via asyncio.to_thread()  (Fix #1)
  2. Transient failures retry with exponential back-off    (Fix #4)
  3. Safe-execute silently returns [] for missing tables
  4. PENDING/RUNNING states are polled (not re-submitted)  (Fix #6)
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import re
import time
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

_log = logging.getLogger("sql_executor")

# ---------------------------------------------------------------------------
# Mock-mode table rewriter
# When MOCK_MODE=true, replace system catalog references with mock tables
# so the app can be tested on free-tier workspaces without system table access.
# ---------------------------------------------------------------------------
_MOCK_MODE_RAW = os.getenv("MOCK_MODE", "false")
_MOCK_MODE = _MOCK_MODE_RAW.lower() == "true"
_log.info("MOCK_MODE env=%r  active=%s", _MOCK_MODE_RAW, _MOCK_MODE)

_MOCK_TABLE_MAP = {
    r"\bsystem\.billing\b":             "workspace.mock_system_billing",
    r"\bsystem\.access\b":              "workspace.mock_system_access",
    r"\bsystem\.compute\b":             "workspace.mock_system_compute",
    r"\bsystem\.lakeflow\b":            "workspace.mock_system_lakeflow",
    r"\bsystem\.query\b":               "workspace.mock_system_query",
    r"\bsystem\.ai_gateway\b":          "workspace.mock_system_ai_gateway",
    r"\bsystem\.serving\b":             "workspace.mock_system_serving",
    r"\bsystem\.mlflow\b":              "workspace.mock_system_mlflow",
    r"\bsystem\.storage\b":             "workspace.mock_system_storage",
    r"\bsystem\.information_schema\b":  "workspace.mock_system_information_schema",
    r"\bsystem\.networking\b":          "workspace.mock_system_networking",
    r"\bsystem\.lakeview\b":            "workspace.mock_system_lakeview",
    r"\bsystem\.dashboards\b":          "workspace.mock_system_dashboards",
    r"\bsystem\.marketplace\b":         "workspace.mock_system_marketplace",
}


def _rewrite_sql(sql: str) -> str:
    if not _MOCK_MODE:
        return sql
    for pattern, replacement in _MOCK_TABLE_MAP.items():
        sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
    return sql

# Retry configuration
MAX_RETRIES = 3
INITIAL_BACKOFF_S = 1.0
MAX_BACKOFF_S = 8.0

# How long to keep polling a PENDING/RUNNING statement before giving up
_POLL_MAX_WAIT_S  = 300
_POLL_INTERVAL_S  = 3

# Errors that are safe to retry (transient / warehouse cold-start)
_RETRYABLE_FRAGMENTS = (
    "TEMPORARILY_UNAVAILABLE",
    "RESOURCE_EXHAUSTED",
    "Connection reset",
    "timeout",
    "Timed out",
    "429",
    "503",
    "502",
    "WAREHOUSE_NOT_RUNNING",
    "Warehouse is starting",
)

# Errors that indicate a missing table or no access — safe to swallow
_MISSING_TABLE_FRAGMENTS = (
    "TABLE_OR_VIEW_NOT_FOUND",
    "SCHEMA_NOT_FOUND",
    "UNRESOLVED_COLUMN",
    "INSUFFICIENT_PERMISSIONS",
    "PERMISSION_DENIED",
)

_IN_PROGRESS_STATES = {StatementState.PENDING, StatementState.RUNNING}


def _poll_statement(client: WorkspaceClient, statement_id: str, label: str):
    """Poll an already-submitted statement until it leaves PENDING/RUNNING."""
    deadline = time.time() + _POLL_MAX_WAIT_S
    while time.time() < deadline:
        time.sleep(_POLL_INTERVAL_S)
        resp = client.statement_execution.get_statement(statement_id=statement_id)
        if resp.status.state not in _IN_PROGRESS_STATES:
            return resp
        _log.debug("%s polling statement_id=%s state=%s", label, statement_id, resp.status.state)
    raise RuntimeError(
        f"{label} query still {StatementState.PENDING} after {_POLL_MAX_WAIT_S}s — "
        "consider optimising the query or adding a date partition filter"
    )


def _extract_rows(resp) -> list[dict]:
    # DDL/DML statements (MERGE, DELETE, CREATE TABLE) succeed but return no result set
    if not resp.manifest or not resp.manifest.schema or not resp.manifest.schema.columns:
        return []
    cols = [c.name for c in resp.manifest.schema.columns]
    return [
        {k: (str(v) if v is not None else None) for k, v in zip(cols, row)}
        for row in (resp.result.data_array or [])
    ]


def execute_sync(
    client: WorkspaceClient,
    warehouse_id: str,
    sql: str,
    *,
    wait_timeout: str = "50s",
    label: str = "SQL",
) -> list[dict]:
    """Execute a SQL statement synchronously with retry + exponential backoff.

    When the warehouse returns PENDING or RUNNING (query still executing after
    wait_timeout), the existing statement is polled via get_statement() rather
    than re-submitted — avoiding duplicate query load on the warehouse.

    This is the blocking version — callers in async context should call
    ``execute_async()`` instead.
    """
    sql = _rewrite_sql(sql)
    last_exc: Optional[Exception] = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=sql,
                wait_timeout=wait_timeout,
            )
            state = resp.status.state

            # Query still running — poll the existing statement instead of re-submitting
            if state in _IN_PROGRESS_STATES:
                _log.info(
                    "%s statement still %s after %s — polling (id=%s)",
                    label, state, wait_timeout, resp.statement_id,
                )
                resp  = _poll_statement(client, resp.statement_id, label)
                state = resp.status.state

            if state == StatementState.SUCCEEDED:
                return _extract_rows(resp)

            # Non-success state — build error message
            err_obj = getattr(resp.status, "error", None)
            msg = getattr(err_obj, "message", None) or str(state)
            last_exc = RuntimeError(f"{label} query failed: {msg}")

            if any(frag.lower() in msg.lower() for frag in _RETRYABLE_FRAGMENTS):
                _backoff(attempt)
                continue
            raise last_exc
        except RuntimeError:
            raise
        except Exception as exc:
            last_exc = exc
            exc_str = str(exc)
            if any(frag.lower() in exc_str.lower() for frag in _RETRYABLE_FRAGMENTS):
                _backoff(attempt)
                continue
            raise RuntimeError(f"{label} query failed: {exc_str}") from exc

    # Exhausted retries
    raise last_exc or RuntimeError(f"{label} query failed after {MAX_RETRIES} retries")


def safe_execute_sync(
    client: WorkspaceClient,
    warehouse_id: str,
    sql: str,
    *,
    wait_timeout: str = "50s",
    label: str = "SQL",
) -> list[dict]:
    """Like execute_sync but returns [] if the table/schema doesn't exist."""
    try:
        return execute_sync(client, warehouse_id, sql, wait_timeout=wait_timeout, label=label)
    except RuntimeError as exc:
        if any(k in str(exc) for k in _MISSING_TABLE_FRAGMENTS):
            return []
        raise


async def execute_async(
    client: WorkspaceClient,
    warehouse_id: str,
    sql: str,
    *,
    wait_timeout: str = "50s",
    label: str = "SQL",
) -> list[dict]:
    """Non-blocking wrapper — runs execute_sync in a thread-pool."""
    return await asyncio.to_thread(
        execute_sync, client, warehouse_id, sql,
        wait_timeout=wait_timeout, label=label,
    )


async def safe_execute_async(
    client: WorkspaceClient,
    warehouse_id: str,
    sql: str,
    *,
    wait_timeout: str = "50s",
    label: str = "SQL",
) -> list[dict]:
    """Non-blocking safe wrapper — returns [] for missing tables."""
    return await asyncio.to_thread(
        safe_execute_sync, client, warehouse_id, sql,
        wait_timeout=wait_timeout, label=label,
    )


def _backoff(attempt: int) -> None:
    """Sleep with jittered exponential backoff."""
    delay = min(INITIAL_BACKOFF_S * (2 ** attempt), MAX_BACKOFF_S)
    delay *= 0.5 + random.random() * 0.5  # jitter
    _log.warning("SQL retry attempt %d/%d — backing off %.1fs", attempt + 1, MAX_RETRIES, delay)
    time.sleep(delay)
