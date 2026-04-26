"""Delta-backed per-user tab/workspace/tag permission store.

Permissions are stored in `<catalog>.default.app_user_permissions` as a single
JSON config column so the schema never needs migration when new filter types
are added.
"""

import json
import logging
import re
import threading
from typing import Optional

from databricks.sdk import WorkspaceClient

from core import sql_executor

_log = logging.getLogger("user_permissions")


def _esc_id(s: str) -> str:
    """Sanitize a short identifier (email, username) for SQL string literals."""
    return re.sub(r"[\x00-\x1f\x7f]", "", str(s))[:512].replace("'", "''")


def _esc_json(s: str) -> str:
    """Escape a JSON string for use inside a SQL single-quoted string literal."""
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", str(s)).replace("'", "''")


class UserPermissionsService:
    def __init__(self, client: WorkspaceClient, catalog: str, warehouse_id: str):
        self._client = client
        self._warehouse_id = warehouse_id
        self._table = f"`{catalog}`.`default`.`app_user_permissions`"

    def _ensure_table(self) -> None:
        sql_executor.execute_sync(
            self._client,
            self._warehouse_id,
            f"""CREATE TABLE IF NOT EXISTS {self._table} (
                email      STRING NOT NULL,
                config     STRING NOT NULL,
                updated_at TIMESTAMP,
                updated_by STRING
            ) USING DELTA""",
            label="ensure_user_perms_table",
        )

    def get_all(self) -> list[dict]:
        rows = sql_executor.safe_execute_sync(
            self._client,
            self._warehouse_id,
            f"SELECT email, config, CAST(updated_at AS STRING) AS updated_at, updated_by "
            f"FROM {self._table} ORDER BY email",
            label="list_user_configs",
        )
        result = []
        for r in rows:
            try:
                cfg = json.loads(r.get("config") or "{}")
            except Exception:
                cfg = {}
            result.append({
                "email": r.get("email"),
                "config": cfg,
                "updated_at": r.get("updated_at"),
                "updated_by": r.get("updated_by"),
            })
        return result

    def get_for_user(self, email: str) -> Optional[dict]:
        rows = sql_executor.safe_execute_sync(
            self._client,
            self._warehouse_id,
            f"SELECT config FROM {self._table} "
            f"WHERE lower(email) = lower('{_esc_id(email)}')",
            label="get_user_config",
        )
        if not rows:
            return None
        try:
            return json.loads(rows[0].get("config") or "{}")
        except Exception:
            return None

    def upsert(self, email: str, config: dict, updated_by: str) -> None:
        self._ensure_table()  # idempotent — creates table if it didn't exist at startup
        config_sql = _esc_json(json.dumps(config, ensure_ascii=False))
        sql_executor.execute_sync(
            self._client,
            self._warehouse_id,
            f"""MERGE INTO {self._table} AS t
                USING (SELECT lower('{_esc_id(email)}') AS email) AS s
                ON t.email = s.email
                WHEN MATCHED THEN UPDATE SET
                    config     = '{config_sql}',
                    updated_at = current_timestamp(),
                    updated_by = '{_esc_id(updated_by)}'
                WHEN NOT MATCHED THEN INSERT (email, config, updated_at, updated_by)
                VALUES (
                    lower('{_esc_id(email)}'),
                    '{config_sql}',
                    current_timestamp(),
                    '{_esc_id(updated_by)}'
                )""",
            label="upsert_user_config",
        )

    def delete(self, email: str) -> None:
        self._ensure_table()
        sql_executor.execute_sync(
            self._client,
            self._warehouse_id,
            f"DELETE FROM {self._table} WHERE lower(email) = lower('{_esc_id(email)}')",
            label="delete_user_config",
        )


# ── Module-level singleton ─────────────────────────────────────────────────────

_singleton: Optional[UserPermissionsService] = None
_singleton_lock = threading.Lock()


def get_service() -> Optional[UserPermissionsService]:
    """Return the shared UserPermissionsService instance, creating it on first call."""
    global _singleton
    if _singleton is not None:
        return _singleton
    with _singleton_lock:
        if _singleton is None:
            try:
                from core.dependencies import get_workspace_client
                from core.config import get_settings
                settings = get_settings()
                _singleton = UserPermissionsService(
                    get_workspace_client(),
                    settings.uc_catalog_name,
                    settings.databricks_warehouse_id,
                )
            except Exception as exc:
                _log.error("UserPermissionsService init failed: %s", exc)
                return None
    return _singleton
