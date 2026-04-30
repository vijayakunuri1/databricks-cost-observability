"""
Cloud Platform Cost Service

Reads from the unified Delta table `<catalog>.platform.cloud_platform_costs`
populated by notebooks/ingest_cloud_costs.ipynb.

Supported providers: azure | aws | gcp
"""

from __future__ import annotations

import datetime
import logging
from typing import Optional

from databricks.sdk import WorkspaceClient

from core.config import get_settings
from core.sql_executor import safe_execute_sync

_log = logging.getLogger(__name__)

_PROVIDERS = {"azure", "aws", "gcp"}


class CloudCostService:
    def __init__(self, client: WorkspaceClient) -> None:
        self._client = client
        self._wh_id = get_settings().databricks_warehouse_id
        self._catalog = get_settings().uc_catalog_name

    def _safe(self, sql: str) -> list[dict]:
        return safe_execute_sync(self._client, self._wh_id, sql, label="CloudCost")

    def _table(self) -> str:
        return f"`{self._catalog}`.platform.cloud_platform_costs"

    def _provider_clause(self, provider: Optional[str]) -> str:
        if provider and provider.lower() in _PROVIDERS:
            p = provider.lower().replace("'", "")
            return f"AND cloud_provider = '{p}'"
        return ""

    def _account_clause(self, account_ids: list[str]) -> str:
        if not account_ids:
            return ""
        safe = [a.replace("'", "") for a in account_ids if a.strip()]
        if not safe:
            return ""
        quoted = ", ".join(f"'{a}'" for a in safe)
        return f"AND account_id IN ({quoted})"

    # ── Public methods ────────────────────────────────────────────────────────

    def get_providers(self) -> list[dict]:
        """Distinct cloud providers that have data in the table."""
        sql = f"""
            SELECT cloud_provider, COUNT(*) AS row_count
            FROM {self._table()}
            GROUP BY cloud_provider
            ORDER BY cloud_provider
        """
        return self._safe(sql)

    def get_accounts(self, provider: Optional[str] = None) -> list[dict]:
        """Distinct accounts (subscription/account/project) optionally filtered by provider."""
        pc = self._provider_clause(provider)
        sql = f"""
            SELECT cloud_provider, account_id, MAX(account_name) AS account_name
            FROM {self._table()}
            WHERE 1=1 {pc}
            GROUP BY cloud_provider, account_id
            ORDER BY cloud_provider, account_name
        """
        return self._safe(sql)

    def get_summary(
        self,
        start_date: str,
        end_date: str,
        provider: Optional[str] = None,
        account_ids: Optional[list[str]] = None,
    ) -> dict:
        pc = self._provider_clause(provider)
        ac = self._account_clause(account_ids or [])
        sql = f"""
            SELECT
                ROUND(SUM(cost_usd), 2)                                  AS total_cost_usd,
                COUNT(DISTINCT account_id)                                AS account_count,
                COUNT(DISTINCT service_name)                              AS service_count,
                COUNT(DISTINCT region)                                    AS region_count,
                MAX(usage_date)                                           AS latest_date,
                MIN(usage_date)                                           AS earliest_date
            FROM {self._table()}
            WHERE usage_date BETWEEN '{start_date}' AND '{end_date}'
            {pc} {ac}
        """
        rows = self._safe(sql)
        row = rows[0] if rows else {}

        # MoM: compare same window shifted back by number of days
        try:
            sd = datetime.date.fromisoformat(start_date)
            ed = datetime.date.fromisoformat(end_date)
            span = (ed - sd).days or 1
            prev_ed = (sd - datetime.timedelta(days=1)).isoformat()
            prev_sd = (sd - datetime.timedelta(days=span)).isoformat()
        except ValueError:
            prev_sd = prev_ed = start_date

        prev_sql = f"""
            SELECT ROUND(SUM(cost_usd), 2) AS prev_cost
            FROM {self._table()}
            WHERE usage_date BETWEEN '{prev_sd}' AND '{prev_ed}'
            {pc} {ac}
        """
        prev_rows = self._safe(prev_sql)
        prev_cost = float(prev_rows[0].get("prev_cost") or 0) if prev_rows else 0.0
        curr_cost = float(row.get("total_cost_usd") or 0)
        mom = round(((curr_cost - prev_cost) / prev_cost * 100) if prev_cost else 0, 1)

        return {
            "total_cost_usd": curr_cost,
            "prev_cost_usd": prev_cost,
            "mom_pct": mom,
            "account_count": int(row.get("account_count") or 0),
            "service_count": int(row.get("service_count") or 0),
            "region_count": int(row.get("region_count") or 0),
            "latest_date": row.get("latest_date"),
        }

    def get_daily_trend(
        self,
        start_date: str,
        end_date: str,
        provider: Optional[str] = None,
        account_ids: Optional[list[str]] = None,
    ) -> list[dict]:
        pc = self._provider_clause(provider)
        ac = self._account_clause(account_ids or [])
        sql = f"""
            SELECT
                CAST(usage_date AS STRING) AS date,
                cloud_provider,
                ROUND(SUM(cost_usd), 2)   AS cost_usd
            FROM {self._table()}
            WHERE usage_date BETWEEN '{start_date}' AND '{end_date}'
            {pc} {ac}
            GROUP BY usage_date, cloud_provider
            ORDER BY usage_date, cloud_provider
        """
        return self._safe(sql)

    def get_by_service(
        self,
        start_date: str,
        end_date: str,
        provider: Optional[str] = None,
        account_ids: Optional[list[str]] = None,
        top_n: int = 20,
    ) -> list[dict]:
        pc = self._provider_clause(provider)
        ac = self._account_clause(account_ids or [])
        sql = f"""
            SELECT
                service_name,
                service_category,
                cloud_provider,
                ROUND(SUM(cost_usd), 2)   AS cost_usd,
                COUNT(DISTINCT account_id) AS account_count
            FROM {self._table()}
            WHERE usage_date BETWEEN '{start_date}' AND '{end_date}'
            {pc} {ac}
            GROUP BY service_name, service_category, cloud_provider
            ORDER BY cost_usd DESC
            LIMIT {int(top_n)}
        """
        return self._safe(sql)

    def get_by_account(
        self,
        start_date: str,
        end_date: str,
        provider: Optional[str] = None,
        account_ids: Optional[list[str]] = None,
    ) -> list[dict]:
        pc = self._provider_clause(provider)
        ac = self._account_clause(account_ids or [])
        sql = f"""
            SELECT
                cloud_provider,
                account_id,
                MAX(account_name)         AS account_name,
                ROUND(SUM(cost_usd), 2)   AS cost_usd,
                COUNT(DISTINCT service_name) AS service_count,
                COUNT(DISTINCT region)    AS region_count
            FROM {self._table()}
            WHERE usage_date BETWEEN '{start_date}' AND '{end_date}'
            {pc} {ac}
            GROUP BY cloud_provider, account_id
            ORDER BY cost_usd DESC
        """
        return self._safe(sql)

    def get_by_region(
        self,
        start_date: str,
        end_date: str,
        provider: Optional[str] = None,
        account_ids: Optional[list[str]] = None,
    ) -> list[dict]:
        pc = self._provider_clause(provider)
        ac = self._account_clause(account_ids or [])
        sql = f"""
            SELECT
                cloud_provider,
                region,
                ROUND(SUM(cost_usd), 2)  AS cost_usd
            FROM {self._table()}
            WHERE usage_date BETWEEN '{start_date}' AND '{end_date}'
            {pc} {ac}
            GROUP BY cloud_provider, region
            ORDER BY cost_usd DESC
            LIMIT 30
        """
        return self._safe(sql)

    def get_by_sku(
        self,
        start_date: str,
        end_date: str,
        provider: Optional[str] = None,
        account_ids: Optional[list[str]] = None,
        top_n: int = 30,
    ) -> list[dict]:
        pc = self._provider_clause(provider)
        ac = self._account_clause(account_ids or [])
        sql = f"""
            SELECT
                cloud_provider,
                service_name,
                service_category,
                sku_description,
                usage_unit,
                pricing_model,
                ROUND(SUM(usage_quantity), 2) AS total_quantity,
                ROUND(AVG(unit_price), 8)     AS avg_unit_price,
                ROUND(SUM(cost_usd), 2)       AS cost_usd,
                COUNT(DISTINCT account_id)    AS account_count
            FROM {self._table()}
            WHERE usage_date BETWEEN '{start_date}' AND '{end_date}'
            {pc} {ac}
            GROUP BY cloud_provider, service_name, service_category,
                     sku_description, usage_unit, pricing_model
            ORDER BY cost_usd DESC
            LIMIT {int(top_n)}
        """
        return self._safe(sql)

    def get_by_category(
        self,
        start_date: str,
        end_date: str,
        provider: Optional[str] = None,
        account_ids: Optional[list[str]] = None,
    ) -> list[dict]:
        pc = self._provider_clause(provider)
        ac = self._account_clause(account_ids or [])
        sql = f"""
            SELECT
                service_category,
                cloud_provider,
                ROUND(SUM(cost_usd), 2) AS cost_usd
            FROM {self._table()}
            WHERE usage_date BETWEEN '{start_date}' AND '{end_date}'
            {pc} {ac}
            GROUP BY service_category, cloud_provider
            ORDER BY cost_usd DESC
        """
        return self._safe(sql)
