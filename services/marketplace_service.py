"""
Databricks Marketplace: listing events, consumer & provider listings.

Queries system.marketplace tables with SDK fallback.
"""

from __future__ import annotations

import datetime
import logging
from typing import Optional

from databricks.sdk import WorkspaceClient

from core.config import get_settings
from core.sql_executor import safe_execute_sync

_log = logging.getLogger(__name__)


class MarketplaceService:
    LOOKBACK_DAYS = 90
    CACHE_TTL_MIN = 60          # marketplace data changes slowly

    def __init__(self, client: WorkspaceClient) -> None:
        self._client = client
        self._wh_id  = get_settings().databricks_warehouse_id
        self._cache: Optional[dict]                  = None
        self._cached_at: Optional[datetime.datetime] = None

    def _safe(self, sql: str) -> list[dict]:
        return safe_execute_sync(self._client, self._wh_id, sql, label="Marketplace")

    # ── Listing events ────────────────────────────────────────────────────────

    def _fetch_listing_events(self) -> list[dict]:
        return self._safe(f"""
            SELECT
                listing_id,
                listing_name,
                event_type,
                CAST(event_time AS STRING) AS event_time,
                consumer_workspace_id,
                consumer_cloud,
                consumer_region
            FROM system.marketplace.listing_events
            WHERE event_time >= timestamp(date_add(current_date(), -{self.LOOKBACK_DAYS}))
            ORDER BY event_time DESC
            LIMIT 2000
        """)

    # ── Consumer listings ─────────────────────────────────────────────────────

    def _fetch_consumer_listings(self) -> list[dict]:
        return self._safe("""
            SELECT
                listing_id,
                listing_name,
                provider_name,
                status,
                listing_type,
                CAST(installed_on AS STRING)    AS installed_on,
                installed_catalog
            FROM system.marketplace.consumer_listings
            ORDER BY installed_on DESC
            LIMIT 500
        """)

    # ── Provider listings ─────────────────────────────────────────────────────

    def _fetch_provider_listings(self) -> list[dict]:
        return self._safe("""
            SELECT
                listing_id,
                listing_name,
                status,
                listing_type,
                CAST(created_at AS STRING) AS created_at,
                CAST(updated_at AS STRING) AS updated_at
            FROM system.marketplace.provider_listings
            ORDER BY updated_at DESC
            LIMIT 200
        """)

    # ── SDK fallback: installed exchange listings ─────────────────────────────

    def _fetch_sdk_listings(self) -> list[dict]:
        try:
            out = []
            for listing in self._client.marketplace_listings.list():
                out.append({
                    "listing_id":   getattr(listing, "id", ""),
                    "listing_name": getattr(listing, "name", ""),
                    "status":       str(getattr(listing, "status", "")),
                    "listing_type": str(getattr(listing, "listing_type", "")),
                })
            return out
        except Exception as e:
            _log.debug("Marketplace SDK list failed: %s", e)
        # Try exchange listings
        try:
            out = []
            for exch in self._client.clean_rooms.list():
                out.append({
                    "listing_id":   getattr(exch, "clean_room_id", ""),
                    "listing_name": getattr(exch, "name", ""),
                    "status":       "ACTIVE",
                    "listing_type": "CLEAN_ROOM",
                })
            return out
        except Exception as e2:
            _log.debug("Exchange SDK list failed: %s", e2)
            return []

    # ── Analysis ──────────────────────────────────────────────────────────────

    def _analyse(self, listing_events, consumer, provider, sdk) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)

        # Use consumer + provider + sdk as inventory
        all_installed = consumer or sdk

        # Event counts by type
        event_type_counts: dict = {}
        daily_events: dict = {}
        for e in listing_events:
            et  = e.get("event_type", "unknown")
            day = (e.get("event_time") or "")[:10]
            event_type_counts[et] = event_type_counts.get(et, 0) + 1
            if day:
                daily_events[day] = daily_events.get(day, 0) + 1

        # Top providers from consumer listings
        provider_counts: dict = {}
        for c in consumer:
            pname = c.get("provider_name", "unknown") or "unknown"
            provider_counts[pname] = provider_counts.get(pname, 0) + 1

        top_providers = sorted(
            [{"provider": k, "listings": v} for k, v in provider_counts.items()],
            key=lambda x: -x["listings"])[:15]

        # Listing type distribution
        type_dist: dict = {}
        for item in all_installed:
            lt = item.get("listing_type", "UNKNOWN") or "UNKNOWN"
            type_dist[lt] = type_dist.get(lt, 0) + 1

        return {
            "generated_at":  now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lookback_days": self.LOOKBACK_DAYS,
            "summary": {
                "listing_events":     len(listing_events),
                "consumer_listings":  len(consumer),
                "provider_listings":  len(provider),
                "unique_event_types": len(event_type_counts),
                "unique_providers":   len(provider_counts),
            },
            "event_type_counts": sorted(
                [{"type": k, "count": v} for k, v in event_type_counts.items()],
                key=lambda x: -x["count"]),
            "daily_trend": sorted(
                [{"date": k, "count": v} for k, v in daily_events.items()],
                key=lambda x: x["date"]),
            "top_providers": top_providers,
            "listing_type_dist": sorted(
                [{"type": k, "count": v} for k, v in type_dist.items()],
                key=lambda x: -x["count"]),
            "consumer_listings": [{
                "listing_id":       c.get("listing_id", ""),
                "listing_name":     c.get("listing_name", ""),
                "provider_name":    c.get("provider_name", ""),
                "status":           c.get("status", ""),
                "listing_type":     c.get("listing_type", ""),
                "installed_on":     c.get("installed_on", ""),
                "installed_catalog": c.get("installed_catalog", ""),
            } for c in consumer],
            "provider_listings": [{
                "listing_id":   p.get("listing_id", ""),
                "listing_name": p.get("listing_name", ""),
                "status":       p.get("status", ""),
                "listing_type": p.get("listing_type", ""),
                "created_at":   p.get("created_at", ""),
                "updated_at":   p.get("updated_at", ""),
            } for p in provider],
            "recent_events": [{
                "event_time":           e.get("event_time", ""),
                "listing_name":         e.get("listing_name", ""),
                "event_type":           e.get("event_type", ""),
                "consumer_workspace_id": e.get("consumer_workspace_id", ""),
                "consumer_cloud":       e.get("consumer_cloud", ""),
                "consumer_region":      e.get("consumer_region", ""),
            } for e in listing_events[:500]],
        }

    def get_marketplace_insights(self) -> dict:
        now = datetime.datetime.now(datetime.timezone.utc)
        if self._cache and self._cached_at:
            age = (now - self._cached_at).total_seconds() / 60
            if age < self.CACHE_TTL_MIN:
                return {**self._cache, "cache_age_minutes": round(age, 1)}

        listing_events  = self._fetch_listing_events()
        consumer        = self._fetch_consumer_listings()
        provider        = self._fetch_provider_listings()
        sdk             = [] if (consumer or provider) else self._fetch_sdk_listings()

        result = self._analyse(listing_events, consumer, provider, sdk)
        result["cache_age_minutes"] = 0
        self._cache    = result
        self._cached_at = now
        return result
