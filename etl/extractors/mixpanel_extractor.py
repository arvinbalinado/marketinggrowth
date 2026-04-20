"""
etl/extractors/mixpanel_extractor.py
Extracts raw events, segmentation metrics, and funnel data from Mixpanel
using the Data Export API and Query API (v2).

Authentication uses a Service Account (recommended) or legacy API secret.

Three extractors are provided:
  - MixpanelEventExtractor    : raw event-level export (Data Export API)
  - MixpanelSegmentExtractor  : aggregated segmentation metrics (Query API)
  - MixpanelFunnelExtractor   : funnel step conversions (Query API)
"""
from __future__ import annotations

import gzip
import json
import logging
from base64 import b64encode
from datetime import date, timedelta
from io import BytesIO
from typing import Any

import pandas as pd
import requests

from config.settings import get_settings
from etl.extractors.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

# ── Base URLs (supports EU data residency) ────────────────────────────────────
_DATA_EXPORT_US = "https://data.mixpanel.com/api/2.0/export"
_QUERY_US = "https://mixpanel.com/api/2.0"
_DATA_EXPORT_EU = "https://eu.data.mixpanel.com/api/2.0/export"
_QUERY_EU = "https://eu.mixpanel.com/api/2.0"


def _get_base_urls(region: str) -> tuple[str, str]:
    if region.upper() == "EU":
        return _DATA_EXPORT_EU, _QUERY_EU
    return _DATA_EXPORT_US, _QUERY_US


class _MixpanelBase(BaseExtractor):
    """Shared auth and request helper for all Mixpanel extractors."""

    def __init__(self, **kwargs: int) -> None:
        super().__init__(**kwargs)
        self._cfg = get_settings().mixpanel
        export_url, query_url = _get_base_urls(self._cfg.region)
        self._export_url = export_url
        self._query_url = query_url

    def validate_config(self) -> None:
        cfg = self._cfg
        # Accept either service-account or legacy API secret
        has_sa = cfg.service_account_username and cfg.service_account_secret
        has_secret = cfg.api_secret
        if not (has_sa or has_secret):
            raise ValueError(
                "Mixpanel requires either MIXPANEL_SERVICE_ACCOUNT_USERNAME + "
                "MIXPANEL_SERVICE_ACCOUNT_SECRET, or MIXPANEL_API_SECRET"
            )
        if not cfg.project_id:
            raise ValueError("MIXPANEL_PROJECT_ID is required")

    @property
    def _auth_header(self) -> dict[str, str]:
        cfg = self._cfg
        if cfg.service_account_username and cfg.service_account_secret:
            token = b64encode(
                f"{cfg.service_account_username}:{cfg.service_account_secret}".encode()
            ).decode()
            return {"Authorization": f"Basic {token}"}
        # Legacy: API secret used as username with empty password
        token = b64encode(f"{cfg.api_secret}:".encode()).decode()
        return {"Authorization": f"Basic {token}"}

    def _get(self, url: str, params: dict[str, Any]) -> requests.Response:
        response = requests.get(
            url,
            headers={**self._auth_header, "Accept": "application/json"},
            params=params,
            timeout=120,
        )
        response.raise_for_status()
        return response


# ─────────────────────────────────────────────────────────────────────────────
# 1. Raw Event Export
# ─────────────────────────────────────────────────────────────────────────────
class MixpanelEventExtractor(_MixpanelBase):
    """
    Exports raw, event-level data via the Mixpanel Data Export API.
    Returns one row per event instance; columns mirror Mixpanel event properties.

    Key output columns:
      event, distinct_id, time, insert_id, mp_lib, mp_country_code,
      mp_city, os, browser, screen_width, screen_height,
      utm_source, utm_medium, utm_campaign, utm_content, utm_term,
      current_url, initial_referrer, plus all custom event properties.
    """

    source_name = "mixpanel_events"

    def __init__(self, event_names: list[str] | None = None, **kwargs: int) -> None:
        super().__init__(**kwargs)
        self.event_names = event_names  # None = all events

    def extract(self, start_date: date, end_date: date) -> pd.DataFrame:
        self.validate_config()
        all_rows: list[dict] = []

        for chunk_start, chunk_end in self.date_range_chunks(start_date, end_date, chunk_days=1):
            params: dict[str, Any] = {
                "from_date": chunk_start.isoformat(),
                "to_date": chunk_end.isoformat(),
                "project_id": self._cfg.project_id,
            }
            if self.event_names:
                params["event"] = json.dumps(self.event_names)

            response = requests.get(
                self._export_url,
                headers=self._auth_header,
                params=params,
                timeout=300,
                stream=True,
            )
            response.raise_for_status()

            # Response is newline-delimited JSON (one JSON object per line)
            for raw_line in response.iter_lines():
                if not raw_line:
                    continue
                try:
                    obj = json.loads(raw_line)
                except json.JSONDecodeError:
                    logger.warning("Skipping malformed Mixpanel event line")
                    continue

                props = obj.get("properties", {})
                row: dict = {
                    "event": obj.get("event"),
                    "distinct_id": props.get("distinct_id"),
                    "insert_id": props.get("$insert_id"),
                    "time": props.get("time"),
                    # Geography
                    "country_code": props.get("mp_country_code"),
                    "city": props.get("mp_city"),
                    "region": props.get("mp_region"),
                    # Device / platform
                    "os": props.get("$os"),
                    "browser": props.get("$browser"),
                    "browser_version": props.get("$browser_version"),
                    "device": props.get("$device"),
                    "screen_width": props.get("$screen_width"),
                    "screen_height": props.get("$screen_height"),
                    # Traffic source
                    "utm_source": props.get("utm_source"),
                    "utm_medium": props.get("utm_medium"),
                    "utm_campaign": props.get("utm_campaign"),
                    "utm_content": props.get("utm_content"),
                    "utm_term": props.get("utm_term"),
                    # Page / session
                    "current_url": props.get("$current_url"),
                    "initial_referrer": props.get("$initial_referrer"),
                    "referrer": props.get("$referrer"),
                    "lib": props.get("mp_lib"),
                    # All remaining custom properties serialised as JSON
                    "custom_properties": json.dumps(
                        {
                            k: v
                            for k, v in props.items()
                            if not k.startswith(("$", "mp_"))
                            and k
                            not in (
                                "distinct_id",
                                "time",
                                "utm_source",
                                "utm_medium",
                                "utm_campaign",
                                "utm_content",
                                "utm_term",
                            )
                        }
                    ),
                    "event_date": chunk_start.isoformat(),
                }
                all_rows.append(row)

        df = pd.DataFrame(all_rows)
        if not df.empty:
            df["time"] = pd.to_datetime(df["time"], unit="s", utc=True, errors="coerce")
            df["event_date"] = pd.to_datetime(df["event_date"])
        return df


# ─────────────────────────────────────────────────────────────────────────────
# 2. Segmentation (aggregated metrics per event, broken down by property)
# ─────────────────────────────────────────────────────────────────────────────
class MixpanelSegmentExtractor(_MixpanelBase):
    """
    Pulls aggregated event counts segmented by a single property using the
    Mixpanel Segmentation (Query) API.

    Returns one row per (event, segment_value, date) combination.
    """

    source_name = "mixpanel_segmentation"

    # Marketing-critical events to pull by default
    DEFAULT_EVENTS = [
        "Signed Up",
        "Page Viewed",
        "Campaign Clicked",
        "Add to Cart",
        "Checkout Started",
        "Order Completed",
        "Lead Form Submitted",
        "Demo Requested",
    ]

    def __init__(
        self,
        events: list[str] | None = None,
        segment_property: str = "utm_source",
        **kwargs: int,
    ) -> None:
        super().__init__(**kwargs)
        self.events = events or self.DEFAULT_EVENTS
        self.segment_property = segment_property

    def extract(self, start_date: date, end_date: date) -> pd.DataFrame:
        self.validate_config()
        all_rows: list[dict] = []

        for event_name in self.events:
            params = {
                "project_id": self._cfg.project_id,
                "event": event_name,
                "on": f'properties["{self.segment_property}"]',
                "from_date": start_date.isoformat(),
                "to_date": end_date.isoformat(),
                "unit": "day",
                "type": "general",
            }

            try:
                resp = self._get(f"{self._query_url}/segmentation", params)
                data = resp.json().get("data", {}).get("values", {})
            except requests.HTTPError as exc:
                logger.warning(
                    "Segmentation API error for event '%s': %s", event_name, exc
                )
                continue

            # Response format: {segment_value: {date_str: count, ...}, ...}
            for segment_value, date_counts in data.items():
                for date_str, count in date_counts.items():
                    all_rows.append(
                        {
                            "event": event_name,
                            "segment_property": self.segment_property,
                            "segment_value": segment_value,
                            "date": date_str,
                            "event_count": int(count),
                        }
                    )

        df = pd.DataFrame(all_rows)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df["event_count"] = pd.to_numeric(df["event_count"], errors="coerce").fillna(0)
        return df


# ─────────────────────────────────────────────────────────────────────────────
# 3. Funnel conversions
# ─────────────────────────────────────────────────────────────────────────────
class MixpanelFunnelExtractor(_MixpanelBase):
    """
    Pulls funnel conversion data for named funnels from the Mixpanel Funnels API.
    Requires funnel IDs from your Mixpanel project (Settings → Funnels).
    """

    source_name = "mixpanel_funnels"

    def __init__(self, funnel_ids: list[int] | None = None, **kwargs: int) -> None:
        super().__init__(**kwargs)
        # funnel_ids can be passed directly or configured via env:
        # MIXPANEL_FUNNEL_IDS=123,456,789
        import os
        env_ids = os.getenv("MIXPANEL_FUNNEL_IDS", "")
        self.funnel_ids = funnel_ids or [
            int(fid.strip()) for fid in env_ids.split(",") if fid.strip()
        ]

    def extract(self, start_date: date, end_date: date) -> pd.DataFrame:
        self.validate_config()
        if not self.funnel_ids:
            logger.info("No MIXPANEL_FUNNEL_IDS configured — skipping funnel extraction")
            return pd.DataFrame()

        all_rows: list[dict] = []
        for funnel_id in self.funnel_ids:
            params = {
                "project_id": self._cfg.project_id,
                "funnel_id": funnel_id,
                "from_date": start_date.isoformat(),
                "to_date": end_date.isoformat(),
                "unit": "day",
                "interval": 1,
            }
            try:
                resp = self._get(f"{self._query_url}/funnels", params)
                meta = resp.json().get("meta", {})
                data = resp.json().get("data", {})
            except requests.HTTPError as exc:
                logger.warning("Funnels API error for funnel %s: %s", funnel_id, exc)
                continue

            funnel_name = meta.get("funnel_name", str(funnel_id))
            steps = meta.get("steps", [])

            # data = { date_str: { steps: [...], analysis: {...} }, ... }
            for date_str, day_data in data.items():
                step_data = day_data.get("steps", [])
                analysis = day_data.get("analysis", {})

                for step_idx, step in enumerate(step_data):
                    all_rows.append(
                        {
                            "funnel_id": funnel_id,
                            "funnel_name": funnel_name,
                            "date": date_str,
                            "step_index": step_idx,
                            "step_label": steps[step_idx] if step_idx < len(steps) else f"step_{step_idx}",
                            "count": step.get("count", 0),
                            "conversion_rate": step.get("step_conv_ratio", 0.0),
                            "avg_time_to_convert_sec": step.get("avg_time", 0),
                            "goal_count": analysis.get("completion", 0),
                            "goal_conversion_rate": analysis.get("completion_ratio", 0.0),
                        }
                    )

        df = pd.DataFrame(all_rows)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            for col in ("count", "conversion_rate", "goal_count", "goal_conversion_rate"):
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return df
