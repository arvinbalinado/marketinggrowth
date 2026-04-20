"""
etl/extractors/hubspot_extractor.py
Extracts contacts, deals, and companies from HubSpot CRM API v3.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

import hubspot
import pandas as pd
from hubspot.crm.contacts import ApiException

from config.settings import get_settings
from etl.extractors.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

_CONTACT_PROPERTIES = [
    "email",
    "firstname",
    "lastname",
    "phone",
    "company",
    "lifecyclestage",
    "hs_lead_status",
    "createdate",
    "lastmodifieddate",
    "hubspot_owner_id",
    "hs_analytics_source",
    "hs_analytics_source_data_1",
    "hs_analytics_first_referrer",
    "hs_analytics_first_url",
    "num_conversion_events",
    "recent_conversion_event_name",
    "hs_email_optout",
]

_DEAL_PROPERTIES = [
    "dealname",
    "amount",
    "dealstage",
    "closedate",
    "createdate",
    "pipeline",
    "hubspot_owner_id",
    "hs_deal_stage_probability",
    "hs_is_closed_won",
    "hs_is_closed",
    "num_associated_contacts",
]

_COMPANY_PROPERTIES = [
    "name",
    "domain",
    "industry",
    "annualrevenue",
    "numberofemployees",
    "lifecyclestage",
    "createdate",
    "hs_lastcontacted",
]


class HubSpotExtractor(BaseExtractor):
    source_name = "hubspot"

    def __init__(self, **kwargs: int) -> None:
        super().__init__(**kwargs)
        self._cfg = get_settings().hubspot

    def validate_config(self) -> None:
        if not self._cfg.api_key:
            raise ValueError("HUBSPOT_API_KEY is required")

    def extract(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Returns a dict-like merged frame; callers should split by `object_type`."""
        self.validate_config()
        frames = {
            "contacts": self._extract_objects(
                "contacts", _CONTACT_PROPERTIES, start_date, end_date
            ),
            "deals": self._extract_objects(
                "deals", _DEAL_PROPERTIES, start_date, end_date
            ),
            "companies": self._extract_objects(
                "companies", _COMPANY_PROPERTIES, start_date, end_date
            ),
        }
        combined: list[pd.DataFrame] = []
        for obj_type, df in frames.items():
            df["object_type"] = obj_type
            combined.append(df)
        return pd.concat(combined, ignore_index=True) if combined else pd.DataFrame()

    def _extract_objects(
        self,
        object_type: str,
        properties: list[str],
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        client = hubspot.Client.create(access_token=self._cfg.api_key)
        api_map = {
            "contacts": client.crm.contacts.basic_api,
            "deals": client.crm.deals.basic_api,
            "companies": client.crm.companies.basic_api,
        }
        api = api_map[object_type]

        # Convert dates to millisecond timestamps for HubSpot filters
        start_ms = int(
            datetime.combine(start_date, datetime.min.time())
            .replace(tzinfo=timezone.utc)
            .timestamp()
            * 1000
        )
        end_ms = int(
            datetime.combine(end_date + timedelta(days=1), datetime.min.time())
            .replace(tzinfo=timezone.utc)
            .timestamp()
            * 1000
        )

        records: list[dict] = []
        after: str | None = None
        page_size = 100

        while True:
            try:
                kwargs: dict[str, Any] = {
                    "limit": page_size,
                    "properties": properties,
                }
                if after:
                    kwargs["after"] = after

                response = api.get_page(**kwargs)
                for obj in response.results:
                    props = obj.properties or {}
                    # Filter by last modified within window
                    modified = props.get("lastmodifieddate") or props.get("createdate")
                    record = {"id": obj.id}
                    record.update(props)
                    records.append(record)

                if response.paging and response.paging.next:
                    after = response.paging.next.after
                else:
                    break
            except ApiException as exc:
                logger.error("HubSpot API error (%s): %s", object_type, exc)
                raise

        df = pd.DataFrame(records)
        if not df.empty and "createdate" in df.columns:
            df["createdate"] = pd.to_datetime(df["createdate"], errors="coerce")
            # Keep rows within requested window
            df = df[
                (df["createdate"] >= pd.Timestamp(start_date, tz="UTC"))
                & (df["createdate"] <= pd.Timestamp(end_date, tz="UTC"))
            ].copy()
        return df
