"""
etl/extractors/linkedin_ads_extractor.py
Extracts campaign analytics from LinkedIn Marketing API v2.
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd
import requests

from config.settings import get_settings
from etl.extractors.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.linkedin.com/v2"
_ADS_ANALYTICS_URL = f"{_BASE_URL}/adAnalytics"

_PIVOT_FIELDS = [
    "CAMPAIGN",
    "CREATIVE",
    "MEMBER_COMPANY",
    "MEMBER_INDUSTRY",
    "MEMBER_JOB_TITLE",
]

_FIELDS = [
    "dateRange",
    "pivot",
    "pivotValue",
    "impressions",
    "clicks",
    "costInLocalCurrency",
    "one_click_leads",
    "conversions",
    "videoViews",
    "sends",
    "opens",
    "actionClicks",
    "cardClicks",
    "comments",
    "companyPageClicks",
    "follows",
    "fullScreenPlays",
    "likes",
    "shares",
    "otherEngagements",
    "totalEngagements",
    "viralImpressions",
    "viralClicks",
]


class LinkedInAdsExtractor(BaseExtractor):
    source_name = "linkedin_ads"

    def __init__(self, **kwargs: int) -> None:
        super().__init__(**kwargs)
        self._cfg = get_settings().linkedin_ads

    def validate_config(self) -> None:
        cfg = self._cfg
        missing = [
            name
            for name, val in {
                "access_token": cfg.access_token,
                "ad_account_id": cfg.ad_account_id,
            }.items()
            if not val
        ]
        if missing:
            raise ValueError(f"Missing LinkedIn Ads credentials: {missing}")

    @property
    def _auth_header(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._cfg.access_token}",
            "LinkedIn-Version": "202401",
            "X-Restli-Protocol-Version": "2.0.0",
        }

    def extract(self, start_date: date, end_date: date) -> pd.DataFrame:
        self.validate_config()
        all_rows: list[dict] = []

        for chunk_start, chunk_end in self.date_range_chunks(start_date, end_date, 7):
            params = {
                "q": "analytics",
                "pivot": "CAMPAIGN",
                "dateRange.start.year": chunk_start.year,
                "dateRange.start.month": chunk_start.month,
                "dateRange.start.day": chunk_start.day,
                "dateRange.end.year": chunk_end.year,
                "dateRange.end.month": chunk_end.month,
                "dateRange.end.day": chunk_end.day,
                "accounts": f"urn:li:sponsoredAccount:{self._cfg.ad_account_id}",
                "timeGranularity": "DAILY",
                "fields": ",".join(_FIELDS),
            }

            response = requests.get(
                _ADS_ANALYTICS_URL,
                headers=self._auth_header,
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            elements = response.json().get("elements", [])

            for elem in elements:
                row: dict = {
                    "date_start": f"{chunk_start.year}-{chunk_start.month:02d}-{chunk_start.day:02d}",
                    "pivot": elem.get("pivot"),
                    "pivot_value": elem.get("pivotValue"),
                    "impressions": elem.get("impressions", 0),
                    "clicks": elem.get("clicks", 0),
                    "cost": float(
                        (elem.get("costInLocalCurrency") or {}).get("amount", 0)
                    ),
                    "one_click_leads": elem.get("one_click_leads", 0),
                    "conversions": elem.get("conversions", 0),
                    "video_views": elem.get("videoViews", 0),
                    "total_engagements": elem.get("totalEngagements", 0),
                    "likes": elem.get("likes", 0),
                    "shares": elem.get("shares", 0),
                    "comments": elem.get("comments", 0),
                    "follows": elem.get("follows", 0),
                }
                all_rows.append(row)

        df = pd.DataFrame(all_rows)
        if not df.empty:
            df["date_start"] = pd.to_datetime(df["date_start"])
            numeric_cols = [
                "impressions", "clicks", "cost", "one_click_leads",
                "conversions", "video_views", "total_engagements",
            ]
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
        return df
