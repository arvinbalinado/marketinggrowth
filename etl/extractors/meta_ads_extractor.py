"""
etl/extractors/meta_ads_extractor.py
Extracts campaign insights from Meta (Facebook/Instagram) Marketing API.
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights

from config.settings import get_settings
from etl.extractors.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

_FIELDS = [
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.campaign_name,
    AdsInsights.Field.adset_id,
    AdsInsights.Field.adset_name,
    AdsInsights.Field.ad_id,
    AdsInsights.Field.ad_name,
    AdsInsights.Field.date_start,
    AdsInsights.Field.date_stop,
    AdsInsights.Field.impressions,
    AdsInsights.Field.clicks,
    AdsInsights.Field.spend,
    AdsInsights.Field.reach,
    AdsInsights.Field.frequency,
    AdsInsights.Field.actions,
    AdsInsights.Field.cost_per_action_type,
    AdsInsights.Field.objective,
    AdsInsights.Field.buying_type,
]


class MetaAdsExtractor(BaseExtractor):
    source_name = "meta_ads"

    def __init__(self, **kwargs: int) -> None:
        super().__init__(**kwargs)
        self._cfg = get_settings().meta_ads

    def validate_config(self) -> None:
        cfg = self._cfg
        missing = [
            name
            for name, val in {
                "app_id": cfg.app_id,
                "app_secret": cfg.app_secret,
                "access_token": cfg.access_token,
                "ad_account_id": cfg.ad_account_id,
            }.items()
            if not val
        ]
        if missing:
            raise ValueError(f"Missing Meta Ads credentials: {missing}")

    def extract(self, start_date: date, end_date: date) -> pd.DataFrame:
        self.validate_config()
        cfg = self._cfg
        FacebookAdsApi.init(cfg.app_id, cfg.app_secret, cfg.access_token)

        params = {
            "level": "ad",
            "time_range": {
                "since": start_date.isoformat(),
                "until": end_date.isoformat(),
            },
            "time_increment": 1,  # daily breakdown
            "limit": 500,
        }

        account = AdAccount(cfg.ad_account_id)
        insights = account.get_insights(fields=_FIELDS, params=params)

        rows: list[dict] = []
        for record in insights:
            row: dict = dict(record)
            # Flatten action types into named columns
            actions = row.pop("actions", []) or []
            for action in actions:
                action_type = action.get("action_type", "").replace(".", "_")
                row[f"action_{action_type}"] = float(action.get("value", 0))

            # Flatten cost_per_action_type
            cpas = row.pop("cost_per_action_type", []) or []
            for cpa in cpas:
                action_type = cpa.get("action_type", "").replace(".", "_")
                row[f"cpa_{action_type}"] = float(cpa.get("value", 0))

            rows.append(row)

        df = pd.DataFrame(rows)
        if not df.empty:
            for col in ("impressions", "clicks", "spend", "reach", "frequency"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            df["date_start"] = pd.to_datetime(df["date_start"])
        return df
