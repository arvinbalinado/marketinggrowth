"""
etl/extractors/google_ads_extractor.py
Extracts campaign, ad-group, and keyword performance data from Google Ads API.
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

from config.settings import get_settings
from etl.extractors.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

_QUERY_TEMPLATE = """
    SELECT
        campaign.id,
        campaign.name,
        campaign.status,
        campaign.advertising_channel_type,
        ad_group.id,
        ad_group.name,
        ad_group.status,
        segments.date,
        segments.device,
        metrics.impressions,
        metrics.clicks,
        metrics.cost_micros,
        metrics.conversions,
        metrics.conversions_value,
        metrics.video_views,
        metrics.ctr,
        metrics.average_cpc,
        metrics.search_impression_share
    FROM ad_group
    WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
      AND campaign.status != 'REMOVED'
    ORDER BY segments.date DESC
"""


class GoogleAdsExtractor(BaseExtractor):
    source_name = "google_ads"

    def __init__(self, customer_id: str | None = None, **kwargs: int) -> None:
        super().__init__(**kwargs)
        cfg = get_settings().google_ads
        self.customer_id = customer_id or cfg.login_customer_id
        self._cfg = cfg

    def validate_config(self) -> None:
        cfg = self._cfg
        missing = [
            name
            for name, val in {
                "developer_token": cfg.developer_token,
                "client_id": cfg.client_id,
                "client_secret": cfg.client_secret,
                "refresh_token": cfg.refresh_token,
            }.items()
            if not val
        ]
        if missing:
            raise ValueError(f"Missing Google Ads credentials: {missing}")

    def extract(self, start_date: date, end_date: date) -> pd.DataFrame:
        self.validate_config()
        cfg = self._cfg
        client = GoogleAdsClient.load_from_dict(
            {
                "developer_token": cfg.developer_token,
                "client_id": cfg.client_id,
                "client_secret": cfg.client_secret,
                "refresh_token": cfg.refresh_token,
                "login_customer_id": cfg.login_customer_id,
                "use_proto_plus": True,
            }
        )
        ga_service = client.get_service("GoogleAdsService")
        query = _QUERY_TEMPLATE.format(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )

        rows: list[dict] = []
        try:
            response = ga_service.search_stream(
                customer_id=self.customer_id, query=query
            )
            for batch in response:
                for row in batch.results:
                    rows.append(
                        {
                            "campaign_id": str(row.campaign.id),
                            "campaign_name": row.campaign.name,
                            "campaign_status": row.campaign.status.name,
                            "channel_type": row.campaign.advertising_channel_type.name,
                            "ad_group_id": str(row.ad_group.id),
                            "ad_group_name": row.ad_group.name,
                            "ad_group_status": row.ad_group.status.name,
                            "date": row.segments.date,
                            "device": row.segments.device.name,
                            "impressions": row.metrics.impressions,
                            "clicks": row.metrics.clicks,
                            "cost": row.metrics.cost_micros / 1_000_000,
                            "conversions": row.metrics.conversions,
                            "conversion_value": row.metrics.conversions_value,
                            "video_views": row.metrics.video_views,
                            "ctr": row.metrics.ctr,
                            "average_cpc": row.metrics.average_cpc / 1_000_000,
                            "search_impression_share": row.metrics.search_impression_share,
                        }
                    )
        except GoogleAdsException as ex:
            for error in ex.failure.errors:
                logger.error("Google Ads API error: %s", error.message)
            raise

        df = pd.DataFrame(rows)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        return df
