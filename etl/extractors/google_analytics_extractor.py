"""
etl/extractors/google_analytics_extractor.py
Extracts session, user, and conversion data from Google Analytics 4 Data API.
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)

from config.settings import get_settings
from etl.extractors.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

_DIMENSIONS = [
    "date",
    "sessionSource",
    "sessionMedium",
    "sessionCampaignName",
    "deviceCategory",
    "country",
    "landingPage",
    "sessionDefaultChannelGroup",
]

_METRICS = [
    "sessions",
    "activeUsers",
    "newUsers",
    "bounceRate",
    "averageSessionDuration",
    "screenPageViews",
    "conversions",
    "totalRevenue",
    "engagedSessions",
    "engagementRate",
]


class GoogleAnalyticsExtractor(BaseExtractor):
    source_name = "google_analytics_4"

    def __init__(self, **kwargs: int) -> None:
        super().__init__(**kwargs)
        self._cfg = get_settings().ga4

    def validate_config(self) -> None:
        if not self._cfg.property_id:
            raise ValueError("GA4_PROPERTY_ID is required")

    def extract(self, start_date: date, end_date: date) -> pd.DataFrame:
        self.validate_config()
        client = BetaAnalyticsDataClient()

        request = RunReportRequest(
            property=f"properties/{self._cfg.property_id}",
            date_ranges=[
                DateRange(
                    start_date=start_date.isoformat(),
                    end_date=end_date.isoformat(),
                )
            ],
            dimensions=[Dimension(name=d) for d in _DIMENSIONS],
            metrics=[Metric(name=m) for m in _METRICS],
            limit=100_000,
        )

        response = client.run_report(request)

        rows: list[dict] = []
        dim_headers = [h.name for h in response.dimension_headers]
        met_headers = [h.name for h in response.metric_headers]

        for row in response.rows:
            record: dict = {}
            for i, dim in enumerate(row.dimension_values):
                record[dim_headers[i]] = dim.value
            for i, met in enumerate(row.metric_values):
                record[met_headers[i]] = met.value
            rows.append(record)

        df = pd.DataFrame(rows)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
            numeric_cols = _METRICS
            df[numeric_cols] = df[numeric_cols].apply(
                pd.to_numeric, errors="coerce"
            )
        return df
