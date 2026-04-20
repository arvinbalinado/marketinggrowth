"""
etl/transformers/campaign_transformer.py
Cleans and enriches raw paid-media campaign data from all ad platforms.
Produces a unified schema ready for the staging layer in BigQuery.
"""
from __future__ import annotations

import pandas as pd

from etl.transformers.base_transformer import BaseTransformer


class CampaignTransformer(BaseTransformer):
    source_name = "campaign"

    # Minimum spend threshold to avoid division noise
    _MIN_SPEND = 0.01

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # ── Standardise column names ──────────────────────────────────────────
        df = self._rename_columns(df)

        # ── Type coercions ────────────────────────────────────────────────────
        numeric_cols = ["impressions", "clicks", "cost", "conversions", "conversion_value"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

        # ── Derived KPIs ──────────────────────────────────────────────────────
        df["ctr"] = self.safe_divide(df.get("clicks", 0), df.get("impressions", 0))
        df["cpc"] = self.safe_divide(df.get("cost", 0), df.get("clicks", 0))
        df["cpm"] = self.safe_divide(df.get("cost", 0), df.get("impressions", 0)) * 1000
        df["cvr"] = self.safe_divide(df.get("conversions", 0), df.get("clicks", 0))
        df["cpa"] = self.safe_divide(df.get("cost", 0), df.get("conversions", 0))
        df["roas"] = self.safe_divide(
            df.get("conversion_value", 0), df.get("cost", 0)
        )

        # ── Channel normalisation ─────────────────────────────────────────────
        if "channel_type" in df.columns:
            df["channel_normalized"] = df["channel_type"].apply(self.normalize_channel)
        elif "_source" in df.columns:
            df["channel_normalized"] = df["_source"].apply(self.normalize_channel)

        # ── Remove invalid rows ───────────────────────────────────────────────
        if "impressions" in df.columns:
            df = df[df["impressions"] >= 0]

        return df

    @staticmethod
    def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
        rename_map = {
            # Google Ads → canonical
            "cost_micros": "cost",
            "conversions_value": "conversion_value",
            "video_views": "video_views",
            # Meta → canonical
            "spend": "cost",
            "date_start": "date",
            # LinkedIn → canonical
            "cost": "cost",
        }
        cols_to_rename = {k: v for k, v in rename_map.items() if k in df.columns and v not in df.columns}
        return df.rename(columns=cols_to_rename)
