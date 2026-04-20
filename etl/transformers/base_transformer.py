"""
etl/transformers/base_transformer.py
Abstract base class for all transformers.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod

import pandas as pd


class BaseTransformer(ABC):
    source_name: str = "base"

    def __init__(self) -> None:
        self._logger = logging.getLogger(f"{__name__}.{self.source_name}")

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        self._logger.info("Transforming %d rows from %s", len(df), self.source_name)
        df = self.transform(df)
        df = self._deduplicate(df)
        self._logger.info("Transform complete — %d rows remain", len(df))
        return df

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply source-specific cleaning and enrichment."""

    # ── Shared helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _deduplicate(df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df.drop_duplicates()
        dropped = before - len(df)
        if dropped:
            logging.getLogger(__name__).warning("Dropped %d duplicate rows", dropped)
        return df

    @staticmethod
    def safe_divide(
        numerator: pd.Series, denominator: pd.Series, fill: float = 0.0
    ) -> pd.Series:
        return (numerator / denominator.replace(0, float("nan"))).fillna(fill)

    @staticmethod
    def normalize_channel(raw: str | None) -> str:
        """Map raw source/medium strings to canonical channel names."""
        if not raw:
            return "unknown"
        raw = raw.lower().strip()
        channel_map = {
            "google": "paid_search",
            "cpc": "paid_search",
            "ppc": "paid_search",
            "facebook": "paid_social",
            "instagram": "paid_social",
            "meta": "paid_social",
            "linkedin": "paid_social",
            "email": "email",
            "organic": "organic_search",
            "seo": "organic_search",
            "direct": "direct",
            "referral": "referral",
            "affiliate": "affiliate",
            "display": "display",
        }
        for keyword, channel in channel_map.items():
            if keyword in raw:
                return channel
        return "other"
