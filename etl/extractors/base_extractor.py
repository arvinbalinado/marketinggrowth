"""
etl/extractors/base_extractor.py
Abstract base class for all data source extractors.
Implements retry logic, logging, and a standard extract() contract.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


class ExtractionError(Exception):
    """Raised when a source extraction fails after all retries."""


class BaseExtractor(ABC):
    """
    Abstract extractor.  All source-specific extractors must implement
    `extract()` and optionally override `validate_config()`.
    """

    source_name: str = "base"

    def __init__(self, lookback_days: int = 30, max_retries: int = 3) -> None:
        self.lookback_days = lookback_days
        self.max_retries = max_retries
        self._logger = logging.getLogger(f"{__name__}.{self.source_name}")

    # ── Public interface ──────────────────────────────────────────────────────

    def run(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        """
        Orchestrate extraction with default date-window logic and retry wrapping.
        Returns a DataFrame that always contains `_extracted_at` and `_source`.
        """
        end_date = end_date or date.today()
        start_date = start_date or (end_date - timedelta(days=self.lookback_days))

        self._logger.info(
            "Starting extraction from %s | window: %s → %s",
            self.source_name,
            start_date,
            end_date,
        )

        df = self._extract_with_retry(start_date, end_date)
        df = self._add_metadata(df)

        self._logger.info(
            "Extraction complete from %s | rows: %d",
            self.source_name,
            len(df),
        )
        return df

    # ── Abstract methods ──────────────────────────────────────────────────────

    @abstractmethod
    def extract(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Pull raw data from the source for the given date range."""

    @abstractmethod
    def validate_config(self) -> None:
        """Raise ValueError if required credentials/settings are missing."""

    # ── Internals ─────────────────────────────────────────────────────────────

    def _extract_with_retry(self, start_date: date, end_date: date) -> pd.DataFrame:
        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=2, max=60),
            retry=retry_if_exception_type((IOError, ConnectionError, TimeoutError)),
            reraise=True,
        )
        def _inner() -> pd.DataFrame:
            return self.extract(start_date, end_date)

        try:
            return _inner()
        except Exception as exc:
            raise ExtractionError(
                f"Extraction failed for {self.source_name} after {self.max_retries} attempts"
            ) from exc

    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["_extracted_at"] = datetime.utcnow().isoformat()
        df["_source"] = self.source_name
        return df

    # ── Utility helpers ───────────────────────────────────────────────────────

    @staticmethod
    def date_range_chunks(
        start_date: date, end_date: date, chunk_days: int = 7
    ) -> list[tuple[date, date]]:
        """Split a date range into smaller chunks to avoid API rate limits."""
        chunks: list[tuple[date, date]] = []
        current = start_date
        while current <= end_date:
            chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
            chunks.append((current, chunk_end))
            current = chunk_end + timedelta(days=1)
        return chunks
