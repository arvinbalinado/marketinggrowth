"""
tests/test_extractors.py — Unit tests for ETL extractors.
Mocks all external API calls; no real credentials required.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from etl.extractors.base_extractor import BaseExtractor, ExtractionError


# ── Concrete stub for abstract class ─────────────────────────────────────────
class _StubExtractor(BaseExtractor):
    source_name = "stub"

    def extract(self, start_date: date, end_date: date) -> pd.DataFrame:
        return pd.DataFrame({"date": [start_date], "value": [1]})

    def validate_config(self) -> None:
        pass


# ── Base extractor tests ──────────────────────────────────────────────────────
class TestBaseExtractor:
    def test_run_adds_metadata(self):
        extractor = _StubExtractor()
        df = extractor.run(start_date=date(2026, 1, 1), end_date=date(2026, 1, 7))
        assert "_extracted_at" in df.columns
        assert "_source" in df.columns
        assert df["_source"].iloc[0] == "stub"

    def test_run_uses_default_date_window(self):
        extractor = _StubExtractor(lookback_days=7)
        df = extractor.run()
        assert len(df) > 0

    def test_date_range_chunks_splits_correctly(self):
        chunks = _StubExtractor.date_range_chunks(
            date(2026, 1, 1), date(2026, 1, 21), chunk_days=7
        )
        assert len(chunks) == 3
        assert chunks[0] == (date(2026, 1, 1), date(2026, 1, 7))
        assert chunks[2][1] == date(2026, 1, 21)

    def test_run_raises_extraction_error_on_persistent_failure(self):
        class _FailingExtractor(_StubExtractor):
            def extract(self, start_date, end_date):
                raise IOError("network failure")

        extractor = _FailingExtractor(max_retries=2)
        with pytest.raises(ExtractionError):
            extractor.run()


# ── Shopify extractor tests ───────────────────────────────────────────────────
class TestShopifyExtractor:
    def test_paginate_follows_link_header(self):
        from etl.extractors.shopify_extractor import ShopifyExtractor

        with patch("etl.extractors.shopify_extractor.requests.get") as mock_get:
            page1 = MagicMock()
            page1.json.return_value = {"orders": [{"id": 1}]}
            page1.headers = {"Link": '<https://next.page>; rel="next"'}
            page1.raise_for_status = MagicMock()

            page2 = MagicMock()
            page2.json.return_value = {"orders": [{"id": 2}]}
            page2.headers = {}
            page2.raise_for_status = MagicMock()

            mock_get.side_effect = [page1, page2]

            extractor = ShopifyExtractor.__new__(ShopifyExtractor)
            extractor._cfg = MagicMock(
                shop_url="test.myshopify.com",
                access_token="token",
                api_version="2024-01",
            )
            records = extractor._paginate("http://fake.url", "orders", {})
            assert len(records) == 2


# ── Transformer tests ─────────────────────────────────────────────────────────
class TestCampaignTransformer:
    def test_derived_kpis_calculated(self):
        from etl.transformers.campaign_transformer import CampaignTransformer

        df = pd.DataFrame(
            {
                "date": [date(2026, 1, 1)],
                "campaign_id": ["c1"],
                "impressions": [1000],
                "clicks": [50],
                "cost": [100.0],
                "conversions": [5.0],
                "conversion_value": [500.0],
                "_source": ["google_ads"],
                "channel_type": ["SEARCH"],
            }
        )
        transformer = CampaignTransformer()
        result = transformer.run(df)

        assert abs(result["ctr"].iloc[0] - 0.05) < 1e-6
        assert abs(result["cpc"].iloc[0] - 2.0) < 1e-6
        assert abs(result["roas"].iloc[0] - 5.0) < 1e-6

    def test_divide_by_zero_returns_zero(self):
        from etl.transformers.base_transformer import BaseTransformer

        s_num = pd.Series([10.0, 0.0])
        s_den = pd.Series([0.0, 0.0])
        result = BaseTransformer.safe_divide(s_num, s_den)
        assert (result == 0.0).all()


# ── BigQuery loader tests ─────────────────────────────────────────────────────
class TestBigQueryLoader:
    def test_empty_dataframe_skips_load(self):
        from etl.loaders.bigquery_loader import BigQueryLoader

        loader = BigQueryLoader.__new__(BigQueryLoader)
        loader._client = MagicMock()
        loader._project = "test_project"
        loader._dataset_raw = "raw"

        loader.load(pd.DataFrame(), table_name="test_table")
        loader._client.load_table_from_dataframe.assert_not_called()
