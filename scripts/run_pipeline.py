"""
scripts/run_pipeline.py
CLI tool to manually trigger a specific pipeline outside of Airflow.
Usage:
  python scripts/run_pipeline.py --source google_ads --start-date 2026-01-01 --end-date 2026-04-20
  python scripts/run_pipeline.py --source all --start-date 2026-04-01
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime

from etl.extractors import (
    GoogleAdsExtractor,
    MetaAdsExtractor,
    LinkedInAdsExtractor,
    GoogleAnalyticsExtractor,
    HubSpotExtractor,
    ShopifyExtractor,
)
from etl.transformers import CampaignTransformer
from etl.loaders import BigQueryLoader
from etl.pipeline import Pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

SOURCE_MAP = {
    "google_ads": {
        "extractor": lambda: GoogleAdsExtractor(),
        "transformer": lambda: CampaignTransformer(),
        "table": "google_ads_campaigns",
        "merge_keys": ["campaign_id", "ad_group_id", "date", "device"],
    },
    "meta_ads": {
        "extractor": lambda: MetaAdsExtractor(),
        "transformer": lambda: CampaignTransformer(),
        "table": "meta_ads_insights",
        "merge_keys": ["ad_id", "date"],
    },
    "linkedin_ads": {
        "extractor": lambda: LinkedInAdsExtractor(),
        "transformer": lambda: CampaignTransformer(),
        "table": "linkedin_ads_analytics",
        "merge_keys": ["pivot_value", "date_start"],
    },
    "ga4": {
        "extractor": lambda: GoogleAnalyticsExtractor(),
        "transformer": None,
        "table": "ga4_sessions",
        "merge_keys": ["date", "sessionSource", "sessionMedium", "sessionCampaignName", "deviceCategory"],
    },
    "hubspot": {
        "extractor": lambda: HubSpotExtractor(),
        "transformer": None,
        "table": "hubspot_objects",
        "merge_keys": ["id", "object_type"],
    },
    "shopify": {
        "extractor": lambda: ShopifyExtractor(),
        "transformer": None,
        "table": "shopify_raw",
        "merge_keys": ["order_id", "object_type"],
    },
}


def run_source(source: str, start_date: date, end_date: date) -> bool:
    config = SOURCE_MAP[source]
    loader = BigQueryLoader()
    transformer = config["transformer"]() if config["transformer"] else None

    pipeline = Pipeline(
        extractor=config["extractor"](),
        loader=loader,
        transformer=transformer,
        table_name=config["table"],
        write_mode="merge",
        merge_keys=config["merge_keys"],
    )

    result = pipeline.run(start_date=start_date, end_date=end_date)
    if result.success:
        logger.info("✓ %s: %d rows loaded", source, result.rows_loaded)
    else:
        logger.error("✗ %s failed: %s", source, result.error)
    return result.success


def main() -> None:
    parser = argparse.ArgumentParser(description="Run marketing data pipeline")
    parser.add_argument(
        "--source",
        choices=list(SOURCE_MAP.keys()) + ["all"],
        required=True,
        help="Data source to ingest",
    )
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        required=True,
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=date.today(),
    )
    args = parser.parse_args()

    sources = list(SOURCE_MAP.keys()) if args.source == "all" else [args.source]
    all_success = True

    for source in sources:
        success = run_source(source, args.start_date, args.end_date)
        all_success = all_success and success

    sys.exit(0 if all_success else 1)


if __name__ == "__main__":
    main()
