"""
data_quality/monitors/pipeline_monitor.py
Monitors pipeline health: data freshness, row counts, and null rates.
Sends Slack alerts when thresholds are breached.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import requests

from config.settings import get_settings
from etl.loaders.bigquery_loader import BigQueryLoader

logger = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────
FRESHNESS_THRESHOLDS = {
    "raw.google_ads_campaigns": 6,   # hours
    "raw.meta_ads_insights": 6,
    "raw.ga4_sessions": 6,
    "raw.hubspot_objects": 12,
    "raw.shopify_raw": 6,
    "raw.mixpanel_events": 12,
    "raw.mixpanel_segmentation": 12,
    "raw.mixpanel_funnel_conversions": 24,
}

ROW_COUNT_MINIMUMS = {
    "raw.google_ads_campaigns": 10,
    "raw.meta_ads_insights": 10,
    "raw.ga4_sessions": 100,
    "raw.mixpanel_events": 50,
}

NULL_RATE_MAXIMUMS = {
    "fct_campaign_performance": {
        "campaign_id": 0.0,
        "cost_usd": 0.0,
        "date": 0.0,
    },
    "fct_revenue": {
        "order_id": 0.0,
        "gross_revenue": 0.0,
    },
}


class PipelineMonitor:
    def __init__(self) -> None:
        self._loader = BigQueryLoader()
        self._settings = get_settings()

    def check_data_freshness(self, execution_date: str) -> None:
        project = self._settings.bigquery.project_id
        alerts: list[str] = []

        for table, max_hours in FRESHNESS_THRESHOLDS.items():
            dataset, table_name = table.split(".")
            sql = f"""
            SELECT
                TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', _extracted_at)), HOUR) as hours_since_load
            FROM `{project}.{dataset}.{table_name}`
            """
            try:
                df = self._loader.run_query(sql)
                hours = df["hours_since_load"].iloc[0] if not df.empty else None
                if hours is not None and hours > max_hours:
                    msg = f"⚠️ `{table}` last loaded {hours}h ago (threshold: {max_hours}h)"
                    alerts.append(msg)
                    logger.warning(msg)
            except Exception as exc:  # noqa: BLE001
                logger.error("Freshness check failed for %s: %s", table, exc)

        if alerts:
            self._send_slack_alert(
                f":clock3: *Data Freshness Alert* for `{execution_date}`:\n"
                + "\n".join(alerts)
            )

    def check_row_counts(self, execution_date: str) -> None:
        project = self._settings.bigquery.project_id
        alerts: list[str] = []

        for table, min_rows in ROW_COUNT_MINIMUMS.items():
            dataset, table_name = table.split(".")
            sql = f"""
            SELECT COUNT(*) as row_count
            FROM `{project}.{dataset}.{table_name}`
            WHERE DATE(_extracted_at) = '{execution_date}'
            """
            try:
                df = self._loader.run_query(sql)
                count = int(df["row_count"].iloc[0]) if not df.empty else 0
                if count < min_rows:
                    msg = f"⚠️ `{table}` has {count} rows for {execution_date} (minimum: {min_rows})"
                    alerts.append(msg)
                    logger.warning(msg)
            except Exception as exc:  # noqa: BLE001
                logger.error("Row count check failed for %s: %s", table, exc)

        if alerts:
            self._send_slack_alert(
                f":bar_chart: *Low Row Count Alert* for `{execution_date}`:\n"
                + "\n".join(alerts)
            )

    def check_null_rates(self, execution_date: str) -> None:
        project = self._settings.bigquery.project_id
        alerts: list[str] = []

        for table, column_thresholds in NULL_RATE_MAXIMUMS.items():
            for col, max_rate in column_thresholds.items():
                sql = f"""
                SELECT
                    SAFE_DIVIDE(COUNTIF({col} IS NULL), COUNT(*)) as null_rate
                FROM `{project}.marts.{table}`
                WHERE date = '{execution_date}'
                """
                try:
                    df = self._loader.run_query(sql)
                    null_rate = float(df["null_rate"].iloc[0]) if not df.empty else 0.0
                    if null_rate > max_rate:
                        msg = (
                            f"⚠️ `{table}.{col}` null rate = {null_rate:.1%} "
                            f"(threshold: {max_rate:.1%})"
                        )
                        alerts.append(msg)
                        logger.warning(msg)
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "Null rate check failed for %s.%s: %s", table, col, exc
                    )

        if alerts:
            self._send_slack_alert(
                f":null: *Null Rate Alert* for `{execution_date}`:\n"
                + "\n".join(alerts)
            )

    def _send_slack_alert(self, message: str) -> None:
        webhook_url = self._settings.slack.webhook_url
        if not webhook_url:
            logger.warning("Slack webhook not configured — skipping alert")
            return
        try:
            response = requests.post(
                webhook_url,
                json={"text": message},
                timeout=10,
            )
            response.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to send Slack alert: %s", exc)
