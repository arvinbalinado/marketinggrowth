"""
data_quality/validators/marketing_validators.py
Custom SQL-based data quality checks for marketing pipeline tables.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from etl.loaders.bigquery_loader import BigQueryLoader

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    check_name: str
    table: str
    passed: bool
    details: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


class MarketingDataValidator:
    """
    Runs targeted data quality assertions against BigQuery tables.
    Each check returns a CheckResult indicating pass/fail.
    """

    def __init__(self) -> None:
        self._loader = BigQueryLoader()

    def run_all_checks(self, execution_date: str) -> list[CheckResult]:
        checks = [
            self.check_no_negative_spend(execution_date),
            self.check_no_future_dates(execution_date),
            self.check_ctr_bounds(execution_date),
            self.check_roas_not_astronomical(execution_date),
            self.check_funnel_stage_order(execution_date),
            self.check_revenue_not_negative(execution_date),
            self.check_duplicate_campaign_rows(execution_date),
            self.check_mixpanel_event_counts(execution_date),
            self.check_no_future_mixpanel_events(execution_date),
        ]
        failed = [c for c in checks if not c.passed]
        logger.info(
            "Data quality: %d/%d checks passed",
            len(checks) - len(failed),
            len(checks),
        )
        for f in failed:
            logger.warning("FAILED check '%s': %s", f.check_name, f.details)
        return checks

    # ── Individual checks ─────────────────────────────────────────────────────

    def check_no_negative_spend(self, execution_date: str) -> CheckResult:
        sql = f"""
        SELECT COUNT(*) as bad_rows
        FROM `{{project}}.marts.fct_campaign_performance`
        WHERE date = '{execution_date}'
          AND cost_usd < 0
        """
        return self._run_count_check(
            "no_negative_spend",
            "fct_campaign_performance",
            sql,
            expected_count=0,
        )

    def check_no_future_dates(self, execution_date: str) -> CheckResult:
        sql = f"""
        SELECT COUNT(*) as bad_rows
        FROM `{{project}}.raw.google_ads_campaigns`
        WHERE date > CURRENT_DATE()
        """
        return self._run_count_check(
            "no_future_dates", "google_ads_campaigns", sql, expected_count=0
        )

    def check_ctr_bounds(self, execution_date: str) -> CheckResult:
        sql = f"""
        SELECT COUNT(*) as bad_rows
        FROM `{{project}}.marts.fct_campaign_performance`
        WHERE date = '{execution_date}'
          AND ctr IS NOT NULL
          AND (ctr < 0 OR ctr > 1)
        """
        return self._run_count_check(
            "ctr_within_bounds", "fct_campaign_performance", sql, expected_count=0
        )

    def check_roas_not_astronomical(self, execution_date: str) -> CheckResult:
        """Flag suspiciously high ROAS (>1000x) that likely indicates bad data."""
        sql = f"""
        SELECT COUNT(*) as bad_rows
        FROM `{{project}}.marts.fct_campaign_performance`
        WHERE date = '{execution_date}'
          AND roas > 1000
          AND cost_usd > 1
        """
        return self._run_count_check(
            "roas_not_astronomical",
            "fct_campaign_performance",
            sql,
            expected_count=0,
        )

    def check_funnel_stage_order(self, execution_date: str) -> CheckResult:
        """Ensures mqls <= total_leads, sqls <= mqls, etc."""
        sql = f"""
        SELECT COUNT(*) as bad_rows
        FROM `{{project}}.marts.fct_funnel_conversion`
        WHERE week_start >= DATE_SUB('{execution_date}', INTERVAL 7 DAY)
          AND (
              mqls > total_leads
              OR sqls > mqls
              OR customers > opportunities
          )
        """
        return self._run_count_check(
            "funnel_stage_order",
            "fct_funnel_conversion",
            sql,
            expected_count=0,
        )

    def check_revenue_not_negative(self, execution_date: str) -> CheckResult:
        sql = f"""
        SELECT COUNT(*) as bad_rows
        FROM `{{project}}.marts.fct_revenue`
        WHERE order_date = '{execution_date}'
          AND (gross_revenue < 0 OR net_revenue < 0)
        """
        return self._run_count_check(
            "revenue_not_negative", "fct_revenue", sql, expected_count=0
        )

    def check_duplicate_campaign_rows(self, execution_date: str) -> CheckResult:
        sql = f"""
        SELECT COUNT(*) as bad_rows
        FROM (
            SELECT campaign_id, ad_group_id, date, device, COUNT(*) as cnt
            FROM `{{project}}.marts.fct_campaign_performance`
            WHERE date = '{execution_date}'
            GROUP BY 1, 2, 3, 4
            HAVING cnt > 1
        )
        """
        return self._run_count_check(
            "no_duplicate_campaign_rows",
            "fct_campaign_performance",
            sql,
            expected_count=0,
        )

    def check_mixpanel_event_counts(self, execution_date: str) -> CheckResult:
        """Ensure at least 50 Mixpanel events were loaded for the execution date."""
        sql = f"""
        SELECT COUNT(*) as event_count
        FROM `{{project}}.raw.mixpanel_events`
        WHERE event_date = '{execution_date}'
        """
        try:
            from config.settings import get_settings

            project = get_settings().bigquery.project_id
            resolved_sql = sql.replace("{project}", project)
            df = self._loader.run_query(resolved_sql)
            count = int(df["event_count"].iloc[0]) if not df.empty else 0
            return CheckResult(
                check_name="mixpanel_minimum_event_count",
                table="mixpanel_events",
                passed=(count >= 50),
                details={"event_count": count, "minimum": 50},
            )
        except Exception as exc:  # noqa: BLE001
            return CheckResult(
                check_name="mixpanel_minimum_event_count",
                table="mixpanel_events",
                passed=False,
                error=str(exc),
            )

    def check_no_future_mixpanel_events(self, execution_date: str) -> CheckResult:
        """Flag any Mixpanel events timestamped in the future (clock skew / bad data)."""
        sql = f"""
        SELECT COUNT(*) as bad_rows
        FROM `{{project}}.raw.mixpanel_events`
        WHERE event_date > '{execution_date}'
        """
        return self._run_count_check(
            "no_future_mixpanel_events",
            "mixpanel_events",
            sql,
            expected_count=0,
        )

    # ── Helper ────────────────────────────────────────────────────────────────

    def _run_count_check(
        self,
        check_name: str,
        table: str,
        sql: str,
        expected_count: int,
    ) -> CheckResult:
        from config.settings import get_settings

        project = get_settings().bigquery.project_id
        resolved_sql = sql.replace("{project}", project)
        try:
            df = self._loader.run_query(resolved_sql)
            actual = int(df["bad_rows"].iloc[0]) if not df.empty else 0
            return CheckResult(
                check_name=check_name,
                table=table,
                passed=(actual == expected_count),
                details={"bad_rows": actual, "expected": expected_count},
            )
        except Exception as exc:  # noqa: BLE001
            return CheckResult(
                check_name=check_name,
                table=table,
                passed=False,
                error=str(exc),
            )
