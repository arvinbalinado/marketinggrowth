"""
dags/marketing_pipeline_dag.py
Daily Airflow DAG that ingests paid media data from
Google Ads, Meta Ads, and LinkedIn Ads into BigQuery raw layer,
then triggers dbt to refresh staging and mart models.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from etl.extractors import (
    GoogleAdsExtractor,
    MetaAdsExtractor,
    LinkedInAdsExtractor,
)
from etl.transformers import CampaignTransformer
from etl.loaders import BigQueryLoader
from etl.pipeline import Pipeline

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

LOOKBACK_DAYS = 2  # re-pull last 2 days to catch late-arriving data


def _run_google_ads(**context: dict) -> None:
    execution_date = context["ds"]
    loader = BigQueryLoader()
    pipeline = Pipeline(
        extractor=GoogleAdsExtractor(lookback_days=LOOKBACK_DAYS),
        loader=loader,
        transformer=CampaignTransformer(),
        table_name="google_ads_campaigns",
        write_mode="merge",
        merge_keys=["campaign_id", "ad_group_id", "date", "device"],
    )
    result = pipeline.run()
    if not result.success:
        raise RuntimeError(f"Google Ads pipeline failed: {result.error}")
    context["ti"].xcom_push(key="google_ads_rows", value=result.rows_loaded)


def _run_meta_ads(**context: dict) -> None:
    loader = BigQueryLoader()
    pipeline = Pipeline(
        extractor=MetaAdsExtractor(lookback_days=LOOKBACK_DAYS),
        loader=loader,
        transformer=CampaignTransformer(),
        table_name="meta_ads_insights",
        write_mode="merge",
        merge_keys=["ad_id", "date"],
    )
    result = pipeline.run()
    if not result.success:
        raise RuntimeError(f"Meta Ads pipeline failed: {result.error}")
    context["ti"].xcom_push(key="meta_ads_rows", value=result.rows_loaded)


def _run_linkedin_ads(**context: dict) -> None:
    loader = BigQueryLoader()
    pipeline = Pipeline(
        extractor=LinkedInAdsExtractor(lookback_days=LOOKBACK_DAYS),
        loader=loader,
        transformer=CampaignTransformer(),
        table_name="linkedin_ads_analytics",
        write_mode="merge",
        merge_keys=["pivot_value", "date_start"],
    )
    result = pipeline.run()
    if not result.success:
        raise RuntimeError(f"LinkedIn Ads pipeline failed: {result.error}")


with DAG(
    dag_id="marketing_paid_media_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily ingestion of paid media data from all ad platforms",
    schedule_interval="0 6 * * *",  # 06:00 UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["marketing", "paid-media", "etl"],
    max_active_runs=1,
) as dag:

    google_ads_task = PythonOperator(
        task_id="extract_google_ads",
        python_callable=_run_google_ads,
    )

    meta_ads_task = PythonOperator(
        task_id="extract_meta_ads",
        python_callable=_run_meta_ads,
    )

    linkedin_ads_task = PythonOperator(
        task_id="extract_linkedin_ads",
        python_callable=_run_linkedin_ads,
    )

    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --select staging --profiles-dir /opt/airflow/dbt --no-use-colors"
        ),
    )

    dbt_marts = BashOperator(
        task_id="dbt_run_marketing_marts",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --select marts.marketing --profiles-dir /opt/airflow/dbt --no-use-colors"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt test --select marketing --profiles-dir /opt/airflow/dbt --no-use-colors"
        ),
    )

    slack_success = SlackWebhookOperator(
        task_id="notify_slack_success",
        slack_webhook_conn_id="slack_default",
        message=(
            ":white_check_mark: *Marketing Paid Media Pipeline* completed successfully "
            "for `{{ ds }}`."
        ),
        trigger_rule="all_success",
    )

    slack_failure = SlackWebhookOperator(
        task_id="notify_slack_failure",
        slack_webhook_conn_id="slack_default",
        message=(
            ":red_circle: *Marketing Paid Media Pipeline* FAILED for `{{ ds }}`. "
            "Check Airflow logs."
        ),
        trigger_rule="one_failed",
    )

    # ── Dependency graph ──────────────────────────────────────────────────────
    [google_ads_task, meta_ads_task, linkedin_ads_task] >> dbt_staging
    dbt_staging >> dbt_marts >> dbt_test
    dbt_test >> [slack_success, slack_failure]
