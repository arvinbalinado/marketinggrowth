"""
dags/mixpanel_pipeline_dag.py
Daily ingestion of Mixpanel raw events, segmentation metrics, and funnel
conversions into BigQuery, followed by a dbt refresh of Mixpanel mart models.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from etl.extractors import (
    MixpanelEventExtractor,
    MixpanelSegmentExtractor,
    MixpanelFunnelExtractor,
)
from etl.loaders import BigQueryLoader
from etl.pipeline import Pipeline

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
}


def _run_mixpanel_events(**context: dict) -> None:
    """Extract raw Mixpanel events via the Data Export API (NDJSON streaming)."""
    loader = BigQueryLoader()
    pipeline = Pipeline(
        extractor=MixpanelEventExtractor(lookback_days=2),
        loader=loader,
        table_name="mixpanel_events",
        write_mode="merge",
        merge_keys=["insert_id", "event_date"],
        partition_field="event_date",
    )
    result = pipeline.run()
    if not result.success:
        raise RuntimeError(f"Mixpanel events pipeline failed: {result.error}")


def _run_mixpanel_segmentation(**context: dict) -> None:
    """Extract aggregated Mixpanel segmentation metrics via the Query API."""
    loader = BigQueryLoader()
    pipeline = Pipeline(
        extractor=MixpanelSegmentExtractor(lookback_days=2),
        loader=loader,
        table_name="mixpanel_segmentation",
        write_mode="merge",
        merge_keys=["event", "segment_property", "segment_value", "date"],
    )
    result = pipeline.run()
    if not result.success:
        raise RuntimeError(f"Mixpanel segmentation pipeline failed: {result.error}")


def _run_mixpanel_funnels(**context: dict) -> None:
    """Extract Mixpanel funnel step conversion data via the Funnels API."""
    loader = BigQueryLoader()
    pipeline = Pipeline(
        extractor=MixpanelFunnelExtractor(lookback_days=7),
        loader=loader,
        table_name="mixpanel_funnel_conversions",
        write_mode="merge",
        merge_keys=["funnel_id", "date", "step_index"],
    )
    result = pipeline.run()
    if not result.success:
        raise RuntimeError(f"Mixpanel funnels pipeline failed: {result.error}")


with DAG(
    dag_id="mixpanel_analytics_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily Mixpanel event export, segmentation, and funnel ingestion",
    schedule_interval="30 7 * * *",  # 07:30 UTC — after ecommerce DAG
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mixpanel", "behavioral", "events", "funnels", "etl"],
    max_active_runs=1,
) as dag:

    events_task = PythonOperator(
        task_id="extract_mixpanel_events",
        python_callable=_run_mixpanel_events,
    )

    segmentation_task = PythonOperator(
        task_id="extract_mixpanel_segmentation",
        python_callable=_run_mixpanel_segmentation,
    )

    funnels_task = PythonOperator(
        task_id="extract_mixpanel_funnels",
        python_callable=_run_mixpanel_funnels,
    )

    # Run all three extractions in parallel, then refresh dbt models
    dbt_mixpanel = BashOperator(
        task_id="dbt_run_mixpanel_models",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run "
            "--select "
            "staging.stg_mixpanel_events "
            "staging.stg_mixpanel_segmentation "
            "staging.stg_mixpanel_funnels "
            "marts.marketing.fct_mixpanel_events "
            "marts.marketing.fct_mixpanel_campaign_attribution "
            "marts.growth.fct_mixpanel_funnel "
            "--profiles-dir /opt/airflow/dbt --no-use-colors"
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    dbt_test_mixpanel = BashOperator(
        task_id="dbt_test_mixpanel_models",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt test "
            "--select "
            "staging.stg_mixpanel_events "
            "staging.stg_mixpanel_segmentation "
            "staging.stg_mixpanel_funnels "
            "marts.marketing.fct_mixpanel_events "
            "marts.growth.fct_mixpanel_funnel "
            "--profiles-dir /opt/airflow/dbt --no-use-colors"
        ),
    )

    [events_task, segmentation_task, funnels_task] >> dbt_mixpanel >> dbt_test_mixpanel
