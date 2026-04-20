"""
dags/ecommerce_analytics_dag.py
Ingests Shopify orders + customers and GA4 session data, then runs
revenue & funnel dbt models.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from etl.extractors import ShopifyExtractor, GoogleAnalyticsExtractor
from etl.loaders import BigQueryLoader
from etl.pipeline import Pipeline

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def _run_shopify(**context: dict) -> None:
    loader = BigQueryLoader()
    pipeline = Pipeline(
        extractor=ShopifyExtractor(lookback_days=3),
        loader=loader,
        table_name="shopify_raw",
        write_mode="merge",
        merge_keys=["order_id", "object_type"],
        partition_field="created_at",
    )
    result = pipeline.run()
    if not result.success:
        raise RuntimeError(f"Shopify pipeline failed: {result.error}")


def _run_ga4(**context: dict) -> None:
    loader = BigQueryLoader()
    pipeline = Pipeline(
        extractor=GoogleAnalyticsExtractor(lookback_days=3),
        loader=loader,
        table_name="ga4_sessions",
        write_mode="merge",
        merge_keys=["date", "sessionSource", "sessionMedium", "sessionCampaignName", "deviceCategory"],
    )
    result = pipeline.run()
    if not result.success:
        raise RuntimeError(f"GA4 pipeline failed: {result.error}")


with DAG(
    dag_id="ecommerce_analytics_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily e-commerce and web analytics ingestion",
    schedule_interval="0 7 * * *",  # 07:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "shopify", "ga4", "etl"],
    max_active_runs=1,
) as dag:

    shopify_task = PythonOperator(
        task_id="extract_shopify",
        python_callable=_run_shopify,
    )

    ga4_task = PythonOperator(
        task_id="extract_ga4",
        python_callable=_run_ga4,
    )

    dbt_revenue = BashOperator(
        task_id="dbt_run_revenue_marts",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --select marts.revenue --profiles-dir /opt/airflow/dbt --no-use-colors"
        ),
    )

    dbt_attribution = BashOperator(
        task_id="dbt_run_attribution",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --select marts.attribution --profiles-dir /opt/airflow/dbt --no-use-colors"
        ),
    )

    [shopify_task, ga4_task] >> dbt_revenue >> dbt_attribution
