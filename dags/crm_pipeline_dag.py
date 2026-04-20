"""
dags/crm_pipeline_dag.py
Daily Airflow DAG for CRM data ingestion:
HubSpot contacts/deals/companies + (optional) Salesforce leads/opportunities.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from etl.extractors import HubSpotExtractor, SalesforceExtractor
from etl.loaders import BigQueryLoader
from etl.pipeline import Pipeline

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

LOOKBACK_DAYS = 7  # CRM data can arrive late


def _run_hubspot(**context: dict) -> None:
    loader = BigQueryLoader()
    pipeline = Pipeline(
        extractor=HubSpotExtractor(lookback_days=LOOKBACK_DAYS),
        loader=loader,
        table_name="hubspot_objects",
        write_mode="merge",
        merge_keys=["id", "object_type"],
    )
    result = pipeline.run()
    if not result.success:
        raise RuntimeError(f"HubSpot pipeline failed: {result.error}")


def _run_salesforce(**context: dict) -> None:
    loader = BigQueryLoader()
    pipeline = Pipeline(
        extractor=SalesforceExtractor(lookback_days=LOOKBACK_DAYS),
        loader=loader,
        table_name="salesforce_objects",
        write_mode="merge",
        merge_keys=["Id", "object_type"],
    )
    result = pipeline.run()
    if not result.success:
        raise RuntimeError(f"Salesforce pipeline failed: {result.error}")


with DAG(
    dag_id="crm_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily CRM data ingestion (HubSpot & Salesforce)",
    schedule_interval="0 5 * * *",  # 05:00 UTC — before paid media DAG
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crm", "hubspot", "salesforce", "etl"],
    max_active_runs=1,
) as dag:

    hubspot_task = PythonOperator(
        task_id="extract_hubspot",
        python_callable=_run_hubspot,
    )

    salesforce_task = PythonOperator(
        task_id="extract_salesforce",
        python_callable=_run_salesforce,
    )

    dbt_crm = BashOperator(
        task_id="dbt_run_crm_marts",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --select marts.growth --profiles-dir /opt/airflow/dbt --no-use-colors"
        ),
    )

    [hubspot_task, salesforce_task] >> dbt_crm
