"""
dags/data_quality_dag.py
Runs Great Expectations checkpoints and custom SQL validators
after all ingestion pipelines complete each day.
Sends Slack alerts on failures.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from data_quality.validators.marketing_validators import MarketingDataValidator
from data_quality.monitors.pipeline_monitor import PipelineMonitor

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _run_validators(**context: dict) -> None:
    validator = MarketingDataValidator()
    results = validator.run_all_checks(execution_date=context["ds"])
    failed = [r for r in results if not r["passed"]]
    if failed:
        context["ti"].xcom_push(key="validation_failures", value=failed)
        raise ValueError(
            f"{len(failed)} data quality checks failed: "
            + str([f["check_name"] for f in failed])
        )


def _run_monitors(**context: dict) -> None:
    monitor = PipelineMonitor()
    monitor.check_data_freshness(context["ds"])
    monitor.check_row_counts(context["ds"])
    monitor.check_null_rates(context["ds"])


with DAG(
    dag_id="data_quality_checks",
    default_args=DEFAULT_ARGS,
    description="Post-ingestion data quality validation and monitoring",
    schedule_interval="0 9 * * *",  # 09:00 UTC — after all ingestion DAGs
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["data-quality", "monitoring"],
    max_active_runs=1,
) as dag:

    validate_task = PythonOperator(
        task_id="run_data_validators",
        python_callable=_run_validators,
    )

    monitor_task = PythonOperator(
        task_id="run_pipeline_monitors",
        python_callable=_run_monitors,
    )

    alert_on_failure = SlackWebhookOperator(
        task_id="alert_data_quality_failure",
        slack_webhook_conn_id="slack_default",
        message=(
            ":warning: *Data Quality Alert* — checks failed for `{{ ds }}`. "
            "Failures: `{{ ti.xcom_pull(task_ids='run_data_validators', key='validation_failures') }}`"
        ),
        trigger_rule="one_failed",
    )

    [validate_task, monitor_task] >> alert_on_failure
