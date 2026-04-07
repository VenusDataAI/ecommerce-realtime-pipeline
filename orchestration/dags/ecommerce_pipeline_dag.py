"""
Airflow DAG: ecommerce_pipeline
Runs every 15 minutes.

Tasks:
  consume_kinesis_batch
  → validate_and_write_bronze
  → trigger_dbt_silver
  → trigger_dbt_gold
  → run_pipeline_health_check
  → notify_slack_on_failure  (trigger_rule=one_failed)
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

try:
    from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
    _SLACK_AVAILABLE = True
except ImportError:
    _SLACK_AVAILABLE = False

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def consume_kinesis_batch(**ctx):
    """Read one batch of events from Kinesis/local and route through processor."""
    import sys
    sys.path.insert(0, Variable.get("pipeline_root", default_var="/opt/pipeline"))

    from consumer.event_processor import EventProcessor
    from consumer.kinesis_consumer import KinesisConsumer
    from storage.redshift_writer import RedshiftWriter
    from storage.s3_checkpoint import S3Checkpoint

    dsn = Variable.get("redshift_dsn", default_var=os.environ.get("REDSHIFT_DSN", ""))
    writer = RedshiftWriter(dsn=dsn)
    processor = EventProcessor(
        flush_size=int(Variable.get("flush_size", default_var="500")),
        flush_interval=float(Variable.get("flush_interval", default_var="30")),
        writer=writer.write,
    )
    checkpoint = S3Checkpoint()
    consumer = KinesisConsumer(
        stream_name=Variable.get("kinesis_stream", default_var="ecommerce-events"),
        checkpoint=checkpoint,
        processor=processor,
    )
    stats = consumer.consume_batch(max_records=5000)
    ctx["ti"].xcom_push(key="consume_stats", value=stats)
    return stats


def validate_and_write_bronze(**ctx):
    """Ensure bronze DDL tables exist in Redshift."""
    import sys
    sys.path.insert(0, Variable.get("pipeline_root", default_var="/opt/pipeline"))

    from storage.redshift_writer import RedshiftWriter
    dsn = Variable.get("redshift_dsn", default_var=os.environ.get("REDSHIFT_DSN", ""))
    writer = RedshiftWriter(dsn=dsn)
    writer.ensure_tables()
    return {"status": "bronze_ready"}


def trigger_dbt_silver(**ctx):
    """Run dbt silver-layer models."""
    import subprocess
    project_dir = Variable.get("dbt_project_dir", default_var="/opt/pipeline/dbt")
    result = subprocess.run(
        ["dbt", "run", "--select", "silver", "--project-dir", project_dir],
        capture_output=True, text=True, check=True,
    )
    return {"stdout": result.stdout[-2000:], "returncode": result.returncode}


def trigger_dbt_gold(**ctx):
    """Run dbt gold-layer models."""
    import subprocess
    project_dir = Variable.get("dbt_project_dir", default_var="/opt/pipeline/dbt")
    result = subprocess.run(
        ["dbt", "run", "--select", "gold", "--project-dir", project_dir],
        capture_output=True, text=True, check=True,
    )
    return {"stdout": result.stdout[-2000:], "returncode": result.returncode}


def run_pipeline_health_check(**ctx):
    """Run health monitor; push GREEN/YELLOW/RED status to XCom."""
    import sys
    sys.path.insert(0, Variable.get("pipeline_root", default_var="/opt/pipeline"))

    from monitoring.pipeline_health import PipelineHealthMonitor
    monitor = PipelineHealthMonitor()
    health = monitor.check()
    ctx["ti"].xcom_push(key="health_status", value=health.status.value)
    ctx["ti"].xcom_push(key="health_details", value=health.details)
    return {"status": health.status.value}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    description="Real-time e-commerce analytics pipeline (Kinesis → Redshift → dbt)",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "realtime", "analytics"],
) as dag:

    t_consume = PythonOperator(
        task_id="consume_kinesis_batch",
        python_callable=consume_kinesis_batch,
    )

    t_bronze = PythonOperator(
        task_id="validate_and_write_bronze",
        python_callable=validate_and_write_bronze,
    )

    t_silver = PythonOperator(
        task_id="trigger_dbt_silver",
        python_callable=trigger_dbt_silver,
    )

    t_gold = PythonOperator(
        task_id="trigger_dbt_gold",
        python_callable=trigger_dbt_gold,
    )

    t_health = PythonOperator(
        task_id="run_pipeline_health_check",
        python_callable=run_pipeline_health_check,
        trigger_rule="all_done",
    )

    if _SLACK_AVAILABLE:
        t_slack = SlackWebhookOperator(
            task_id="notify_slack_on_failure",
            slack_webhook_conn_id="slack_webhook",
            message=(
                "Pipeline {{ dag.dag_id }} FAILED on {{ ds }}. "
                "Check Airflow logs for details."
            ),
            trigger_rule="one_failed",
        )
    else:
        # Fallback if Slack provider not installed
        t_slack = PythonOperator(
            task_id="notify_slack_on_failure",
            python_callable=lambda **kw: print("Slack provider not available — skipping notification"),
            trigger_rule="one_failed",
        )

    # Wiring
    t_consume >> t_bronze >> t_silver >> t_gold >> t_health >> t_slack
