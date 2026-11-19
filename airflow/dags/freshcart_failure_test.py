
import os
import logging
import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.email import send_email


def task_failure_alert(context):
    task = context["task_instance"]

    recipient = os.getenv("AIRFLOW_ALERT_EMAIL")

    if not recipient:
        logging.error("AIRFLOW_ALERT_EMAIL is missing")
        return

    exception = context.get("exception", "Unknown error")

    send_email(
        to=recipient,
        subject=f"FreshCart Test Alert: {task.task_id} Failed",
        html_content=f"""
        <h2>FreshCart Failure Alert Test</h2>
        <p><b>DAG:</b> {task.dag_id}</p>
        <p><b>Task:</b> {task.task_id}</p>
        <p><b>Error:</b> {exception}</p>
        <p><a href="{task.log_url}">View Task Logs</a></p>
        """
    )

    logging.info("Test failure email sent.")


def intentional_failure():
    raise RuntimeError(
        "This is an intentional failure to test Gmail alerts."
    )


with DAG(
    dag_id="freshcart_failure_test",
    description="Test Gmail failure notifications",
    schedule=None,
    start_date=pendulum.datetime(
        2026, 11, 13, tz="Africa/Cairo"
    ),
    catchup=False,
    on_failure_callback=task_failure_alert,
    tags=["freshcart", "testing", "alerts"],
) as dag:

    fail_on_purpose = PythonOperator(
        task_id="fail_on_purpose",
        python_callable=intentional_failure,
    )
