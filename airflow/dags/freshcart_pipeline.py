
import logging
from pathlib import Path

import pendulum

from airflow.sdk import DAG
import os
from airflow.utils.email import send_email
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor


# --------------------------------------------------
# Configuration
# --------------------------------------------------

DAG_ID = "freshcart_daily_pipeline"

SUPPLIERS_DIR = Path("/opt/airflow/suppliers")

DBT_CONTAINER = "freshcart-dbt"
DBT_PROJECT_DIR = "/usr/app/freshcart_dbt"


# --------------------------------------------------
# Failure callback
# --------------------------------------------------

def task_failure_alert(context):
    task = context["task_instance"]

    dag_id = task.dag_id
    task_id = task.task_id
    execution_date = context.get("logical_date")
    exception = context.get("exception")
    log_url = task.log_url

    recipient = os.getenv("AIRFLOW_ALERT_EMAIL")

    subject = f"FreshCart Alert: {task_id} Failed"

    html_content = f"""
    <h2>FreshCart Pipeline Failed!</h2>

    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Task:</b> {task_id}</p>
    <p><b>Execution Date:</b> {execution_date}</p>
    <p><b>Error:</b> {exception}</p>

    <p>
        <a href="{log_url}">View Task Logs</a>
    </p>
    """

    if not recipient:
        logging.error("AIRFLOW_ALERT_EMAIL is not configured.")
        return

    send_email(
        to=recipient,
        subject=subject,
        html_content=html_content,
    )

    logging.info("Failure alert sent to %s", recipient)


# --------------------------------------------------
# Supplier files sensor
# --------------------------------------------------

def check_supplier_files():

    supplier_types = ["fresh", "general"]

    for supplier in supplier_types:

        folder = SUPPLIERS_DIR / supplier

        if not folder.exists():
            logging.info("Waiting for folder: %s", folder)
            return False

        files = [
            f for f in folder.rglob("*")
            if f.is_file() and not f.name.startswith(".")
        ]

        if not files:
            logging.info("Waiting for files in: %s", folder)
            return False

    logging.info("Supplier files are available.")
    return True


# --------------------------------------------------
# dbt execution function
# --------------------------------------------------

def run_dbt(select_path):

    import docker

    client = docker.from_env()

    container = client.containers.get(DBT_CONTAINER)

    command = [
        "dbt",
        "run",
        "--select",
        select_path
    ]

    logging.info("Running dbt command: %s", command)

    exit_code, output = container.exec_run(
        cmd=command,
        workdir=DBT_PROJECT_DIR
    )

    result = output.decode("utf-8", errors="replace")

    logging.info("dbt output:\n%s", result)

    if exit_code != 0:
        raise RuntimeError(
            f"dbt failed with exit code {exit_code}"
        )

    logging.info("dbt completed successfully: %s", select_path)


def run_dbt_silver():
    run_dbt("path:models/silver")


def run_dbt_gold():
    run_dbt("path:models/gold")


# --------------------------------------------------
# DAG definition
# --------------------------------------------------

with DAG(
    dag_id=DAG_ID,

    description="FreshCart Bronze to Silver to Gold pipeline",

    schedule="0 1 * * *",

    start_date=pendulum.datetime(
        2026, 11, 13, 0, 0,
        tz="Africa/Cairo"
    ),

    catchup=False,

    max_active_runs=1,

    default_args={
        "owner": "freshcart",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
        "on_failure_callback": task_failure_alert,
    },

    tags=["freshcart", "etl", "dbt"],

) as dag:

    # Wait for supplier files

    wait_for_supplier_files = PythonSensor(
        task_id="wait_for_supplier_files",
        python_callable=check_supplier_files,
        poke_interval=60,
        timeout=21600,
        mode="reschedule",
    )

    # Bronze ETL tasks

    load_app = BashOperator(
        task_id="load_app",
        bash_command="python /opt/airflow/etl/load_app.py",
    )

    load_fx = BashOperator(
        task_id="load_fx",
        bash_command="python /opt/airflow/etl/load_fx.py",
    )

    load_suppliers = BashOperator(
        task_id="load_suppliers",
        bash_command="python /opt/airflow/etl/load_suppliers.py",
    )

    # Silver transformation

    dbt_silver = PythonOperator(
        task_id="dbt_silver",
        python_callable=run_dbt_silver,
    )

    # Gold transformation

    dbt_gold = PythonOperator(
        task_id="dbt_gold",
        python_callable=run_dbt_gold,
    )

    # --------------------------------------------------
    # Dependencies
    # --------------------------------------------------

    wait_for_supplier_files >> load_suppliers

    load_app >> dbt_silver
    load_fx >> dbt_silver
    load_suppliers >> dbt_silver

    dbt_silver >> dbt_gold
