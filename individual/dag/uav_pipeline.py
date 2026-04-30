import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow import DAG

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "uav_telemetry_pipeline",
    default_args=default_args,
    description="Multi-stage UAV Telemetry Analytics Pipeline",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

BASE_DIR = Path(__file__).parent.parent.parent / "individual"
PIPELINE_SCRIPT = os.path.join(BASE_DIR, "pipeline.py")
PYTHON_EXEC = "/home/yur4uwe/uni/engeneering-data/.venv/bin/python"

# Task 1: Extract (Scraping)
extract_task = BashOperator(
    task_id="extract",
    bash_command=f"{PYTHON_EXEC} {PIPELINE_SCRIPT} --step extract --max-downloads 5",
    dag=dag,
)

# Task 2: Transform & Load (ETL + Checkpointing)
# Note: This is where we ensure files stay in 'raw' by NOT passing --backup
transform_load_task = BashOperator(
    task_id="transform_load",
    bash_command=f"{PYTHON_EXEC} {PIPELINE_SCRIPT} --step transform_load",
    dag=dag,
)

# Task 3: Analyze (ML + BI)
analyze_task = BashOperator(
    task_id="analyze",
    bash_command=f"{PYTHON_EXEC} {PIPELINE_SCRIPT} --step analyze",
    dag=dag,
)

extract_task >> transform_load_task >> analyze_task
