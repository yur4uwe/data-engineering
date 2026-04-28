# NOTE: This DAG is an original to which symlink points in dedicated DAGs
# directory in the arflow HOME directory.
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

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
    description="Automated UAV Telemetry Analytics Pipeline",
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Base directory for the pipeline scripts
# dags -> airflow -> root -> individual
BASE_DIR = Path(__file__).parent.parent.parent / "individual"

# We use Airflow's exit code 99 to signal a skip in newer Airflow versions, 
# or we just let it fail and handle the "preemptive" logic inside the scripts.
# However, to meet the requirement of "preemptively ending" gracefully, 
# we'll use the ShortCircuitOperator again but correctly defined.

from airflow.operators.python import ShortCircuitOperator

def run_script(script_name):
    import subprocess
    script_path = os.path.join(BASE_DIR, script_name)
    print(f"Executing: python {script_path}")
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    print(f"STDOUT: {result.stdout}")
    print(f"STDERR: {result.stderr}")
    # Return True if returncode is 0, which allows the DAG to continue.
    # Return False if returncode is non-zero (like 1 when no data), which short-circuits.
    return result.returncode == 0

extract_task = ShortCircuitOperator(
    task_id="extract_data",
    python_callable=run_script,
    op_args=["extract.py"],
    dag=dag,
)

transform_task = ShortCircuitOperator(
    task_id="transform_data",
    python_callable=run_script,
    op_args=["transform.py"],
    dag=dag,
)

analyze_task = BashOperator(
    task_id="analyze_data",
    bash_command=f"python {os.path.join(BASE_DIR, 'analyze.py')}",
    dag=dag,
)

extract_task >> transform_task >> analyze_task
