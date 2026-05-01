import sys

# Add the individual directory to sys.path so Airflow can import your modules
# This assumes the DAG is running on a worker where the code is available
INDIVIDUAL_PATH = "/home/yur4uwe/uni/engeneering-data/individual"
if INDIVIDUAL_PATH not in sys.path:
    sys.path.append(INDIVIDUAL_PATH)

# Module imports were moved below to establish correct pash
from datetime import datetime, timedelta  # noqa: E402
from airflow.decorators import dag, task  # noqa: E402
from tasks import extract_task, transform_load_task, analyze_task  # noqa: E402


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    "uav_telemetry_pipeline",
    default_args=default_args,
    description="Native Python UAV Telemetry Pipeline",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["uav", "ml"],
)
def uav_pipeline():

    @task
    def extract():
        # Calls the function from tasks.py
        result = extract_task(max_downloads=5)
        return result["raw_dir"]  # XCom will store this string

    @task
    def transform_load(raw_dir_path):
        # Receives raw_dir from the previous task via XCom
        csv_path = transform_load_task(raw_dir_path)
        return csv_path

    @task
    def analyze(csv_file_path):
        # Receives csv_path from the previous task via XCom
        analyze_task(csv_file_path)

    # Define the flow
    raw_path = extract()
    csv_path = transform_load(raw_path)
    analyze(csv_path)


# Instantiate the DAG
uav_dag = uav_pipeline()
