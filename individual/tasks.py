import os
from extract import are_files_relevant, scrape_logs
from transform import etl_pipeline
from load import load_to_analytical_store, mark_as_processed
from analyze import perform_analysis

# Constants
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data/uav_analytics.db")
RAW_DIR = os.path.join(BASE_DIR, "data/raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data/processed")
PLOTS_DIR = os.path.join(BASE_DIR, "plots")


def extract_task(max_downloads=5):
    """Scrapes logs and returns the directory where they are stored."""
    print(f"Starting extraction to {RAW_DIR}...")
    if are_files_relevant(RAW_DIR):
        print("All files are currently relevant")
        return {"raw_dir": RAW_DIR, "count": 0}

    num_downloaded = scrape_logs(staging_dir=RAW_DIR, max_downloads=max_downloads)
    return {"raw_dir": RAW_DIR, "count": num_downloaded}


def transform_load_task(raw_dir):
    """Runs ETL and loads to DB. Returns path to the processed CSV."""
    print(f"Starting Transform & Load from {raw_dir}...")
    df, processed_files = etl_pipeline(
        input_dir=raw_dir, output_dir=PROCESSED_DIR, db_path=DB_PATH
    )

    if df is not None and not df.empty:
        csv_path = os.path.join(PROCESSED_DIR, "telemetry_dataset.csv")
        df.to_csv(csv_path, index=False)

        if load_to_analytical_store(df, db_path=DB_PATH):
            mark_as_processed(processed_files, db_path=DB_PATH)
            return csv_path

    print("No new data to process.")
    return None


def analyze_task(csv_path):
    """Runs ML and BI analysis on the processed data."""
    if not csv_path or not os.path.exists(csv_path):
        print("Skipping analysis: No valid CSV path provided.")
        return

    print(f"Starting Analysis on {csv_path}...")
    perform_analysis(input_file=csv_path, db_path=DB_PATH, output_dir=PLOTS_DIR)
    return PLOTS_DIR
