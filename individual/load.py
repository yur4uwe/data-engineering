import os
import pandas as pd
import sqlite3
import shutil
from datetime import datetime

def load_to_analytical_store(df, db_path="data/uav_analytics.db", table_name="telemetry"):
    """
    Loads the structured dataframe into a SQLite database for BI and long-term storage.
    """
    if df is None or df.empty:
        print("No data to load.")
        return False

    print(f"Loading {len(df)} rows into analytical store: {db_path}...")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Use SQLite for structured, queryable storage
    try:
        conn = sqlite3.connect(db_path)
        # Append data to the table
        df.to_sql(table_name, conn, if_exists='append', index=False)
        conn.close()
        print(f"Successfully loaded data into table '{table_name}'.")
        return True
    except Exception as e:
        print(f"Failed to load data to SQLite: {e}")
        return False

def archive_raw_logs(raw_dir="data/raw", archive_dir="data/archive"):
    """
    Moves processed logs to an archive directory to prevent re-processing.
    """
    os.makedirs(archive_dir, exist_ok=True)
    
    files = [f for f in os.listdir(raw_dir) if f.lower().endswith('.bin')]
    if not files:
        return
        
    print(f"Archiving {len(files)} logs...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_archive = os.path.join(archive_dir, timestamp)
    os.makedirs(run_archive, exist_ok=True)
    
    for f in files:
        shutil.move(os.path.join(raw_dir, f), os.path.join(run_archive, f))
    
    print(f"Logs moved to {run_archive}")

if __name__ == "__main__":
    # This script would normally be called by the pipeline with a dataframe
    # But for standalone testing, we can read the existing CSV if it exists
    csv_path = "data/processed/telemetry_dataset.csv"
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        if load_to_analytical_store(df):
            archive_raw_logs()
