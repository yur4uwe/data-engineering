import os
import pandas as pd
import sqlite3
import shutil
from datetime import datetime


def load_to_analytical_store(
    df, db_path="data/uav_analytics.db", table_name="telemetry"
):
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

        # Check if table exists and handle schema evolution (new columns)
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        )
        if cursor.fetchone():
            # Get existing columns
            cursor.execute(f"PRAGMA table_info({table_name})")
            existing_cols = [info[1] for info in cursor.fetchall()]

            # Find new columns in DF
            for col in df.columns:
                if col in existing_cols:
                    continue

                print(f"Adding new column '{col}' to table '{table_name}'...")
                # Basic type mapping: float -> REAL, int -> INTEGER, others -> TEXT
                col_type = "TEXT"
                if pd.api.types.is_float_dtype(df[col]):
                    col_type = "REAL"
                elif pd.api.types.is_integer_dtype(df[col]):
                    col_type = "INTEGER"

                cursor.execute(
                    f'ALTER TABLE {table_name} ADD COLUMN "{col}" {col_type}'
                )

        # Append data to the table
        df.to_sql(table_name, conn, if_exists="append", index=False)
        conn.close()
        print(f"Successfully loaded data into table '{table_name}'.")
        return True
    except Exception as e:
        print(f"Failed to load data to SQLite: {e}")
        return False


def is_file_processed(filename, db_path="data/uav_analytics.db"):
    """Checks if a file has already been processed and loaded."""
    if not os.path.exists(db_path):
        return False
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS processed_files (filename TEXT PRIMARY KEY, processed_at TIMESTAMP)"
        )
        cursor.execute("SELECT 1 FROM processed_files WHERE filename = ?", (filename,))
        result = cursor.fetchone()
        conn.close()
        return result is not None
    except Exception as e:
        print(f"Error checking checkpoint: {e}")
        return False


def mark_as_processed(filenames, db_path="data/uav_analytics.db"):
    """Records filenames in the checkpoint table."""
    if not filenames:
        return

    print(f"Marking {len(filenames)} files as processed in metadata...")
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS processed_files (filename TEXT PRIMARY KEY, processed_at TIMESTAMP)"
        )

        now = datetime.now().isoformat()
        data = [(f, now) for f in filenames]
        cursor.executemany("INSERT OR REPLACE INTO processed_files VALUES (?, ?)", data)

        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Failed to update checkpoints: {e}")


def archive_raw_logs(raw_dir="data/raw", archive_dir="data/archive"):
    """
    Optional: Copies processed logs to archive instead of moving them,
    keeping the raw directory populated.
    """
    os.makedirs(archive_dir, exist_ok=True)
    files = [f for f in os.listdir(raw_dir) if f.lower().endswith(".bin")]
    if not files:
        return

    print(f"Backing up {len(files)} logs to archive...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_archive = os.path.join(archive_dir, timestamp)
    os.makedirs(run_archive, exist_ok=True)

    for f in files:
        shutil.copy2(os.path.join(raw_dir, f), os.path.join(run_archive, f))


if __name__ == "__main__":
    # This script would normally be called by the pipeline with a dataframe
    # But for standalone testing, we can read the existing CSV if it exists
    csv_path = "data/processed/telemetry_dataset.csv"
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        if load_to_analytical_store(df):
            archive_raw_logs()
