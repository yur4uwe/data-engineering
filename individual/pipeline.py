import argparse
import os

from analyze import perform_analysis
from extract import are_files_relevant
from load import archive_raw_logs, load_to_analytical_store, mark_as_processed
from scraper.scrape import scrape_logs
from transform import etl_pipeline


def main():
    parser = argparse.ArgumentParser(
        description="UAV Telemetry Analytics Pipeline Orchestrator"
    )
    parser.add_argument(
        "--step",
        choices=["extract", "transform_load", "analyze", "full"],
        default="full",
    )
    parser.add_argument("--force-extract", action="store_true")
    parser.add_argument("--max-downloads", type=int, default=5)
    parser.add_argument("--db-path", type=str, default="data/uav_analytics.db")
    parser.add_argument("--backup", action="store_true")

    args = parser.parse_args()
    base_dir = os.path.dirname(os.path.abspath(__file__))
    raw_dir = os.path.join(base_dir, "data/raw")
    processed_dir = os.path.join(base_dir, "data/processed")
    archive_dir = os.path.join(base_dir, "data/archive")
    plots_dir = os.path.join(base_dir, "plots")
    db_path = os.path.join(base_dir, args.db_path)

    # --- STEP 1: EXTRACT ---
    if args.step in ["extract", "full"]:
        print("\n[PHASE: EXTRACTION]")
        if args.force_extract or not are_files_relevant(raw_dir):
            scrape_logs(staging_dir=raw_dir, max_downloads=args.max_downloads)
        else:
            print("Logs are recent. Skipping extraction.")

    # --- STEP 2: TRANSFORM & LOAD ---
    if args.step in ["transform_load", "full"]:
        print("\n[PHASE: TRANSFORM & LOAD]")
        df, processed_files = etl_pipeline(
            input_dir=raw_dir, output_dir=processed_dir, db_path=db_path
        )

        if df is not None and not df.empty:
            csv_path = os.path.join(processed_dir, "telemetry_dataset.csv")
            df.to_csv(csv_path, index=False)
            if load_to_analytical_store(df, db_path=db_path):
                mark_as_processed(processed_files, db_path=db_path)
                if args.backup:
                    archive_raw_logs(raw_dir=raw_dir, archive_dir=archive_dir)
        else:
            print("No new data to process.")

    # --- STEP 3: ANALYZE ---
    if args.step in ["analyze", "full"]:
        print("\n[PHASE: ANALYSIS]")
        csv_path = os.path.join(processed_dir, "telemetry_dataset.csv")
        perform_analysis(input_file=csv_path, db_path=db_path, output_dir=plots_dir)


if __name__ == "__main__":
    main()
