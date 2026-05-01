import glob
import os
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
from load import is_file_processed, load_to_analytical_store, mark_as_processed
from pymavlink import mavutil


def parse_bin_file(filepath):
    print(f"Parsing {filepath}...")
    try:
        mlog = mavutil.mavlink_connection(filepath)
    except Exception as e:
        print(f"Failed to open {filepath}: {e}")
        return pd.DataFrame()

    data = {"ATT": [], "BAT": [], "GPS": []}

    while True:
        try:
            m = mlog.recv_match(
                type=["ATT", "BAT", "GPS", "GPS_RAW_INT", "BATTERY_STATUS", "ATTITUDE"]
            )
            if m is None:
                break

            m_type = m.get_type()
            m_dict = m.to_dict()  # pyright: ignore[reportOptionalCall, reportCallIssue]

            if m_type == "ATT" or m_type == "ATTITUDE":
                data["ATT"].append(m_dict)
            elif m_type == "BAT" or m_type == "BATTERY_STATUS":
                data["BAT"].append(m_dict)
            elif m_type == "GPS" or m_type == "GPS_RAW_INT":
                data["GPS"].append(m_dict)
        except Exception:
            continue

    df_att = pd.DataFrame(data["ATT"])
    df_bat = pd.DataFrame(data["BAT"])
    df_gps = pd.DataFrame(data["GPS"])

    def set_time_index(df):
        if df.empty:
            return df
        if "TimeUS" in df.columns:
            df.set_index("TimeUS", inplace=True)
        elif "time_boot_ms" in df.columns:
            df.set_index("time_boot_ms", inplace=True)
        df.index.name = "timestamp_us"
        return df

    df_att = set_time_index(df_att)
    df_bat = set_time_index(df_bat)
    df_gps = set_time_index(df_gps)

    dfs = []
    if not df_att.empty:
        dfs.append(df_att)
    if not df_bat.empty:
        dfs.append(df_bat.add_prefix("BAT_"))
    if not df_gps.empty:
        dfs.append(df_gps.add_prefix("GPS_"))

    if not dfs:
        return pd.DataFrame()

    combined_df = dfs[0]
    for i in range(1, len(dfs)):
        combined_df = combined_df.sort_index()
        dfs[i] = dfs[i].sort_index()
        combined_df = pd.merge_asof(
            combined_df, dfs[i], left_index=True, right_index=True, direction="nearest"
        )

    combined_df = combined_df.ffill().bfill()
    combined_df.reset_index(inplace=True)
    return combined_df


def process_single_file(args):
    filepath, flight_id = args
    try:
        df = parse_bin_file(filepath)
        if df.empty:
            return None

        df["flight_id"] = flight_id
        return df
    except Exception as e:
        print(f"Error in process_single_file for {filepath}: {e}")
    return None


def etl_pipeline(
    input_dir="data/raw", output_dir="data/processed", db_path="data/uav_analytics.db"
):
    os.makedirs(output_dir, exist_ok=True)

    all_bin_files = glob.glob(os.path.join(input_dir, "*.[bB][iI][nN]"))
    bin_files = [
        f for f in all_bin_files if not is_file_processed(os.path.basename(f), db_path)
    ]

    if not bin_files:
        if all_bin_files:
            print(
                f"All {len(all_bin_files)} files in {input_dir} have already been processed."
            )
        else:
            print(f"No binary logs found in {input_dir}.")
        return None, []

    print(
        f"Found {len(bin_files)} new files to process. Starting parallel processing..."
    )

    all_flights = []
    file_args = [(f, i) for i, f in enumerate(bin_files)]

    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_single_file, arg) for arg in file_args]
        for future in as_completed(futures):
            result = future.result()
            if result is None:
                continue

            all_flights.append(result)

    processed_filenames = [os.path.basename(f) for f in bin_files]

    if all_flights:
        print(f"Concatenating {len(all_flights)} parsed flights...")
        final_dataset = pd.concat(all_flights, ignore_index=True)
        return final_dataset, processed_filenames
    else:
        print("No data extracted from new files.")
        return None, []


if __name__ == "__main__":
    import sys

    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(base_dir, "data/uav_analytics.db")
    df, processed_files = etl_pipeline(
        input_dir=os.path.join(base_dir, "data/raw"),
        output_dir=os.path.join(base_dir, "data/processed"),
        db_path=db_path,
    )

    if df is None:
        sys.exit(0)  # Not an error, just no new data

    # Load the data
    if load_to_analytical_store(df, db_path=db_path):
        # Mark as processed only if load succeeded
        mark_as_processed(processed_files, db_path=db_path)
    sys.exit(0)
