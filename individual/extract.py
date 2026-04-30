from datetime import datetime
import os
import sys
from scraper.scrape import scrape_logs


def are_files_relevant(staging_dir: str):
    if not os.path.exists(staging_dir) or not os.listdir(staging_dir):
        return False
        
    most_recent = datetime(1970, 1, 1)
    paths = os.listdir(staging_dir)
    for file in paths:
        full_path = os.path.join(staging_dir, file)
        if not os.path.isfile(full_path):
            continue
        mtime = datetime.fromtimestamp(os.path.getmtime(full_path))
        if most_recent < mtime:
            most_recent = mtime

    now = datetime.now()
    delta = now - most_recent
    if delta.days < 5:
        return True
    else:
        return False


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.join(base_dir, "data/raw")

    if are_files_relevant(base_dir):
        print("All files are currently relevant")
        sys.exit(0)

    num_downloaded = scrape_logs(staging_dir=base_dir, max_downloads=5)
    if num_downloaded == 0:
        print("No new data found. Exiting.")
        sys.exit(1)
    sys.exit(0)
