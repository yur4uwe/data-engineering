import os
import sys
from scraper.scrape import scrape_logs


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    num_downloaded = scrape_logs(
        staging_dir=os.path.join(base_dir, "data/raw"), max_downloads=20
    )
    if num_downloaded == 0:
        print("No new data found. Exiting.")
        sys.exit(1)
    sys.exit(0)
