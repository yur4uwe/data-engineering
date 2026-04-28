import os
from collections import deque
from .finder import discussion_finder
from .scanner import scan_discussion


def scrape_logs(staging_dir="data/raw", max_downloads=20):
    """
    Orchestrates the scraping of ArduPilot telemetry logs.
    """
    os.makedirs(staging_dir, exist_ok=True)

    # 1. Find initial discussions
    initial_threads = discussion_finder()

    # Use a queue for BFS (Breadth-First Search) scanning
    queue = deque(initial_threads)
    visited_threads = {t[0] for t in initial_threads}

    downloaded = 0
    processed_urls = set()

    print(f"Starting scan of {len(queue)} threads...")

    # 2. Iterate through the queue until we hit max_downloads or run out of threads
    while queue and downloaded < max_downloads:
        t_url, depth, t_title = queue.popleft()

        print(f"\n[Scanning Thread] depth={depth} | {t_title}")
        print(f"URL: {t_url}")

        newly_downloaded, discovered_threads = scan_discussion(
            url=t_url,
            depth=depth,
            staging_dir=staging_dir,
            processed_urls=processed_urls,
            max_downloads=max_downloads,
            current_downloaded=downloaded,
        )

        downloaded += newly_downloaded

        # Add newly discovered threads to the queue if they haven't been visited
        for thread in discovered_threads:
            url = thread[0]
            if url not in visited_threads:
                visited_threads.add(url)
                queue.append(thread)
                print(f"  Added nested thread: {url}")

    print(f"\nExtraction finished. Total valid logs acquired: {downloaded}")
    return downloaded
