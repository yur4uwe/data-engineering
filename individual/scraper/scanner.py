import time
import requests
import os
from .utils import headers, get_direct_link, handle_zip, is_valid_bin
from .filter import get_all_links, filter_links

MAX_DEPTH = 1


def download_log(url, staging_dir, processed_urls):
    """
    Downloads and verifies a single log file.
    Returns 1 if successful, 0 otherwise.
    """
    if url in processed_urls:
        return 0

    processed_urls.add(url)
    target_url = get_direct_link(url)

    try:
        with requests.Session() as s:
            file_response = s.get(target_url, headers=headers, stream=True, timeout=20)
            file_response.raise_for_status()
            ctype = file_response.headers.get("Content-Type", "").lower()

            # Zip files
            if url.lower().endswith(".zip") or ".zip?" in url.lower() or "zip" in ctype:
                return handle_zip(file_response.content, staging_dir)

            # Binary files
            if "text/html" in ctype and not url.lower().endswith(".bin"):
                return 0

            filename = url.split("/")[-1].split("?")[0]
            if not filename.lower().endswith(".bin"):
                filename += ".bin"

            filepath = os.path.join(staging_dir, filename)
            with open(filepath, "wb") as f:
                for chunk in file_response.iter_content(chunk_size=8192):
                    f.write(chunk)

            if is_valid_bin(filepath):
                print(f"  Saved and verified: {filename}")
                return 1
            else:
                os.remove(filepath)
                return 0
    except Exception as e:
        print(f"  Download failed from {url}: {e}")
        return 0


def scan_discussion(
    url, depth, staging_dir, processed_urls, max_downloads, current_downloaded
):
    """
    Scans a discussion thread using refactored modular logic.
    """
    newly_downloaded = 0
    discovered_threads = []

    t_json_url = f"{url}.json"
    try:
        t_response = requests.get(t_json_url, headers=headers, timeout=15)
        t_response.raise_for_status()
        t_data = t_response.json()
    except Exception as e:
        print(f"Error fetching thread {url}: {e}")
        return 0, []

    posts = t_data.get("post_stream", {}).get("posts", [])
    for post in posts:
        if current_downloaded + newly_downloaded >= max_downloads:
            break

        # 1. Extract Links (URL Scanner)
        all_links = get_all_links(post.get("cooked", ""))

        # 2. Filter Links (Categorization)
        categorized = filter_links(all_links)

        # Log counts for transparency
        if categorized["threads"] or categorized["logs"]:
            print(
                f"    Found: {len(categorized['threads'])} threads, {len(categorized['logs'])} logs (ignored {len(categorized['ignored'])} links)"
            )

        # 3. Handle Nested Threads
        if depth < MAX_DEPTH:
            for thread_url in categorized["threads"]:
                discovered_threads.append((thread_url, depth + 1, "Linked Thread"))

        # 4. Process Logs
        for log_url in categorized["logs"]:
            if current_downloaded + newly_downloaded >= max_downloads:
                break
            newly_downloaded += download_log(log_url, staging_dir, processed_urls)

    time.sleep(1)
    return newly_downloaded, discovered_threads
