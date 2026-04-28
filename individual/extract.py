import os
import requests
from bs4 import BeautifulSoup
import urllib.parse
import re
import time
import zipfile
import io
from collections import deque


def get_google_drive_direct_link(url):
    """
    Handles Google Drive 'large file' confirmation tokens.
    """
    gd_match = re.search(r"drive\.google\.com/file/d/([a-zA-Z0-9_-]+)", url)
    if not gd_match:
        gd_match = re.search(r"id=([a-zA-Z0-9_-]+)", url)

    if gd_match:
        file_id = gd_match.group(1)
        session = requests.Session()
        download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
        try:
            response = session.get(download_url, stream=True, timeout=10)
            if "confirm=" in response.text:
                token = re.search(r"confirm=([a-zA-Z0-9_-]+)", response.text)
                if token:
                    return f"https://drive.google.com/uc?export=download&confirm={token.group(1)}&id={file_id}"
        except:
            pass
        return download_url
    return url


def get_direct_link(url):
    """
    Attempts to convert cloud storage links to direct download links.
    """
    if "dropbox.com" in url:
        if "dl=0" in url:
            return url.replace("dl=0", "dl=1")
        elif "dl=1" not in url:
            return url + ("?" if "?" not in url else "&") + "dl=1"

    if "drive.google.com" in url:
        return get_google_drive_direct_link(url)

    return url


def is_valid_bin(filepath):
    """
    Checks if the file has the ArduPilot DataFlash magic header.
    Format 1: 0x89 0x44 (FMT)
    """
    try:
        with open(filepath, "rb") as f:
            header = f.read(2)
            return header == b"\x89\x44" or header == b"\x89\x00"
    except:
        return False


def handle_zip(zip_content, staging_dir):
    """
    Extracts .bin files from a zip archive.
    """
    extracted_count = 0
    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
            for file_info in z.infolist():
                if file_info.filename.lower().endswith(".bin"):
                    filename = os.path.basename(file_info.filename)
                    if not filename:
                        continue

                    filepath = os.path.join(staging_dir, filename)
                    with z.open(file_info) as source, open(filepath, "wb") as target:
                        target.write(source.read())

                    if is_valid_bin(filepath):
                        print(f"    Extracted and verified from zip: {filename}")
                        extracted_count += 1
                    else:
                        os.remove(filepath)
    except Exception as e:
        print(f"    Failed to process zip: {e}")
    return extracted_count


def scrape_logs(staging_dir="data/raw", max_downloads=5):
    """
    Deep scrapes ArduPilot Discuss forum with recursive thread following.
    """
    os.makedirs(staging_dir, exist_ok=True)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    # Initial search
    search_api_url = "https://discuss.ardupilot.org/search.json?q=.bin%20.zip"
    print(f"Searching topics on {search_api_url}...")

    try:
        response = requests.get(search_api_url, headers=headers)
        response.raise_for_status()
        search_data = response.json()
    except Exception as e:
        print(f"Failed to fetch search results: {e}")
        return 0

    topics = search_data.get("topics", [])
    if not topics:
        print("No topics found.")
        return 0

    # Queue of threads to scan (url, depth, title)
    thread_queue = deque()
    visited_threads = set()

    for topic in topics:
        slug = topic.get("slug")
        topic_id = topic.get("id")
        title = topic.get("title", "Unknown Title")
        if slug and topic_id:
            url = f"https://discuss.ardupilot.org/t/{slug}/{topic_id}"
            thread_queue.append((url, 0, title))
            visited_threads.add(url)
            print(f"  Found topic: {title} ({url})")

    downloaded = 0
    processed_urls = set()
    MAX_DEPTH = 1 

    while thread_queue and downloaded < max_downloads:
        t_url, depth, t_title = thread_queue.popleft()

        print(f"\n[Scanning Thread] depth={depth} | {t_title}")
        print(f"URL: {t_url}")
        try:
            t_json_url = f"{t_url}.json"
            t_response = requests.get(t_json_url, headers=headers)
            t_response.raise_for_status()
            t_data = t_response.json()

            # Update title from JSON if it was missing
            current_title = t_data.get('title', t_title)


            posts = t_data.get("post_stream", {}).get("posts", [])
            for post in posts:
                content = post.get("cooked", "")
                soup = BeautifulSoup(content, "html.parser")

                for link in soup.find_all("a", href=True):
                    href = link["href"]
                    if href.startswith("/"):
                        href = urllib.parse.urljoin(
                            "https://discuss.ardupilot.org", href
                        )

                    # 1. Check if it's an internal thread link to follow
                    if depth < MAX_DEPTH and "discuss.ardupilot.org/t/" in href:
                        # Normalize URL to topic root
                        topic_match = re.search(
                            r"(https://discuss\.ardupilot\.org/t/[^/]+/\d+)", href
                        )
                        if topic_match:
                            base_topic_url = topic_match.group(1)
                            if base_topic_url not in visited_threads:
                                visited_threads.add(base_topic_url)
                                thread_queue.append((base_topic_url, depth + 1))
                                print(
                                    f"  Added nested thread to queue: {base_topic_url}"
                                )

                    # 2. Check if it's a log or zip file
                    is_bin = href.lower().endswith(".bin") or ".bin?" in href.lower()
                    is_zip = href.lower().endswith(".zip") or ".zip?" in href.lower()
                    is_cloud = any(
                        domain in href for domain in ["dropbox.com", "drive.google.com"]
                    )

                    if (is_bin or is_zip or is_cloud) and href not in processed_urls:
                        processed_urls.add(href)
                        target_url = get_direct_link(href)
                        print(f"  Attempting download from: {target_url}")

                        try:
                            with requests.Session() as s:
                                file_response = s.get(
                                    target_url, headers=headers, stream=True, timeout=20
                                )
                                file_response.raise_for_status()
                                ctype = file_response.headers.get(
                                    "Content-Type", ""
                                ).lower()

                                if is_zip or "zip" in ctype:
                                    print(f"  Processing zip archive...")
                                    zip_content = file_response.content
                                    extracted = handle_zip(zip_content, staging_dir)
                                    downloaded += extracted
                                    if downloaded >= max_downloads:
                                        break
                                else:
                                    if "text/html" in ctype and not is_bin:
                                        continue

                                    filename = href.split("/")[-1].split("?")[0]
                                    if not filename.lower().endswith(".bin"):
                                        filename += ".bin"

                                    filepath = os.path.join(staging_dir, filename)
                                    with open(filepath, "wb") as f:
                                        for chunk in file_response.iter_content(
                                            chunk_size=8192
                                        ):
                                            f.write(chunk)

                                    if is_valid_bin(filepath):
                                        print(f"  Saved and verified: {filepath}")
                                        downloaded += 1
                                        if downloaded >= max_downloads:
                                            break
                                    else:
                                        os.remove(filepath)
                                        print(f"  Discarded: Invalid log signature.")
                        except Exception as e:
                            print(f"  Download failed: {e}")

                if downloaded >= max_downloads:
                    break
            time.sleep(1)
        except Exception as e:
            print(f"Error scanning thread {t_url}: {e}")

    print(f"Extraction finished. Total valid logs acquired: {downloaded}")
    return downloaded


if __name__ == "__main__":
    import sys

    base_dir = os.path.dirname(os.path.abspath(__file__))
    num_downloaded = scrape_logs(staging_dir=os.path.join(base_dir, "data/raw"), max_downloads=20)

    if num_downloaded == 0:
        print("No new data found. Exiting.")
        sys.exit(1)
    sys.exit(0)
