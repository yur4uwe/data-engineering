from collections import deque
import requests
from .utils import headers


def process_page(page_url, thread_queue, visited_threads):
    response = requests.get(page_url, headers=headers)
    response.raise_for_status()
    search_data = response.json()

    topics = search_data.get("topics", [])
    if not topics:
        return

    for topic in topics:
        slug = topic.get("slug")
        topic_id = topic.get("id")
        title = topic.get("title", "Unknown Title")

        if not slug or not topic_id:
            continue

        url = f"https://discuss.ardupilot.org/t/{slug}/{topic_id}"
        if url in visited_threads:
            continue

        thread_queue.append((url, 0, title))
        visited_threads.add(url)
        print(f"  Found topic: {title}")


def discussion_finder():
    """
    Finds ArduPilot Discuss forum threads.
    """

    thread_queue = deque()
    visited_threads = set()

    # Scan multiple pages of search results
    MAX_SEARCH_PAGES = 10
    print(f"Searching topics across {MAX_SEARCH_PAGES} pages...")

    for page in range(MAX_SEARCH_PAGES):
        try:
            process_page(
                f"https://discuss.ardupilot.org/search.json?q=.bin%20.zip&page={page}",
                thread_queue,
                visited_threads,
            )
        except Exception as e:
            print(f"Error on search page {page}: {e}")
            break

    return list(thread_queue)
