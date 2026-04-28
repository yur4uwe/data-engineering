import re
import urllib.parse
from bs4 import BeautifulSoup


def get_all_links(content, base_url="https://discuss.ardupilot.org"):
    """
    Extracts all absolute links from HTML content.
    """
    soup = BeautifulSoup(content, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href_attr = a["href"]
        if isinstance(href_attr, list):
            href = "".join(href_attr)
        else:
            href = str(href_attr)

        if href.startswith("/"):
            href = urllib.parse.urljoin(base_url, href)
        links.append(href)
    return links


def filter_links(links):
    """
    Categorizes links into discussion threads, log files, or others.
    Returns a dictionary with 'threads', 'logs', and 'ignored' lists.
    """
    categories = {"threads": [], "logs": [], "ignored": []}

    for link in links:
        link_lower = link.lower()

        # 1. Check for discussion threads
        if "discuss.ardupilot.org/t/" in link_lower:
            topic_match = re.search(
                r"(https://discuss\.ardupilot\.org/t/[^/]+/\d+)", link
            )
            if topic_match:
                categories["threads"].append(topic_match.group(1))
                continue

        # 2. Check for potential logs
        is_bin = link_lower.endswith(".bin") or ".bin?" in link_lower
        is_zip = link_lower.endswith(".zip") or ".zip?" in link_lower
        is_tlog = link_lower.endswith(".tlog") or ".tlog?" in link_lower
        is_cloud = any(
            domain in link_lower
            for domain in [
                "dropbox.com",
                "drive.google.com",
                "1drv.ms",
                "onedrive.live.com",
            ]
        )

        if (is_bin or is_zip or is_cloud) and not is_tlog:
            categories["logs"].append(link)
        else:
            categories["ignored"].append(link)

    return categories
