import os
import requests
import re
import urllib.parse
import zipfile
import io

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

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

def get_onedrive_direct_link(url):
    """
    Converts OneDrive share links to direct download links.
    """
    if "1drv.ms" in url:
        return url.replace("/u/", "/download/")
    if "onedrive.live.com" in url:
        if "resid=" in url and "download=1" not in url:
            return url + ("&" if "?" in url else "?") + "download=1"
    return url

def get_dropbox_direct_link(url):
    """
    Converts Dropbox share links to direct download links.
    """
    if "dl=0" in url:
        return url.replace("dl=0", "dl=1")
    elif "dl=1" not in url:
        return url + ("?" if "?" not in url else "&") + "dl=1"
    return url

def get_direct_link(url):
    """
    Attempts to convert cloud storage links to direct download links.
    """
    domain = urllib.parse.urlparse(url).netloc.lower()

    match domain:
        case "dropbox.com" | "www.dropbox.com":
            return get_dropbox_direct_link(url)
        case "drive.google.com" | "www.drive.google.com":
            return get_google_drive_direct_link(url)
        case "1drv.ms" | "www.1drv.ms" | "onedrive.live.com" | "www.onedrive.live.com":
            return get_onedrive_direct_link(url)
        case _:
            return url

def is_valid_bin(filepath):
    """
    Checks if the file has the ArduPilot DataFlash magic header.
    Format 1: 0xA3 0x95 (standard DataFlash)
    Format 2: 0x80 0x80 (modern alternate)
    """
    try:
        with open(filepath, "rb") as f:
            header = f.read(2)
            return header == b"\xa3\x95" or header == b"\x80\x80"
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
