"""
URL fetcher — downloads arbitrary URLs to staging/downloads/ for offline use
on the cluster. Validates content-type to block executables and scripts,
enforces a size limit, sanitizes filenames, and saves a metadata.json
alongside each download.
"""
import json
import logging
import os
import re
import urllib.request
import urllib.error
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

logger = logging.getLogger("hzl.fetcher.url")

# Blocked MIME type prefixes / exact types
BLOCKED_CONTENT_TYPES = {
    "application/x-msdownload",
    "application/x-executable",
    "application/x-sh",
    "application/x-csh",
    "application/x-bat",
    "application/x-msdos-program",
    "application/x-dosexec",
    "application/octet-stream",  # generic binary — too risky
    "text/x-shellscript",
    "text/x-python",
    "text/x-perl",
    "text/x-ruby",
}

BLOCKED_EXTENSIONS = {
    ".exe", ".bat", ".cmd", ".sh", ".ps1", ".py",
    ".rb", ".pl", ".com", ".msi", ".dll", ".so",
}

DEFAULT_MAX_BYTES = 50 * 1024 * 1024  # 50 MB


def _sanitize_filename(raw: str) -> str:
    """
    Convert a raw filename (or URL path) into a safe filename:
    - Strip path separators and directory traversal sequences
    - Keep only alphanumeric characters, dots, dashes, and underscores
    - Collapse repeated dots (prevents hidden-file tricks)
    - Fall back to 'download' if nothing survives the filter
    """
    # Take only the last component — removes any directory prefix
    name = os.path.basename(raw)
    # Strip query strings if the name came from a URL
    name = name.split("?")[0].split("#")[0]
    # Allow letters, digits, dots, dashes, underscores
    name = re.sub(r"[^\w.\-]", "_", name)
    # Collapse runs of dots
    name = re.sub(r"\.{2,}", ".", name)
    # Strip leading dots / dashes
    name = name.lstrip(".-")
    return name or "download"


def fetch_url(
    url: str,
    staging_dir: str,
    filename: Optional[str] = None,
    max_bytes: int = DEFAULT_MAX_BYTES,
    simulate: bool = False,
) -> dict:
    """
    Download a URL to staging/downloads/{sanitized_filename}.

    In simulate mode, no network request is made; a realistic metadata.json
    is written and the result dict is returned as if the download succeeded.

    Returns:
        {
            "success": bool,
            "path": str or None,
            "metadata": str or None,
            "url": str,
            "content_type": str or None,
            "size_bytes": int,
            "summary": str,
        }
    """
    downloads_dir = os.path.join(staging_dir, "downloads")
    os.makedirs(downloads_dir, exist_ok=True)

    # Derive a safe filename from the supplied name or the URL path
    raw_name = filename or urlparse(url).path.split("/")[-1] or "download"
    safe_name = _sanitize_filename(raw_name)

    dest_path = os.path.join(downloads_dir, safe_name)
    metadata_path = os.path.join(downloads_dir, "metadata.json")

    if simulate:
        # Produce a realistic result without touching the network
        simulated_type = "text/html"
        simulated_size = 8192

        metadata = {
            "url": url,
            "filename": safe_name,
            "content_type": simulated_type,
            "size_bytes": simulated_size,
            "fetch_time": datetime.now().isoformat(),
            "simulate": True,
            "path": dest_path,
        }
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        summary = f"Simulated download of {url} -> {safe_name}"
        logger.info(f"URL fetch simulated: {summary}")
        return {
            "success": True,
            "path": dest_path,
            "metadata": metadata_path,
            "url": url,
            "content_type": simulated_type,
            "size_bytes": simulated_size,
            "summary": summary,
        }

    # --- Real download ---
    def _fail(reason: str) -> dict:
        logger.error(f"URL fetch failed: {reason}")
        return {
            "success": False,
            "path": None,
            "metadata": None,
            "url": url,
            "content_type": None,
            "size_bytes": 0,
            "summary": reason,
        }

    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "HZL-Cluster-Fetcher/1.0"},
        )
        with urllib.request.urlopen(req, timeout=60) as response:
            content_type = response.headers.get("Content-Type", "").split(";")[0].strip().lower()

            # Content-type safety check
            if content_type in BLOCKED_CONTENT_TYPES:
                return _fail(f"Blocked content-type: {content_type}")

            # Extension safety check on the safe_name
            ext = os.path.splitext(safe_name)[1].lower()
            if ext in BLOCKED_EXTENSIONS:
                return _fail(f"Blocked file extension: {ext}")

            # Stream download with size enforcement
            data = b""
            while True:
                chunk = response.read(65536)
                if not chunk:
                    break
                data += chunk
                if len(data) > max_bytes:
                    return _fail(
                        f"Response exceeded size limit of {max_bytes} bytes"
                    )

        size = len(data)
        with open(dest_path, "wb") as f:
            f.write(data)

        metadata = {
            "url": url,
            "filename": safe_name,
            "content_type": content_type,
            "size_bytes": size,
            "fetch_time": datetime.now().isoformat(),
            "simulate": False,
            "path": dest_path,
        }
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        summary = f"Downloaded {url} -> {safe_name} ({size} bytes, {content_type})"
        logger.info(f"URL fetch complete: {summary}")
        return {
            "success": True,
            "path": dest_path,
            "metadata": metadata_path,
            "url": url,
            "content_type": content_type,
            "size_bytes": size,
            "summary": summary,
        }

    except urllib.error.HTTPError as e:
        return _fail(f"HTTP {e.code}: {e.reason}")
    except urllib.error.URLError as e:
        return _fail(f"URL error: {e.reason}")
    except OSError as e:
        return _fail(f"IO error: {e}")
