"""
Podcast fetcher — parses RSS feeds and downloads audio episodes to staging.
Standard RSS with enclosure tags. No API key required.
"""
import json
import logging
import os
from datetime import datetime
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import URLError
from xml.etree import ElementTree as ET

logger = logging.getLogger("hzl.fetcher.podcast")

DEFAULT_FEEDS = {
    "99pi": "https://feeds.99percentinvisible.org/99percentinvisible",
    "radiolab": "https://feeds.feedburner.com/radiolab",
    "darknet_diaries": "https://feeds.megaphone.fm/darknetdiaries",
}

# Supported audio MIME types and extensions
AUDIO_TYPES = {"audio/mpeg", "audio/mp4", "audio/x-m4a", "audio/ogg", "audio/opus"}
AUDIO_EXTS = {".mp3", ".m4a", ".ogg", ".opus", ".aac"}


def fetch_podcasts(
    staging_dir: str,
    feeds: Optional[dict] = None,
    max_episodes: int = 3,
    max_total_mb: float = 200.0,
    simulate: bool = False,
) -> dict:
    """
    Parse podcast RSS feeds and download audio episodes to staging directory.

    Args:
        staging_dir: root staging directory; audio saved to staging_dir/podcasts/
        feeds: dict of {show_name: rss_url}, defaults to DEFAULT_FEEDS
        max_episodes: maximum number of episodes to download across all feeds
        max_total_mb: hard cap on total downloaded audio in megabytes
        simulate: if True, write fake data without network or disk-heavy downloads

    Returns: {
        "success": bool,
        "index_file": str or None,
        "episodes_downloaded": int,
        "total_mb": float,
        "feeds_fetched": int,
    }
    """
    podcasts_dir = os.path.join(staging_dir, "podcasts")
    os.makedirs(podcasts_dir, exist_ok=True)
    feeds = feeds or DEFAULT_FEEDS

    if simulate:
        episodes = [
            {
                "title": "The Smell of Concrete After Rain",
                "show": "99pi",
                "duration": "00:38:12",
                "description": "The word petrichor describes the earthy scent produced when rain falls on dry soil.",
                "file": os.path.join(podcasts_dir, "99pi_petrichor.mp3"),
                "url": "https://feeds.99percentinvisible.org/episodes/petrichor.mp3",
                "fetched_at": datetime.now().isoformat(),
            },
            {
                "title": "Darknet Diaries Ep 100: NSA Leaker",
                "show": "darknet_diaries",
                "duration": "01:12:44",
                "description": "Inside the story of the NSA leaker who changed everything.",
                "file": os.path.join(podcasts_dir, "dd_ep100.mp3"),
                "url": "https://feeds.megaphone.fm/episodes/dd100.mp3",
                "fetched_at": datetime.now().isoformat(),
            },
        ]
        # Write placeholder audio stubs so file paths exist
        for ep in episodes:
            with open(ep["file"], "wb") as f:
                f.write(b"SIMULATED_AUDIO")

        index_path = os.path.join(podcasts_dir, "index.json")
        index = {
            "fetched_at": datetime.now().isoformat(),
            "episodes": episodes,
        }
        with open(index_path, "w") as f:
            json.dump(index, f, indent=2)

        return {
            "success": True,
            "index_file": index_path,
            "episodes_downloaded": len(episodes),
            "total_mb": 0.0,
            "feeds_fetched": 2,
        }

    # Real fetch
    all_episodes = []
    feeds_fetched = 0
    total_bytes = 0
    max_bytes = max_total_mb * 1024 * 1024

    for show_name, feed_url in feeds.items():
        if len(all_episodes) >= max_episodes:
            break
        if total_bytes >= max_bytes:
            logger.warning("Total size cap reached — stopping downloads")
            break

        try:
            req = Request(feed_url, headers={"User-Agent": "HazelOS/1.0"})
            with urlopen(req, timeout=15) as resp:
                xml_data = resp.read()

            root = ET.fromstring(xml_data)
            items = root.findall(".//item")

            for item in items:
                if len(all_episodes) >= max_episodes:
                    break
                if total_bytes >= max_bytes:
                    break

                title = _text(item, "title") or "Untitled"
                description = _clean_html(_text(item, "description") or "")[:500]
                duration = _text(item, "itunes:duration") or _text(
                    item, "{http://www.itunes.com/dtds/podcast-1.0.dtd}duration"
                ) or ""

                # Find enclosure for audio URL
                enclosure = item.find("enclosure")
                if enclosure is None:
                    continue

                audio_url = enclosure.get("url", "")
                mime_type = enclosure.get("type", "")
                enc_length = int(enclosure.get("length", 0) or 0)

                if not audio_url:
                    continue

                # Validate it is an audio file
                ext = os.path.splitext(audio_url.split("?")[0])[-1].lower()
                if mime_type not in AUDIO_TYPES and ext not in AUDIO_EXTS:
                    logger.debug(f"Skipping non-audio enclosure: {mime_type} {ext}")
                    continue

                # Check size cap before downloading
                if enc_length and (total_bytes + enc_length) > max_bytes:
                    logger.warning(f"Skipping {title}: would exceed size cap")
                    continue

                # Build safe filename
                safe_title = "".join(
                    c if c.isalnum() or c in "-_" else "_"
                    for c in title[:60]
                ).strip("_")
                audio_ext = ext if ext in AUDIO_EXTS else ".mp3"
                filename = f"{show_name}_{safe_title}{audio_ext}"
                filepath = os.path.join(podcasts_dir, filename)

                try:
                    logger.info(f"Downloading: {title}")
                    audio_req = Request(audio_url, headers={"User-Agent": "HazelOS/1.0"})
                    with urlopen(audio_req, timeout=60) as audio_resp:
                        audio_data = audio_resp.read()

                    with open(filepath, "wb") as af:
                        af.write(audio_data)

                    downloaded_bytes = len(audio_data)
                    total_bytes += downloaded_bytes

                    all_episodes.append({
                        "title": title.strip(),
                        "show": show_name,
                        "duration": duration.strip(),
                        "description": description,
                        "file": filepath,
                        "url": audio_url,
                        "fetched_at": datetime.now().isoformat(),
                    })
                    logger.info(f"Saved {filename} ({downloaded_bytes / 1024 / 1024:.1f} MB)")

                except (URLError, OSError) as e:
                    logger.error(f"Failed to download {title}: {e}")

            feeds_fetched += 1

        except (URLError, ET.ParseError, OSError) as e:
            logger.error(f"Failed to fetch feed {show_name}: {e}")

    index_path = os.path.join(podcasts_dir, "index.json")
    index = {
        "fetched_at": datetime.now().isoformat(),
        "episodes": all_episodes,
    }
    with open(index_path, "w") as f:
        json.dump(index, f, indent=2)

    total_mb = round(total_bytes / 1024 / 1024, 2)
    logger.info(f"Podcast fetch complete: {len(all_episodes)} episodes, {total_mb} MB")
    return {
        "success": feeds_fetched > 0,
        "index_file": index_path,
        "episodes_downloaded": len(all_episodes),
        "total_mb": total_mb,
        "feeds_fetched": feeds_fetched,
    }


def _text(element, tag, ns=None):
    """Get text content of a child element."""
    child = element.find(tag, ns) if ns else element.find(tag)
    return child.text if child is not None and child.text else None


def _clean_html(text: str) -> str:
    """Strip HTML tags from text."""
    import re
    clean = re.sub(r'<[^>]+>', '', text)
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean
