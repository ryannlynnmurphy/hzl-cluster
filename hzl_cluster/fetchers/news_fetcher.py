"""
News fetcher — pulls RSS feeds and saves articles as JSON.
No API key required. Uses Python's built-in xml.etree.
"""
import json
import logging
import os
from datetime import datetime
from typing import List, Optional
from urllib.request import urlopen, Request
from urllib.error import URLError
from xml.etree import ElementTree as ET

logger = logging.getLogger("hzl.fetcher.news")

DEFAULT_FEEDS = {
    "hackernews": "https://hnrss.org/frontpage",
    "ars": "https://feeds.arstechnica.com/arstechnica/index",
    "lobsters": "https://lobste.rs/rss",
}


def fetch_news(
    staging_dir: str,
    feeds: Optional[dict] = None,
    max_articles_per_feed: int = 10,
    simulate: bool = False,
) -> dict:
    """
    Fetch RSS feeds and save articles to staging directory.

    Args:
        staging_dir: where to save the output JSON
        feeds: dict of {name: url}, defaults to DEFAULT_FEEDS
        max_articles_per_feed: limit per feed
        simulate: if True, return fake data without network

    Returns: {"success": bool, "file": str or None, "articles_count": int, "feeds_fetched": int}
    """
    os.makedirs(staging_dir, exist_ok=True)
    feeds = feeds or DEFAULT_FEEDS

    if simulate:
        articles = [
            {
                "feed": "hackernews",
                "title": "Show HN: Hazel OS - AI-native, security-first operating system",
                "link": "https://news.ycombinator.com/item?id=12345",
                "published": "2026-04-06T10:00:00",
                "summary": "A 22-year-old founder built an air-gapped OS with voice-first AI.",
            },
            {
                "feed": "ars",
                "title": "Local-first AI is the future of personal computing",
                "link": "https://arstechnica.com/ai/local-first",
                "published": "2026-04-06T08:00:00",
                "summary": "New trend in AI development prioritizes on-device processing.",
            },
            {
                "feed": "lobsters",
                "title": "Building a Pi cluster with physical air-gap security",
                "link": "https://lobste.rs/s/abc123",
                "published": "2026-04-06T12:00:00",
                "summary": "USB relay modules as hardware security boundaries.",
            },
        ]
        outpath = os.path.join(staging_dir, "news.json")
        data = {"fetched_at": datetime.now().isoformat(), "articles": articles}
        with open(outpath, "w") as f:
            json.dump(data, f, indent=2)
        return {"success": True, "file": outpath, "articles_count": 3, "feeds_fetched": 3}

    # Real fetch
    all_articles = []
    feeds_fetched = 0

    for feed_name, feed_url in feeds.items():
        try:
            req = Request(feed_url, headers={"User-Agent": "HazelOS/1.0"})
            with urlopen(req, timeout=15) as resp:
                xml_data = resp.read()

            root = ET.fromstring(xml_data)

            # Handle both RSS and Atom feeds
            items = root.findall(".//item")  # RSS
            if not items:
                # Atom namespace
                ns = {"atom": "http://www.w3.org/2005/Atom"}
                items = root.findall(".//atom:entry", ns)

            count = 0
            for item in items[:max_articles_per_feed]:
                # RSS format
                title = _text(item, "title") or _text(item, "atom:title", ns={"atom": "http://www.w3.org/2005/Atom"})
                link = _text(item, "link") or _attr(item, "atom:link", "href", ns={"atom": "http://www.w3.org/2005/Atom"})
                pub = _text(item, "pubDate") or _text(item, "atom:published", ns={"atom": "http://www.w3.org/2005/Atom"})
                desc = _text(item, "description") or _text(item, "atom:summary", ns={"atom": "http://www.w3.org/2005/Atom"})

                if title:
                    all_articles.append({
                        "feed": feed_name,
                        "title": title.strip(),
                        "link": (link or "").strip(),
                        "published": (pub or "").strip(),
                        "summary": _clean_html(desc or "")[:500],
                    })
                    count += 1

            feeds_fetched += 1
            logger.info(f"Fetched {count} articles from {feed_name}")

        except (URLError, ET.ParseError, OSError) as e:
            logger.error(f"Failed to fetch {feed_name}: {e}")

    outpath = os.path.join(staging_dir, "news.json")
    data = {"fetched_at": datetime.now().isoformat(), "articles": all_articles}
    with open(outpath, "w") as f:
        json.dump(data, f, indent=2)

    return {
        "success": feeds_fetched > 0,
        "file": outpath,
        "articles_count": len(all_articles),
        "feeds_fetched": feeds_fetched,
    }


def _text(element, tag, ns=None):
    """Get text content of a child element."""
    child = element.find(tag, ns) if ns else element.find(tag)
    return child.text if child is not None and child.text else None


def _attr(element, tag, attr, ns=None):
    """Get attribute of a child element."""
    child = element.find(tag, ns) if ns else element.find(tag)
    return child.get(attr) if child is not None else None


def _clean_html(text: str) -> str:
    """Strip HTML tags from text."""
    import re
    clean = re.sub(r'<[^>]+>', '', text)
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean
