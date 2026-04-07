"""
Calendar fetcher — pulls events from a CalDAV server.
Supports any CalDAV provider: Nextcloud, Radicale, iCloud, Google (via bridge).
Parses raw iCalendar text with no external dependencies.
Saves JSON to staging.
"""
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import URLError
from urllib.parse import urljoin
import base64

logger = logging.getLogger("hzl.fetcher.calendar")

_SIMULATE_EVENTS = [
    {
        "title": "Team Standup",
        "start": "2026-04-07T09:00:00",
        "end": "2026-04-07T09:30:00",
        "location": "Zoom",
        "description": "Daily sync with the cluster team.",
        "all_day": False,
    },
    {
        "title": "HZL Studio Planning",
        "start": "2026-04-08T13:00:00",
        "end": "2026-04-08T14:30:00",
        "location": "Studio A",
        "description": "Phase 3 roadmap review.",
        "all_day": False,
    },
    {
        "title": "Deployment Day",
        "start": "2026-04-09",
        "end": "2026-04-09",
        "location": "",
        "description": "Roll out gateway updates to all nodes.",
        "all_day": True,
    },
]


# ---------------------------------------------------------------------------
# iCalendar parser
# ---------------------------------------------------------------------------

def _unfold_ical(raw: str) -> str:
    """Unfold iCalendar line continuations (RFC 5545 §3.1)."""
    return re.sub(r"\r?\n[ \t]", "", raw)


def _parse_ical_value(line: str) -> tuple[str, str]:
    """
    Split a single unfolded iCal line into (name, value).
    Strips any parameter sections (e.g. DTSTART;TZID=America/New_York:...).
    Returns (property_name, value).
    """
    colon_pos = line.index(":")
    name_part = line[:colon_pos]
    value = line[colon_pos + 1:]
    # Strip parameters — keep only the base property name
    base_name = name_part.split(";")[0].upper()
    return base_name, value.strip()


def _ical_to_dt(value: str) -> Optional[str]:
    """
    Convert an iCalendar DATE or DATE-TIME value to an ISO 8601 string.
    Returns a date string ("YYYY-MM-DD") for all-day events,
    or a datetime string ("YYYY-MM-DDTHH:MM:SS[±HH:MM]") for timed events.
    """
    value = value.strip()
    # Strip TZID parameter if present in the value itself (shouldn't be, but guard)
    if "TZID=" in value:
        value = value.split(":")[-1]

    # All-day: YYYYMMDD
    if re.fullmatch(r"\d{8}", value):
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"

    # Date-time UTC: YYYYMMDDTHHMMSSZ
    if re.fullmatch(r"\d{8}T\d{6}Z", value):
        dt = datetime(
            int(value[:4]), int(value[4:6]), int(value[6:8]),
            int(value[9:11]), int(value[11:13]), int(value[13:15]),
            tzinfo=timezone.utc,
        )
        return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    # Date-time local: YYYYMMDDTHHMMSS
    if re.fullmatch(r"\d{8}T\d{6}", value):
        return (
            f"{value[:4]}-{value[4:6]}-{value[6:8]}"
            f"T{value[9:11]}:{value[11:13]}:{value[13:15]}"
        )

    return value  # fallback: return as-is


def _is_all_day(raw_dtstart: str) -> bool:
    """True when DTSTART is a bare DATE (no time component)."""
    raw = raw_dtstart.strip()
    # Strip any embedded TZID prefix
    if ":" in raw:
        raw = raw.split(":")[-1]
    return bool(re.fullmatch(r"\d{8}", raw))


def _parse_ical_events(ical_text: str) -> list[dict]:
    """
    Parse a VCALENDAR blob and return a list of event dicts.
    """
    text = _unfold_ical(ical_text)
    events = []

    for vevent_match in re.finditer(r"BEGIN:VEVENT(.*?)END:VEVENT", text, re.DOTALL):
        block = vevent_match.group(1)
        props: dict[str, str] = {}
        raw_props: dict[str, str] = {}  # keyed by base name, raw value (pre-parsing)

        for line in block.splitlines():
            line = line.strip()
            if not line or ":" not in line:
                continue
            try:
                name, value = _parse_ical_value(line)
                props[name] = value
                # Keep the raw token before the first colon for date detection
                raw_props[name] = line[line.index(":") + 1:]
            except (ValueError, IndexError):
                continue

        raw_dtstart = raw_props.get("DTSTART", "")
        raw_dtend = raw_props.get("DTEND", raw_props.get("DTSTART", ""))

        all_day = _is_all_day(raw_dtstart)

        start_str = _ical_to_dt(raw_dtstart) if raw_dtstart else None
        end_str = _ical_to_dt(raw_dtend) if raw_dtend else None

        event = {
            "title": props.get("SUMMARY", ""),
            "start": start_str,
            "end": end_str,
            "location": props.get("LOCATION", ""),
            "description": props.get("DESCRIPTION", ""),
            "all_day": all_day,
        }
        events.append(event)

    return events


# ---------------------------------------------------------------------------
# CalDAV HTTP helpers
# ---------------------------------------------------------------------------

def _caldav_report(url: str, username: str, password: str, days: int) -> str:
    """
    Issue a CalDAV REPORT request for the given time range and return
    the raw response body as a string.
    """
    now = datetime.now(tz=timezone.utc)
    start = now.strftime("%Y%m%dT%H%M%SZ")
    end = (now + timedelta(days=days)).strftime("%Y%m%dT%H%M%SZ")

    body = (
        '<?xml version="1.0" encoding="utf-8" ?>'
        '<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">'
        "  <D:prop>"
        "    <D:getetag/>"
        "    <C:calendar-data/>"
        "  </D:prop>"
        "  <C:filter>"
        f'    <C:comp-filter name="VCALENDAR">'
        f'      <C:comp-filter name="VEVENT">'
        f'        <C:time-range start="{start}" end="{end}"/>'
        "      </C:comp-filter>"
        "    </C:comp-filter>"
        "  </C:filter>"
        "</C:calendar-query>"
    )

    credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
    req = Request(
        url,
        data=body.encode("utf-8"),
        method="REPORT",
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/xml; charset=utf-8",
            "Depth": "1",
            "User-Agent": "HazelOS/1.0",
        },
    )

    with urlopen(req, timeout=15) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _extract_ical_blocks(xml_body: str) -> list[str]:
    """Pull all <calendar-data> payloads out of a CalDAV REPORT response."""
    blocks = re.findall(
        r"<[^>]*:?calendar-data[^>]*>(.*?)</[^>]*:?calendar-data>",
        xml_body,
        re.DOTALL | re.IGNORECASE,
    )
    return blocks


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_calendar(
    staging_dir: str,
    caldav_url: str = "",
    username: str = "",
    password: str = "",
    days: int = 7,
    simulate: bool = False,
) -> dict:
    """
    Fetch calendar events from a CalDAV server and save to staging directory.

    Args:
        staging_dir: Directory where events.json is written.
        caldav_url:  Full URL to the CalDAV calendar collection.
        username:    CalDAV account username.
        password:    CalDAV account password.
        days:        How many days ahead to fetch (default 7).
        simulate:    If True, skip network and return deterministic fake data.

    Returns:
        {"success": bool, "file": str or None, "summary": str, "count": int}
    """
    calendar_staging = os.path.join(staging_dir, "calendar")
    os.makedirs(calendar_staging, exist_ok=True)
    outpath = os.path.join(calendar_staging, "events.json")

    if simulate:
        data = {
            "fetched_at": datetime.now().isoformat(),
            "days_ahead": days,
            "source": "simulate",
            "events": _SIMULATE_EVENTS,
        }
        with open(outpath, "w") as f:
            json.dump(data, f, indent=2)
        count = len(_SIMULATE_EVENTS)
        return {
            "success": True,
            "file": outpath,
            "summary": f"{count} simulated event(s)",
            "count": count,
        }

    # Real CalDAV fetch
    if not caldav_url:
        return {
            "success": False,
            "file": None,
            "summary": "No caldav_url provided",
            "count": 0,
        }

    try:
        xml_body = _caldav_report(caldav_url, username, password, days)
    except (URLError, OSError) as e:
        logger.error(f"CalDAV fetch failed: {e}")
        return {
            "success": False,
            "file": None,
            "summary": f"Fetch failed: {e}",
            "count": 0,
        }

    ical_blocks = _extract_ical_blocks(xml_body)
    events: list[dict] = []
    for block in ical_blocks:
        # Blocks may be XML-escaped
        block = block.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
        events.extend(_parse_ical_events(block))

    data = {
        "fetched_at": datetime.now().isoformat(),
        "days_ahead": days,
        "source": caldav_url,
        "events": events,
    }

    with open(outpath, "w") as f:
        json.dump(data, f, indent=2)

    count = len(events)
    logger.info(f"Calendar fetched: {count} event(s) from {caldav_url}")
    return {
        "success": True,
        "file": outpath,
        "summary": f"{count} event(s) fetched",
        "count": count,
    }
