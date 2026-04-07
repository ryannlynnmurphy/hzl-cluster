"""
Contacts fetcher — pulls contacts from a CardDAV server.
Parses vCards and saves JSON to staging. No external libraries required.
"""
import json
import logging
import os
from datetime import datetime
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import URLError
from base64 import b64encode

logger = logging.getLogger("hzl.fetcher.contacts")

SIMULATE_CONTACTS = [
    {
        "name": "Hazel Studio",
        "email": "hello@hzlstudio.com",
        "phone": "+1-212-555-0101",
        "organization": "HZL Studio",
    },
    {
        "name": "Ryann Murphy",
        "email": "ryann@hzlstudio.com",
        "phone": "+1-212-555-0102",
        "organization": "HZL Studio",
    },
    {
        "name": "Technical Support",
        "email": "support@example.com",
        "phone": "+1-800-555-0199",
        "organization": "Example Corp",
    },
    {
        "name": "Ada Lovelace",
        "email": "ada@babbage.io",
        "phone": "+44-20-555-0001",
        "organization": "Analytical Engine Inc.",
    },
]


def _parse_vcard(vcard_text: str) -> Optional[dict]:
    """
    Parse a single vCard block into a contact dict.
    Extracts FN, EMAIL, TEL, and ORG fields.
    Returns None if the block has no usable name.
    """
    contact = {"name": "", "email": "", "phone": "", "organization": ""}

    for raw_line in vcard_text.splitlines():
        # vCard line folding: lines beginning with whitespace continue the previous
        line = raw_line.strip()
        if not line:
            continue

        upper = line.upper()

        # FN — formatted name
        if upper.startswith("FN:") or upper.startswith("FN;"):
            contact["name"] = line.split(":", 1)[-1].strip()

        # EMAIL — take the first one found
        elif ("EMAIL" in upper) and not contact["email"]:
            contact["email"] = line.split(":", 1)[-1].strip()

        # TEL — take the first one found
        elif ("TEL" in upper) and not contact["phone"]:
            contact["phone"] = line.split(":", 1)[-1].strip()

        # ORG — first component before semicolon
        elif upper.startswith("ORG:") or upper.startswith("ORG;"):
            org_value = line.split(":", 1)[-1].strip()
            contact["organization"] = org_value.split(";")[0].strip()

    return contact if contact["name"] else None


def _fetch_vcards(url: str, username: str = "", password: str = "") -> list[str]:
    """
    Perform a CardDAV REPORT request and return a list of raw vCard strings.
    Falls back to a plain GET if the server does not support REPORT.
    """
    body = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<C:addressbook-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav">'
        "<D:prop><D:getetag/><C:address-data/></D:prop>"
        "</C:addressbook-query>"
    ).encode("utf-8")

    headers = {
        "User-Agent": "HazelOS/1.0",
        "Content-Type": "application/xml; charset=utf-8",
        "Depth": "1",
    }

    if username:
        creds = b64encode(f"{username}:{password}".encode()).decode()
        headers["Authorization"] = f"Basic {creds}"

    req = Request(url, data=body, headers=headers, method="REPORT")

    try:
        with urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except URLError as e:
        raise RuntimeError(f"CardDAV REPORT failed: {e}") from e

    # Pull each BEGIN:VCARD ... END:VCARD block out of the response XML
    vcards = []
    in_card = False
    current: list[str] = []
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped == "BEGIN:VCARD":
            in_card = True
            current = [stripped]
        elif stripped == "END:VCARD" and in_card:
            current.append(stripped)
            vcards.append("\n".join(current))
            in_card = False
            current = []
        elif in_card:
            current.append(stripped)

    return vcards


def fetch_contacts(
    staging_dir: str,
    carddav_url: str = "",
    username: str = "",
    password: str = "",
    simulate: bool = False,
) -> dict:
    """
    Fetch contacts from a CardDAV server and save to staging directory.

    Args:
        staging_dir:  Path where contacts.json will be written.
        carddav_url:  Full URL of the CardDAV address book endpoint.
        username:     Optional HTTP Basic auth username.
        password:     Optional HTTP Basic auth password.
        simulate:     If True, write fake contacts without a network call.

    Returns:
        {"success": bool, "file": str or None, "summary": str}
    """
    os.makedirs(staging_dir, exist_ok=True)

    if simulate:
        data = {
            "fetched_at": datetime.now().isoformat(),
            "source": "simulate",
            "contacts": SIMULATE_CONTACTS,
        }
        outpath = os.path.join(staging_dir, "contacts.json")
        with open(outpath, "w") as f:
            json.dump(data, f, indent=2)
        count = len(SIMULATE_CONTACTS)
        return {
            "success": True,
            "file": outpath,
            "summary": f"{count} contacts (simulated)",
        }

    if not carddav_url:
        return {
            "success": False,
            "file": None,
            "summary": "No CardDAV URL provided",
        }

    try:
        raw_vcards = _fetch_vcards(carddav_url, username, password)
    except RuntimeError as e:
        logger.error(f"Contacts fetch failed: {e}")
        return {"success": False, "file": None, "summary": str(e)}

    contacts = []
    for vcard_text in raw_vcards:
        contact = _parse_vcard(vcard_text)
        if contact:
            contacts.append(contact)

    data = {
        "fetched_at": datetime.now().isoformat(),
        "source": carddav_url,
        "contacts": contacts,
    }

    outpath = os.path.join(staging_dir, "contacts.json")
    with open(outpath, "w") as f:
        json.dump(data, f, indent=2)

    count = len(contacts)
    summary = f"{count} contact{'s' if count != 1 else ''} fetched"
    logger.info(f"Contacts fetched: {summary}")
    return {"success": True, "file": outpath, "summary": summary}
