"""
Email fetcher — pulls email via IMAP and saves to staging.
Supports any IMAP server (ProtonMail Bridge, Gmail, Fastmail, etc.)
"""
import email
import email.policy
import imaplib
import json
import logging
import os
from datetime import datetime, timedelta
from typing import List, Optional

logger = logging.getLogger("hzl.fetcher.email")


def fetch_email(
    staging_dir: str,
    imap_host: str = "127.0.0.1",
    imap_port: int = 1143,
    username: str = "",
    password: str = "",
    folder: str = "INBOX",
    since_days: int = 3,
    max_emails: int = 50,
    use_ssl: bool = False,
    simulate: bool = False,
) -> dict:
    """
    Fetch emails via IMAP and save to staging directory.

    Args:
        staging_dir: where to save .eml files and index
        imap_host: IMAP server (default localhost for ProtonMail Bridge)
        imap_port: IMAP port (1143 for ProtonMail Bridge, 993 for Gmail SSL)
        username: IMAP username
        password: IMAP password
        folder: mailbox folder to check
        since_days: only fetch emails from last N days
        max_emails: maximum emails to fetch
        use_ssl: use SSL connection (True for Gmail, False for local bridges)
        simulate: return fake data without connecting

    Returns: {"success": bool, "emails_fetched": int, "file": str, "summary": str}
    """
    mail_dir = os.path.join(staging_dir, "mail")
    os.makedirs(mail_dir, exist_ok=True)

    if simulate:
        fake_emails = [
            {
                "id": "msg001",
                "from": "Tim <tim@bloomberg.com>",
                "to": username or "ryann@hzl.ai",
                "subject": "Re: Solutions Engineer Role",
                "date": datetime.now().isoformat(),
                "snippet": "Thanks for your interest. Are you available for a call next Tuesday?",
                "has_attachments": False,
            },
            {
                "id": "msg002",
                "from": "NYU ITP <admissions@nyu.edu>",
                "to": username or "ryann@hzl.ai",
                "subject": "Application Status Update",
                "date": datetime.now().isoformat(),
                "snippet": "We are pleased to inform you that your application has been moved to the next stage.",
                "has_attachments": True,
            },
            {
                "id": "msg003",
                "from": "GitHub <noreply@github.com>",
                "to": username or "ryann@hzl.ai",
                "subject": "New star on hzl-cluster",
                "date": datetime.now().isoformat(),
                "snippet": "Someone starred your repository ryannlynnmurphy/hzl-cluster",
                "has_attachments": False,
            },
        ]

        index_path = os.path.join(mail_dir, "index.json")
        with open(index_path, "w") as f:
            json.dump({
                "fetched_at": datetime.now().isoformat(),
                "account": username or "simulate",
                "emails": fake_emails,
            }, f, indent=2)

        return {
            "success": True,
            "emails_fetched": len(fake_emails),
            "file": index_path,
            "summary": f"{len(fake_emails)} emails fetched",
        }

    # Real IMAP fetch
    try:
        if use_ssl:
            conn = imaplib.IMAP4_SSL(imap_host, imap_port)
        else:
            conn = imaplib.IMAP4(imap_host, imap_port)

        conn.login(username, password)
        conn.select(folder, readonly=True)

        # Search for recent emails
        since_date = (datetime.now() - timedelta(days=since_days)).strftime("%d-%b-%Y")
        status, message_ids = conn.search(None, f'(SINCE {since_date})')

        if status != "OK":
            conn.logout()
            return {"success": False, "emails_fetched": 0, "file": None, "summary": "IMAP search failed"}

        ids = message_ids[0].split()
        ids = ids[-max_emails:]  # take most recent

        email_index = []

        for msg_id in ids:
            status, msg_data = conn.fetch(msg_id, "(RFC822)")
            if status != "OK":
                continue

            raw = msg_data[0][1]
            msg = email.message_from_bytes(raw, policy=email.policy.default)

            # Save .eml file
            msg_filename = f"msg_{msg_id.decode()}.eml"
            eml_path = os.path.join(mail_dir, msg_filename)
            with open(eml_path, "wb") as f:
                f.write(raw)

            # Extract metadata
            from_addr = str(msg.get("From", ""))
            subject = str(msg.get("Subject", ""))
            date = str(msg.get("Date", ""))

            # Get text snippet
            snippet = ""
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        try:
                            snippet = part.get_content()[:200]
                        except Exception:
                            pass
                        break
            else:
                try:
                    snippet = msg.get_content()[:200]
                except Exception:
                    pass

            has_attachments = any(
                part.get_content_disposition() == "attachment"
                for part in msg.walk()
            ) if msg.is_multipart() else False

            email_index.append({
                "id": msg_id.decode(),
                "from": from_addr,
                "to": str(msg.get("To", "")),
                "subject": subject,
                "date": date,
                "snippet": snippet.strip()[:200],
                "has_attachments": has_attachments,
                "file": msg_filename,
            })

        conn.logout()

        # Write index
        index_path = os.path.join(mail_dir, "index.json")
        with open(index_path, "w") as f:
            json.dump({
                "fetched_at": datetime.now().isoformat(),
                "account": username,
                "emails": email_index,
            }, f, indent=2)

        summary = f"{len(email_index)} emails fetched"
        logger.info(f"Email: {summary}")
        return {
            "success": True,
            "emails_fetched": len(email_index),
            "file": index_path,
            "summary": summary,
        }

    except (imaplib.IMAP4.error, OSError, ConnectionRefusedError) as e:
        logger.error(f"Email fetch failed: {e}")
        return {
            "success": False,
            "emails_fetched": 0,
            "file": None,
            "summary": f"IMAP error: {e}",
        }
