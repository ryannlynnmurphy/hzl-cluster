"""
Email sender -- sends email via SMTP during Gateway sync cycles.
Supports any SMTP server (ProtonMail Bridge, Gmail, Fastmail, etc.)
Uses Python stdlib smtplib only.
"""
import logging
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate, make_msgid

logger = logging.getLogger("hzl.sender.email")


def send_email(
    from_addr: str,
    to_addr: str,
    subject: str,
    body: str,
    smtp_host: str = "127.0.0.1",
    smtp_port: int = 1025,
    username: str = "",
    password: str = "",
    use_ssl: bool = False,
    use_tls: bool = False,
    simulate: bool = False,
) -> dict:
    """
    Send an email via SMTP.

    Args:
        from_addr: sender address
        to_addr: recipient address
        subject: email subject line
        body: plain-text email body
        smtp_host: SMTP server host (default localhost for ProtonMail Bridge)
        smtp_port: SMTP port (1025 for ProtonMail Bridge, 465 for Gmail SSL, 587 for TLS)
        username: SMTP username (leave empty for unauthenticated local relays)
        password: SMTP password
        use_ssl: wrap connection in SSL from the start (port 465)
        use_tls: upgrade to TLS via STARTTLS after connect (port 587)
        simulate: log intent but do not actually connect or send

    Returns:
        {"success": bool, "message_id": str or None, "summary": str}
    """
    # Build message
    msg = MIMEMultipart()
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)
    message_id = make_msgid(domain=from_addr.split("@")[-1] if "@" in from_addr else "hzl.local")
    msg["Message-ID"] = message_id
    msg.attach(MIMEText(body, "plain"))

    if simulate:
        logger.info(
            f"[simulate] Would send email: '{subject}' from {from_addr} to {to_addr} "
            f"via {smtp_host}:{smtp_port}"
        )
        return {
            "success": True,
            "message_id": message_id,
            "summary": f"[simulate] Email queued: '{subject}' to {to_addr}",
        }

    # Real SMTP send
    try:
        context = ssl.create_default_context() if (use_ssl or use_tls) else None

        if use_ssl:
            conn = smtplib.SMTP_SSL(smtp_host, smtp_port, context=context)
        else:
            conn = smtplib.SMTP(smtp_host, smtp_port)
            if use_tls:
                conn.starttls(context=context)

        if username and password:
            conn.login(username, password)

        conn.sendmail(from_addr, [to_addr], msg.as_string())
        conn.quit()

        summary = f"Email sent: '{subject}' to {to_addr}"
        logger.info(summary)
        return {
            "success": True,
            "message_id": message_id,
            "summary": summary,
        }

    except (smtplib.SMTPException, OSError, ConnectionRefusedError) as e:
        logger.error(f"Email send failed: {e}")
        return {
            "success": False,
            "message_id": None,
            "summary": f"SMTP error: {e}",
        }
