"""
Signal sender -- sends Signal messages via signal-cli during Gateway sync cycles.
Requires signal-cli to be installed and registered on the host (typically a Pi node).
https://github.com/AsamK/signal-cli
"""
import logging
import subprocess

logger = logging.getLogger("hzl.sender.signal")


def send_signal_message(
    sender_number: str,
    recipient_number: str,
    message: str,
    simulate: bool = False,
) -> dict:
    """
    Send a Signal message via signal-cli.

    Args:
        sender_number: registered Signal phone number for this node (e.g. "+15551234567")
        recipient_number: destination phone number (e.g. "+15559876543")
        message: plain-text message body
        simulate: log intent but do not invoke signal-cli

    Returns:
        {"success": bool, "summary": str}
    """
    if simulate:
        logger.info(
            f"[simulate] Would send Signal message from {sender_number} "
            f"to {recipient_number}: {message!r}"
        )
        return {
            "success": True,
            "summary": (
                f"[simulate] Signal message queued: to {recipient_number} — {message!r}"
            ),
        }

    cmd = [
        "signal-cli",
        "-u", sender_number,
        "send",
        "-m", message,
        recipient_number,
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode == 0:
            summary = f"Signal message sent to {recipient_number}"
            logger.info(summary)
            return {"success": True, "summary": summary}

        stderr = result.stderr.strip()
        summary = f"signal-cli exited {result.returncode}: {stderr}"
        logger.error(summary)
        return {"success": False, "summary": summary}

    except FileNotFoundError:
        summary = "signal-cli not found — install signal-cli and ensure it is on PATH"
        logger.error(summary)
        return {"success": False, "summary": summary}

    except subprocess.TimeoutExpired:
        summary = "signal-cli timed out after 30 seconds"
        logger.error(summary)
        return {"success": False, "summary": summary}

    except OSError as e:
        summary = f"signal-cli OS error: {e}"
        logger.error(summary)
        return {"success": False, "summary": summary}
