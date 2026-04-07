import smtplib
import pytest
from unittest.mock import patch
from hzl_cluster.senders.email_sender import send_email


DEFAULTS = dict(
    from_addr="hazel@hzl.local",
    to_addr="ryann@hzl.ai",
    subject="Daily Briefing",
    body="Here is your morning update.",
)


class TestEmailSender:

    def test_simulate_returns_success(self):
        result = send_email(**DEFAULTS, simulate=True)
        assert result["success"] is True
        assert result["message_id"] is not None
        assert "simulate" in result["summary"].lower()

    def test_simulate_does_not_connect(self):
        """Simulate mode must never open a network connection."""
        with patch("smtplib.SMTP") as mock_smtp, patch("smtplib.SMTP_SSL") as mock_ssl:
            result = send_email(**DEFAULTS, simulate=True)
            mock_smtp.assert_not_called()
            mock_ssl.assert_not_called()
        assert result["success"] is True

    def test_real_connection_refused(self):
        """Connecting to a port with nothing listening should fail gracefully."""
        result = send_email(
            **DEFAULTS,
            smtp_host="127.0.0.1",
            smtp_port=19999,
            simulate=False,
        )
        assert result["success"] is False
        assert result["message_id"] is None
        assert "smtp" in result["summary"].lower() or "error" in result["summary"].lower()

    def test_message_structure(self):
        """Simulate result must carry message_id, success, and summary keys."""
        result = send_email(**DEFAULTS, simulate=True)
        assert "success" in result
        assert "message_id" in result
        assert "summary" in result
        # message_id should look like a proper RFC 2822 Message-ID
        assert result["message_id"].startswith("<")
        assert result["message_id"].endswith(">")
