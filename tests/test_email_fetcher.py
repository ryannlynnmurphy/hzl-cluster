import json
import os
import tempfile
import pytest
from hzl_cluster.fetchers.email_fetcher import fetch_email


class TestEmailFetcher:
    def setup_method(self):
        self.staging = tempfile.mkdtemp()

    def test_simulate_returns_emails(self):
        result = fetch_email(self.staging, simulate=True)
        assert result["success"] is True
        assert result["emails_fetched"] == 3

    def test_simulate_writes_index(self):
        result = fetch_email(self.staging, simulate=True)
        with open(result["file"]) as f:
            data = json.load(f)
        assert "emails" in data
        assert len(data["emails"]) == 3

    def test_simulate_email_structure(self):
        result = fetch_email(self.staging, simulate=True)
        with open(result["file"]) as f:
            data = json.load(f)
        email_entry = data["emails"][0]
        assert "from" in email_entry
        assert "subject" in email_entry
        assert "snippet" in email_entry
        assert "date" in email_entry
        assert "has_attachments" in email_entry

    def test_simulate_creates_mail_dir(self):
        result = fetch_email(self.staging, simulate=True)
        mail_dir = os.path.join(self.staging, "mail")
        assert os.path.isdir(mail_dir)

    def test_real_connection_refused(self):
        """Real IMAP to localhost should fail gracefully."""
        result = fetch_email(
            self.staging,
            imap_host="127.0.0.1",
            imap_port=19999,  # nothing listening
            username="test",
            password="test",
            simulate=False,
        )
        assert result["success"] is False
        assert "error" in result["summary"].lower() or "IMAP" in result["summary"]
