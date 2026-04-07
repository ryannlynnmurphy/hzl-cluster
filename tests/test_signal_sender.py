import subprocess
import pytest
from unittest.mock import patch, MagicMock
from hzl_cluster.senders.signal_sender import send_signal_message


DEFAULTS = dict(
    sender_number="+15550000001",
    recipient_number="+15550000002",
    message="Hazel says hello.",
)


class TestSignalSender:

    def test_simulate_returns_success(self):
        result = send_signal_message(**DEFAULTS, simulate=True)
        assert result["success"] is True
        assert "simulate" in result["summary"].lower()

    def test_simulate_does_not_run_subprocess(self):
        """Simulate mode must never invoke signal-cli."""
        with patch("subprocess.run") as mock_run:
            result = send_signal_message(**DEFAULTS, simulate=True)
            mock_run.assert_not_called()
        assert result["success"] is True

    def test_missing_signal_cli_graceful(self):
        """If signal-cli is not installed, return a clear failure without raising."""
        with patch("subprocess.run", side_effect=FileNotFoundError):
            result = send_signal_message(**DEFAULTS, simulate=False)
        assert result["success"] is False
        assert "signal-cli" in result["summary"].lower()

    def test_message_format(self):
        """signal-cli is called with the correct arguments in the correct order."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stderr = ""

        with patch("subprocess.run", return_value=mock_result) as mock_run:
            result = send_signal_message(**DEFAULTS, simulate=False)

        assert result["success"] is True
        called_cmd = mock_run.call_args[0][0]
        assert called_cmd[0] == "signal-cli"
        assert "-u" in called_cmd
        assert DEFAULTS["sender_number"] in called_cmd
        assert "-m" in called_cmd
        assert DEFAULTS["message"] in called_cmd
        assert DEFAULTS["recipient_number"] in called_cmd
