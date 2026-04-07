"""
tests/test_cli.py — Basic smoke tests for the unified hazel CLI.
"""

import subprocess
import sys


def _run(*args: str) -> subprocess.CompletedProcess:
    """Run the CLI as a subprocess and return the result."""
    return subprocess.run(
        [sys.executable, "-m", "hzl_cluster.cli", *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


class TestCLI:
    def test_version(self):
        """hazel version prints a version string containing a dot."""
        result = _run("version")
        assert result.returncode == 0
        output = result.stdout.strip()
        assert "hazel" in output
        assert "." in output  # e.g. "hazel 1.0.0"

    def test_help(self):
        """hazel --help exits cleanly and lists key subcommands."""
        result = _run("--help")
        assert result.returncode == 0
        combined = result.stdout + result.stderr
        assert "status" in combined
        assert "dashboard" in combined
        assert "deploy" in combined
        assert "relay" in combined
        assert "queue" in combined

    def test_unknown_command(self):
        """hazel <unknown> exits with a non-zero code."""
        result = _run("not-a-real-command")
        assert result.returncode != 0

    def test_queue_send_missing_required_args(self):
        """hazel queue send without required flags exits with an error."""
        result = _run("queue", "send")
        assert result.returncode != 0

    def test_relay_no_subcommand(self):
        """hazel relay with no subcommand exits with an error."""
        result = _run("relay")
        assert result.returncode != 0

    def test_fetch_invalid_target(self):
        """hazel fetch bogus exits with an error."""
        result = _run("fetch", "bogus")
        assert result.returncode != 0

    def test_deploy_no_role(self):
        """hazel deploy without --role exits with an error."""
        result = _run("deploy")
        assert result.returncode != 0
