"""Tests for ClusterDiagnostics -- network troubleshooting tools."""

from __future__ import annotations

import asyncio
import socket
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _run(coro):
    """Run an async coroutine synchronously."""
    return asyncio.run(coro)

from hzl_cluster.diagnostics import ClusterDiagnostics


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _make_diag(nodes: dict | None = None) -> ClusterDiagnostics:
    """Return a ClusterDiagnostics instance with optional node overrides."""
    config = {"nodes": nodes or {}}
    return ClusterDiagnostics(config)


def _unreachable_config() -> dict:
    return {
        "nodes": {
            "dead-node": {
                "ip": "192.0.2.1",   # TEST-NET address -- guaranteed unreachable
                "orchestrator_port": 9999,
            }
        }
    }


# ---------------------------------------------------------------------------
# 1. test_check_port_localhost -- TCP connect on a port we know is open
# ---------------------------------------------------------------------------

class TestCheckPortLocalhost:
    def test_check_port_localhost(self):
        """check_port_open succeeds on a port that is actively listening."""
        # Create a real listening socket so the test is self-contained.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
            srv.bind(("127.0.0.1", 0))
            srv.listen(1)
            _, port = srv.getsockname()

            diag = _make_diag()
            result = diag.check_port_open("127.0.0.1", port, timeout=2.0)

        assert result["host"] == "127.0.0.1"
        assert result["port"] == port
        assert result["open"] is True
        assert result["latency_ms"] is not None
        assert result["latency_ms"] >= 0
        assert result["error"] is None

    def test_check_port_closed_returns_false(self):
        """check_port_open returns open=False when nothing is listening."""
        # Find a free port then immediately close it so we know it's shut.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        # Port is now closed.

        diag = _make_diag()
        result = diag.check_port_open("127.0.0.1", port, timeout=1.0)

        assert result["open"] is False
        assert result["error"] is not None


# ---------------------------------------------------------------------------
# 2. test_check_dns_localhost -- resolve 'localhost'
# ---------------------------------------------------------------------------

class TestCheckDnsLocalhost:
    def test_check_dns_localhost(self):
        """Resolving 'localhost' should succeed on any dev machine."""
        diag = _make_diag()
        result = diag.check_dns("localhost")

        assert result["hostname"] == "localhost"
        assert result["resolved"] is True
        assert result["ip"] is not None
        assert result["error"] is None

    def test_check_dns_unknown_host_fails_gracefully(self):
        """A bogus hostname must return resolved=False with an error string."""
        diag = _make_diag()
        result = diag.check_dns("definitely-not-a-real-host.hzl.invalid")

        assert result["resolved"] is False
        assert result["ip"] is None
        assert result["error"] is not None
        assert isinstance(result["error"], str)


# ---------------------------------------------------------------------------
# 3. test_ping_node_unreachable -- graceful handling of non-existent host
# ---------------------------------------------------------------------------

class TestPingNodeUnreachable:
    def test_ping_node_unreachable(self):
        """ping_node on a non-existent host returns reachable=False, no crash."""
        diag = _make_diag()
        result = _run(diag.ping_node("192.0.2.255", 9999, timeout=0.5))

        assert result["host"] == "192.0.2.255"
        assert result["port"] == 9999
        assert result["reachable"] is False
        assert result["latency_ms"] is not None
        assert result["latency_ms"] >= 0
        assert result["error"] is not None

    def test_ping_node_returns_expected_keys(self):
        """All five required keys must be present in the return dict."""
        diag = _make_diag()
        result = _run(diag.ping_node("192.0.2.1", 1234, timeout=0.3))

        for key in ("host", "port", "reachable", "latency_ms", "error"):
            assert key in result, f"Missing key: {key}"


# ---------------------------------------------------------------------------
# 4. test_full_diagnostic_structure -- report has expected top-level keys
# ---------------------------------------------------------------------------

class TestFullDiagnosticStructure:
    def test_full_diagnostic_structure(self):
        """full_diagnostic must return a dict with all documented keys."""
        diag = ClusterDiagnostics(_unreachable_config())
        report = _run(diag.full_diagnostic())

        expected_keys = {
            "nodes", "dns", "ports", "reachable_count",
            "unreachable_count", "dns_failures", "closed_ports",
            "issues", "healthy",
        }
        for key in expected_keys:
            assert key in report, f"Missing key in full_diagnostic report: {key}"

    def test_full_diagnostic_counts_are_consistent(self):
        """reachable_count + unreachable_count == number of configured nodes."""
        diag = ClusterDiagnostics(_unreachable_config())
        report = _run(diag.full_diagnostic())

        total = report["reachable_count"] + report["unreachable_count"]
        assert total == len(report["nodes"])

    def test_full_diagnostic_issues_is_list(self):
        """issues field must always be a list, even when empty."""
        diag = _make_diag()
        report = _run(diag.full_diagnostic())

        assert isinstance(report["issues"], list)

    def test_full_diagnostic_healthy_false_when_unreachable(self):
        """healthy flag is False when an unreachable node exists."""
        diag = ClusterDiagnostics(_unreachable_config())
        report = _run(diag.full_diagnostic())

        assert report["healthy"] is False


# ---------------------------------------------------------------------------
# 5. test_suggest_fixes_unreachable -- fix suggestions for a down node
# ---------------------------------------------------------------------------

class TestSuggestFixesUnreachable:
    def _report_with_unreachable_node(self, error: str = "timed out") -> dict:
        return {
            "nodes": [
                {
                    "host": "192.0.2.1",
                    "port": 9000,
                    "reachable": False,
                    "latency_ms": 2001.0,
                    "error": error,
                }
            ],
            "dns": [],
            "ports": [],
            "reachable_count": 0,
            "unreachable_count": 1,
            "dns_failures": [],
            "closed_ports": [],
            "issues": ["Node 192.0.2.1:9000 is unreachable -- timed out"],
            "healthy": False,
        }

    def test_suggest_fixes_returns_list(self):
        diag = _make_diag()
        report = self._report_with_unreachable_node()
        fixes = diag.suggest_fixes(report)

        assert isinstance(fixes, list)
        assert len(fixes) > 0

    def test_suggest_fixes_mentions_unreachable_host(self):
        """At least one suggestion must reference the unreachable host."""
        diag = _make_diag()
        report = self._report_with_unreachable_node(error="timed out after 2s")
        fixes = diag.suggest_fixes(report)

        combined = " ".join(fixes)
        assert "192.0.2.1" in combined

    def test_suggest_fixes_timeout_advice(self):
        """A timeout error should trigger ping/power-off advice."""
        diag = _make_diag()
        report = self._report_with_unreachable_node(error="timed out after 2s")
        fixes = diag.suggest_fixes(report)

        combined = " ".join(fixes).lower()
        assert "timeout" in combined or "timed out" in combined or "ping" in combined

    def test_suggest_fixes_refused_advice(self):
        """A connection-refused error should trigger systemctl advice."""
        diag = _make_diag()
        report = self._report_with_unreachable_node(error="Connection refused")
        fixes = diag.suggest_fixes(report)

        combined = " ".join(fixes).lower()
        assert "systemctl" in combined or "running" in combined

    def test_suggest_fixes_dns_failure_advice(self):
        """DNS failures should trigger /etc/hosts or DNS server advice."""
        diag = _make_diag()
        report = {
            "nodes": [],
            "dns": [],
            "ports": [],
            "reachable_count": 0,
            "unreachable_count": 0,
            "dns_failures": ["hazel-worker-03"],
            "closed_ports": [],
            "issues": ["DNS resolution failed for hazel-worker-03"],
            "healthy": False,
        }
        fixes = diag.suggest_fixes(report)

        combined = " ".join(fixes).lower()
        assert "dns" in combined or "/etc/hosts" in combined

    def test_suggest_fixes_no_issues_returns_all_clear(self):
        """A healthy report should produce a single all-clear message."""
        diag = _make_diag()
        healthy_report = {
            "nodes": [
                {
                    "host": "192.168.1.10",
                    "port": 9000,
                    "reachable": True,
                    "latency_ms": 4.2,
                    "error": None,
                }
            ],
            "dns": [],
            "ports": [],
            "reachable_count": 1,
            "unreachable_count": 0,
            "dns_failures": [],
            "closed_ports": [],
            "issues": [],
            "healthy": True,
        }
        fixes = diag.suggest_fixes(healthy_report)

        assert len(fixes) == 1
        assert "no fixes" in fixes[0].lower() or "reachable" in fixes[0].lower()
