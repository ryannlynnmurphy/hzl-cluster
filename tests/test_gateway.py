"""
test_gateway.py — Tests for GatewayDaemon.

All tests use simulate=True so no real GPIO, nmcli, or network calls are made.
"""

import asyncio
import os
import tempfile

import pytest

from hzl_cluster.gateway import GatewayDaemon
from hzl_cluster.queue_hub import HazelMessage
from hzl_cluster.relay import RelayState


def _run(coro):
    """Run a coroutine in a fresh event loop."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class TestGatewayDaemon:
    def setup_method(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        base = self._tmpdir.name

        staging_dir    = os.path.join(base, "staging")
        quarantine_dir = os.path.join(base, "quarantine")
        db_path        = os.path.join(base, "queue.db")

        os.makedirs(staging_dir,    exist_ok=True)
        os.makedirs(quarantine_dir, exist_ok=True)

        self.staging_dir = staging_dir

        self.config = {
            "relay": {
                "gpio_pin":             17,
                "max_internet_duration": 600,
                "watchdog_interval":    30,
                "wifi_interface":       "wlan0",
                "ethernet_interface":   "eth0",
            },
            "sync": {
                "staging_dir":        staging_dir,
                "quarantine_dir":     quarantine_dir,
                "max_staging_size_mb": 500,
            },
            "queue": {
                "db_path": db_path,
            },
        }

        self.daemon = GatewayDaemon(self.config, simulate=True)

    def teardown_method(self):
        self.daemon.close()
        self._tmpdir.cleanup()

    # ------------------------------------------------------------------

    def test_queue_request(self):
        """Queuing a message increments the pending count."""
        before = self.daemon.queue.status()["total_pending"]

        msg = HazelMessage.create(
            source="test",
            destination="gateway",
            msg_type="fetch",
            action="download",
            payload={"url": "https://example.com/data.json"},
        )
        result = self.daemon.queue_request(msg)

        assert result["accepted"] == 1
        assert result["rejected"] == 0

        after = self.daemon.queue.status()["total_pending"]
        assert after == before + 1

    def test_relay_state(self):
        """Initial relay state must be CORE_CONNECTED."""
        assert self.daemon.relay.state == RelayState.CORE_CONNECTED

    def test_sync_cycle_smoke(self):
        """Sync cycle runs and relay returns to CORE_CONNECTED afterward."""
        result = _run(self.daemon.run_sync_cycle())

        assert isinstance(result, dict)
        assert "fetched"     in result
        assert "scanned"     in result
        assert "quarantined" in result
        assert "delivered"   in result

        # After the cycle the relay must be back in core mode.
        assert self.daemon.relay.state == RelayState.CORE_CONNECTED

    def test_staging_list_empty(self):
        """Empty staging directory returns an empty list."""
        entries = self.daemon.list_staging()
        assert entries == []

    def test_staging_list_with_files(self):
        """A file placed in staging appears in list_staging()."""
        test_file = os.path.join(self.staging_dir, "report.txt")
        with open(test_file, "w") as fh:
            fh.write("hello cluster\n")

        entries = self.daemon.list_staging()

        assert len(entries) == 1
        assert entries[0]["name"] == "report.txt"
        assert entries[0]["size"] > 0
        assert "modified" in entries[0]
