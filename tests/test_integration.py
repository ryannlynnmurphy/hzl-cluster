"""
Integration test: full message flow through the air-gapped cluster.
Simulates: Phone -> Core (ingest) -> Gateway (fetch) -> Core (deliver)
"""
import asyncio
import os
import tempfile
import pytest
from hzl_cluster.queue_hub import HazelMessage, QueueHub
from hzl_cluster.gateway import GatewayDaemon


def _run(coro):
    """Run a coroutine in a fresh event loop."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class TestFullMessageFlow:
    def setup_method(self):
        self.tmp_staging = tempfile.mkdtemp()
        self.tmp_quarantine = tempfile.mkdtemp()
        self.tmp_core_db = tempfile.mktemp(suffix=".db")
        self.tmp_gw_db = tempfile.mktemp(suffix=".db")

        self.core_config = {"queue": {"db_path": self.tmp_core_db}}
        self.gw_config = {
            "relay": {
                "gpio_pin": 17, "max_internet_duration": 600,
                "watchdog_interval": 30, "wifi_interface": "wlan0",
                "ethernet_interface": "eth0", "watchdog_policy": "finish_active",
            },
            "sync": {
                "staging_dir": self.tmp_staging, "quarantine_dir": self.tmp_quarantine,
                "max_staging_size_mb": 500, "content_scan": True, "schedule": "0 6 * * *",
            },
            "queue": {"db_path": self.tmp_gw_db},
        }
        self.core_hub = QueueHub(self.core_config)
        self.gateway = GatewayDaemon(self.gw_config, simulate=True)

    def teardown_method(self):
        self.core_hub.close()
        self.gateway.close()
        for p in [self.tmp_core_db, self.tmp_gw_db]:
            if os.path.exists(p):
                os.unlink(p)

    def test_phone_to_core_to_gateway_flow(self):
        """Phone creates fetch.email -> Core ingests -> forwards to Gateway -> Gateway processes."""
        phone_msg = HazelMessage.create(
            source="hazel-phone", destination="gateway",
            msg_type="fetch", action="fetch.email",
            payload={"account": "protonmail", "since": "2026-04-06"},
        )
        # Core ingests
        result = self.core_hub.ingest([phone_msg])
        assert result["accepted"] == 1
        # Core gets pending for gateway
        outbound = self.core_hub.get_outbound("gateway")
        assert len(outbound) == 1
        assert outbound[0].action == "fetch.email"
        # Core forwards to Gateway
        self.gateway.queue_request(outbound[0])
        # Gateway runs sync cycle
        sync_result = _run(self.gateway.run_sync_cycle())
        assert sync_result["fetched"] == 1
        assert self.gateway.relay.state.value == "core_connected"

    def test_multiple_requests_batch(self):
        """Multiple fetch requests processed in one sync cycle."""
        for action in ["fetch.email", "fetch.weather", "fetch.news"]:
            msg = HazelMessage.create(
                source="hazel-core", destination="gateway",
                msg_type="fetch", action=action, payload={},
            )
            self.gateway.queue_request(msg)
        result = _run(self.gateway.run_sync_cycle())
        assert result["fetched"] == 3

    def test_scanner_quarantines_during_sync(self):
        """Files in staging get scanned during sync cycle."""
        exe_path = os.path.join(self.tmp_staging, "virus.exe")
        with open(exe_path, "wb") as f:
            f.write(b"MZ\x90\x00")
        result = _run(self.gateway.run_sync_cycle())
        assert result["quarantined"] == 1
        assert not os.path.exists(exe_path)

    def test_core_queue_status(self):
        """Core hub reports accurate queue status."""
        for dest in ["gateway", "gateway", "hazel-phone"]:
            self.core_hub.ingest([HazelMessage.create(
                source="core", destination=dest,
                msg_type="fetch", action="fetch.email", payload={},
            )])
        status = self.core_hub.status()
        assert status["total_pending"] == 3
        assert status["by_destination"]["gateway"] == 2
        assert status["by_destination"]["hazel-phone"] == 1

    def test_relay_audit_trail(self):
        """Sync cycle produces audit log entries."""
        _run(self.gateway.run_sync_cycle())
        log = self.gateway.relay.get_audit_log()
        assert len(log) >= 2  # at least RELAY_OPEN and RELAY_CLOSE

    def test_weather_fetch_creates_file_during_sync(self):
        """Weather fetch request produces actual weather.json in staging."""
        msg = HazelMessage.create(
            source="hazel-core", destination="gateway",
            msg_type="fetch", action="fetch.weather",
            payload={"latitude": 40.7128, "longitude": -74.0060, "days": 3},
        )
        self.gateway.queue_request(msg)
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(self.gateway.run_sync_cycle())
        loop.close()
        assert result["fetched"] == 1
        # Check that weather.json was actually created in staging
        weather_file = os.path.join(self.tmp_staging, "weather.json")
        assert os.path.exists(weather_file)
        import json
        with open(weather_file) as f:
            data = json.load(f)
        assert "current" in data
        assert data["current"]["temperature"] == 72.0  # simulate mode
