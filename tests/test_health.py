"""Tests for HealthMonitor (deep cluster health checks)."""

from unittest.mock import MagicMock, mock_open, patch

import pytest

from hzl_cluster.health import HealthMonitor, _THERMAL_PATH


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def monitor():
    return HealthMonitor(config={})


# ---------------------------------------------------------------------------
# 1. check_disk -- structure and healthy flag
# ---------------------------------------------------------------------------

class TestCheckDisk:
    def test_check_disk_structure(self, monitor):
        result = monitor.check_disk()

        assert "path" in result
        assert "total_gb" in result
        assert "used_gb" in result
        assert "free_gb" in result
        assert "percent" in result
        assert "healthy" in result
        assert isinstance(result["healthy"], bool)
        assert result["total_gb"] >= result["used_gb"]
        assert result["free_gb"] >= 0

    def test_check_disk_healthy_flag_reflects_threshold(self):
        """Monitor with a 100% threshold should always report healthy."""
        monitor = HealthMonitor(config={"thresholds": {"disk_percent": 100}})
        result = monitor.check_disk()
        assert result["healthy"] is True

    def test_check_disk_unhealthy_flag_at_zero_threshold(self):
        """Monitor with a 0% threshold should always report unhealthy."""
        monitor = HealthMonitor(config={"thresholds": {"disk_percent": 0}})
        result = monitor.check_disk()
        assert result["healthy"] is False


# ---------------------------------------------------------------------------
# 2. check_memory -- structure
# ---------------------------------------------------------------------------

class TestCheckMemory:
    def test_check_memory_structure(self, monitor):
        result = monitor.check_memory()

        assert "total_mb" in result
        assert "used_mb" in result
        assert "available_mb" in result
        assert "percent" in result
        assert "healthy" in result
        assert isinstance(result["healthy"], bool)
        assert result["total_mb"] > 0
        assert 0.0 <= result["percent"] <= 100.0


# ---------------------------------------------------------------------------
# 3. check_load -- structure
# ---------------------------------------------------------------------------

class TestCheckLoad:
    def test_check_load_structure(self, monitor):
        result = monitor.check_load()

        assert "load1" in result
        assert "load5" in result
        assert "load15" in result
        assert "cpu_count" in result
        assert "healthy" in result
        assert isinstance(result["healthy"], bool)
        assert result["cpu_count"] >= 1
        assert result["load1"] >= 0.0


# ---------------------------------------------------------------------------
# 4. check_cpu_temperature -- graceful on non-Pi
# ---------------------------------------------------------------------------

class TestCpuTemperature:
    def test_cpu_temperature_graceful_on_non_pi(self, monitor):
        """Returns None when the thermal sysfs node does not exist."""
        with patch("os.path.exists", return_value=False):
            result = monitor.check_cpu_temperature()
        assert result is None

    def test_cpu_temperature_returns_dict_on_pi(self, monitor):
        """Returns a well-formed dict when the thermal node is readable."""
        fake_temp_millic = "52000"  # 52.0 C
        with patch("os.path.exists", return_value=True), \
             patch("builtins.open", mock_open(read_data=fake_temp_millic)):
            result = monitor.check_cpu_temperature()

        assert result is not None
        assert result["celsius"] == pytest.approx(52.0, abs=0.1)
        assert isinstance(result["healthy"], bool)
        assert result["healthy"] is True  # 52C < 80C threshold

    def test_cpu_temperature_unhealthy_above_threshold(self, monitor):
        """Reports unhealthy when temperature exceeds 80C."""
        fake_temp_millic = "85000"  # 85.0 C
        with patch("os.path.exists", return_value=True), \
             patch("builtins.open", mock_open(read_data=fake_temp_millic)):
            result = monitor.check_cpu_temperature()

        assert result is not None
        assert result["healthy"] is False


# ---------------------------------------------------------------------------
# 5. full_report -- structure and overall healthy flag
# ---------------------------------------------------------------------------

class TestFullReport:
    def test_full_report_structure(self, monitor):
        report = monitor.full_report()

        assert "disk" in report
        assert "memory" in report
        assert "load" in report
        assert "temperature" in report  # may be None on non-Pi
        assert "healthy" in report
        assert isinstance(report["healthy"], bool)

    def test_full_report_healthy_is_conjunction(self, monitor):
        """Overall healthy is False when any sub-check is False."""
        with patch.object(monitor, "check_disk", return_value={
            "path": "/", "total_gb": 100, "used_gb": 95, "free_gb": 5,
            "percent": 95.0, "healthy": False,
        }), patch.object(monitor, "check_memory", return_value={
            "total_mb": 4096, "used_mb": 2048, "available_mb": 2048,
            "percent": 50.0, "healthy": True,
        }), patch.object(monitor, "check_load", return_value={
            "load1": 0.5, "load5": 0.5, "load15": 0.5,
            "cpu_count": 4, "healthy": True,
        }), patch.object(monitor, "check_cpu_temperature", return_value=None):
            report = monitor.full_report()

        assert report["healthy"] is False


# ---------------------------------------------------------------------------
# 6. alerts -- empty when everything is healthy
# ---------------------------------------------------------------------------

class TestAlertsEmpty:
    def test_alerts_empty_when_healthy(self, monitor):
        healthy_disk = {
            "path": "/", "total_gb": 100, "used_gb": 50, "free_gb": 50,
            "percent": 50.0, "healthy": True,
        }
        healthy_memory = {
            "total_mb": 4096, "used_mb": 1024, "available_mb": 3072,
            "percent": 25.0, "healthy": True,
        }
        healthy_load = {
            "load1": 0.3, "load5": 0.3, "load15": 0.3,
            "cpu_count": 4, "healthy": True,
        }

        with patch.object(monitor, "check_disk", return_value=healthy_disk), \
             patch.object(monitor, "check_memory", return_value=healthy_memory), \
             patch.object(monitor, "check_load", return_value=healthy_load), \
             patch.object(monitor, "check_cpu_temperature", return_value=None):
            result = monitor.alerts()

        assert result == []


# ---------------------------------------------------------------------------
# 7. alerts -- triggered on low disk
# ---------------------------------------------------------------------------

class TestAlertsOnLowDisk:
    def test_alerts_on_low_disk(self, monitor):
        """A disk at 95% used produces exactly one alert mentioning 95%."""
        low_disk = {
            "path": "/", "total_gb": 100, "used_gb": 95, "free_gb": 5,
            "percent": 95.0, "healthy": False,
        }
        healthy_memory = {
            "total_mb": 4096, "used_mb": 1024, "available_mb": 3072,
            "percent": 25.0, "healthy": True,
        }
        healthy_load = {
            "load1": 0.1, "load5": 0.1, "load15": 0.1,
            "cpu_count": 4, "healthy": True,
        }

        with patch.object(monitor, "check_disk", return_value=low_disk), \
             patch.object(monitor, "check_memory", return_value=healthy_memory), \
             patch.object(monitor, "check_load", return_value=healthy_load), \
             patch.object(monitor, "check_cpu_temperature", return_value=None):
            result = monitor.alerts()

        assert len(result) == 1
        assert "95" in result[0]
        assert "Disk" in result[0] or "disk" in result[0]
