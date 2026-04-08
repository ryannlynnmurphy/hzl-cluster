"""
test_power.py — Tests for PowerManager.
"""

from __future__ import annotations

import time
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from hzl_cluster.power import PowerManager


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _sim_config(node_count: int = 3) -> dict:
    """Return a simulate-mode config."""
    return {"simulate": True, "node_count": node_count}


# ─────────────────────────────────────────────────────────────
# 1. test_uptime_structure
# ─────────────────────────────────────────────────────────────

class TestUptimeStructure:
    """get_uptime() returns the expected keys and sane values."""

    def test_keys_present(self):
        pm = PowerManager(_sim_config())
        result = pm.get_uptime()
        assert "seconds" in result
        assert "human_readable" in result

    def test_seconds_is_non_negative(self):
        pm = PowerManager(_sim_config())
        assert pm.get_uptime()["seconds"] >= 0

    def test_human_readable_is_string(self):
        pm = PowerManager(_sim_config())
        assert isinstance(pm.get_uptime()["human_readable"], str)

    def test_human_readable_for_known_duration(self):
        """_format_uptime produces correct output for a fixed duration."""
        # 2 days, 14 hours, 5 minutes = 2*86400 + 14*3600 + 5*60
        seconds = 2 * 86400 + 14 * 3600 + 5 * 60
        result = PowerManager._format_uptime(seconds)
        assert "2 days" in result
        assert "14 hours" in result
        assert "5 minutes" in result

    def test_human_readable_singular(self):
        result = PowerManager._format_uptime(3661)   # 1 hour, 1 minute, 1 second
        assert "1 hour" in result
        assert "1 minute" in result

    def test_human_readable_under_one_minute(self):
        result = PowerManager._format_uptime(45)
        assert "second" in result


# ─────────────────────────────────────────────────────────────
# 2. test_estimate_power_draw
# ─────────────────────────────────────────────────────────────

class TestEstimatePowerDraw:
    """estimate_power_draw() returns correct watt/amp values per Pi 5 specs."""

    def test_keys_present(self):
        pm = PowerManager(_sim_config())
        result = pm.estimate_power_draw(4)
        for key in ("node_count", "idle_watts", "load_watts",
                    "idle_amps_at_5v", "load_amps_at_5v", "note"):
            assert key in result

    def test_single_node_idle(self):
        pm = PowerManager(_sim_config())
        result = pm.estimate_power_draw(1)
        # idle: 5W + 2W overhead = 7W
        assert result["idle_watts"] == pytest.approx(7.0)

    def test_single_node_load(self):
        pm = PowerManager(_sim_config())
        result = pm.estimate_power_draw(1)
        # load: 12W + 2W overhead = 14W
        assert result["load_watts"] == pytest.approx(14.0)

    def test_multi_node_scales_linearly(self):
        pm = PowerManager(_sim_config())
        one = pm.estimate_power_draw(1)
        four = pm.estimate_power_draw(4)
        assert four["idle_watts"] == pytest.approx(one["idle_watts"] * 4)
        assert four["load_watts"] == pytest.approx(one["load_watts"] * 4)

    def test_amps_derived_from_watts(self):
        pm = PowerManager(_sim_config())
        result = pm.estimate_power_draw(2)
        assert result["idle_amps_at_5v"] == pytest.approx(result["idle_watts"] / 5.0, rel=1e-3)
        assert result["load_amps_at_5v"] == pytest.approx(result["load_watts"] / 5.0, rel=1e-3)

    def test_node_count_reflected(self):
        pm = PowerManager(_sim_config())
        assert pm.estimate_power_draw(7)["node_count"] == 7


# ─────────────────────────────────────────────────────────────
# 3. test_schedule_shutdown_simulate
# ─────────────────────────────────────────────────────────────

class TestScheduleShutdownSimulate:
    """schedule_shutdown() records the action and returns a correct dict."""

    def test_returns_expected_keys(self):
        pm = PowerManager(_sim_config())
        result = pm.schedule_shutdown(delay_minutes=5, reason="maintenance")
        for key in ("action", "delay_minutes", "fires_at", "reason", "simulated"):
            assert key in result

    def test_action_is_shutdown(self):
        pm = PowerManager(_sim_config())
        result = pm.schedule_shutdown(delay_minutes=10)
        assert result["action"] == "shutdown"

    def test_simulated_flag_is_true(self):
        pm = PowerManager(_sim_config())
        result = pm.schedule_shutdown(delay_minutes=1)
        assert result["simulated"] is True

    def test_delay_minutes_reflected(self):
        pm = PowerManager(_sim_config())
        result = pm.schedule_shutdown(delay_minutes=30, reason="nightly")
        assert result["delay_minutes"] == 30

    def test_reason_reflected(self):
        pm = PowerManager(_sim_config())
        result = pm.schedule_shutdown(delay_minutes=5, reason="low_power")
        assert result["reason"] == "low_power"

    def test_fires_at_is_iso_string(self):
        pm = PowerManager(_sim_config())
        result = pm.schedule_shutdown(delay_minutes=5)
        # Should parse without raising.
        datetime.fromisoformat(result["fires_at"])

    def test_fires_at_is_in_the_future(self):
        pm = PowerManager(_sim_config())
        result = pm.schedule_shutdown(delay_minutes=5)
        fires = datetime.fromisoformat(result["fires_at"])
        assert fires > datetime.now()

    def test_internal_state_updated(self):
        pm = PowerManager(_sim_config())
        pm.schedule_shutdown(delay_minutes=5, reason="test")
        assert pm._scheduled_action == "shutdown"
        assert pm._scheduled_reason == "test"
        assert pm._scheduled_fire is not None

    def test_no_subprocess_in_simulate(self):
        """subprocess.run must never be called in simulate mode."""
        pm = PowerManager(_sim_config())
        with patch("subprocess.run") as mock_run:
            pm.schedule_shutdown(delay_minutes=1)
            mock_run.assert_not_called()


# ─────────────────────────────────────────────────────────────
# 4. test_schedule_reboot_simulate
# ─────────────────────────────────────────────────────────────

class TestScheduleRebootSimulate:
    """schedule_reboot() records the action and returns a correct dict."""

    def test_action_is_reboot(self):
        pm = PowerManager(_sim_config())
        result = pm.schedule_reboot(delay_minutes=2)
        assert result["action"] == "reboot"

    def test_simulated_flag_is_true(self):
        pm = PowerManager(_sim_config())
        assert pm.schedule_reboot(delay_minutes=1)["simulated"] is True

    def test_delay_and_reason_reflected(self):
        pm = PowerManager(_sim_config())
        result = pm.schedule_reboot(delay_minutes=15, reason="update")
        assert result["delay_minutes"] == 15
        assert result["reason"] == "update"

    def test_fires_at_in_future(self):
        pm = PowerManager(_sim_config())
        result = pm.schedule_reboot(delay_minutes=3)
        fires = datetime.fromisoformat(result["fires_at"])
        assert fires > datetime.now()

    def test_internal_state_updated(self):
        pm = PowerManager(_sim_config())
        pm.schedule_reboot(delay_minutes=2, reason="ota")
        assert pm._scheduled_action == "reboot"
        assert pm._scheduled_reason == "ota"

    def test_no_subprocess_in_simulate(self):
        pm = PowerManager(_sim_config())
        with patch("subprocess.run") as mock_run:
            pm.schedule_reboot(delay_minutes=1)
            mock_run.assert_not_called()

    def test_overwrites_previous_schedule(self):
        pm = PowerManager(_sim_config())
        pm.schedule_shutdown(delay_minutes=60)
        pm.schedule_reboot(delay_minutes=5, reason="override")
        assert pm._scheduled_action == "reboot"


# ─────────────────────────────────────────────────────────────
# 5. test_cancel_scheduled
# ─────────────────────────────────────────────────────────────

class TestCancelScheduled:
    """cancel_scheduled() clears pending actions and returns correct bool."""

    def test_returns_true_when_something_pending(self):
        pm = PowerManager(_sim_config())
        pm.schedule_shutdown(delay_minutes=10)
        assert pm.cancel_scheduled() is True

    def test_returns_false_when_nothing_pending(self):
        pm = PowerManager(_sim_config())
        assert pm.cancel_scheduled() is False

    def test_clears_internal_state(self):
        pm = PowerManager(_sim_config())
        pm.schedule_reboot(delay_minutes=5)
        pm.cancel_scheduled()
        assert pm._scheduled_action is None
        assert pm._scheduled_fire is None
        assert pm._scheduled_reason == ""

    def test_double_cancel_returns_false(self):
        pm = PowerManager(_sim_config())
        pm.schedule_shutdown(delay_minutes=5)
        pm.cancel_scheduled()
        assert pm.cancel_scheduled() is False

    def test_power_status_reflects_cancellation(self):
        pm = PowerManager(_sim_config())
        pm.schedule_reboot(delay_minutes=2)
        pm.cancel_scheduled()
        status = pm.power_status()
        assert status["scheduled_action"] is None
        assert status["scheduled_fires_at"] is None

    def test_no_subprocess_in_simulate(self):
        pm = PowerManager(_sim_config())
        pm.schedule_shutdown(delay_minutes=5)
        with patch("subprocess.run") as mock_run:
            pm.cancel_scheduled()
            mock_run.assert_not_called()


# ─────────────────────────────────────────────────────────────
# 6. test_power_status
# ─────────────────────────────────────────────────────────────

class TestPowerStatus:
    """power_status() returns a complete and correct snapshot."""

    def test_keys_present(self):
        pm = PowerManager(_sim_config())
        result = pm.power_status()
        for key in ("uptime", "scheduled_action", "scheduled_fires_at",
                    "scheduled_reason", "power_draw"):
            assert key in result

    def test_no_pending_action_on_fresh_instance(self):
        pm = PowerManager(_sim_config())
        status = pm.power_status()
        assert status["scheduled_action"] is None
        assert status["scheduled_fires_at"] is None
        assert status["scheduled_reason"] == ""

    def test_reflects_scheduled_shutdown(self):
        pm = PowerManager(_sim_config())
        pm.schedule_shutdown(delay_minutes=20, reason="eod")
        status = pm.power_status()
        assert status["scheduled_action"] == "shutdown"
        assert status["scheduled_reason"] == "eod"
        assert status["scheduled_fires_at"] is not None

    def test_reflects_scheduled_reboot(self):
        pm = PowerManager(_sim_config())
        pm.schedule_reboot(delay_minutes=10, reason="patch")
        status = pm.power_status()
        assert status["scheduled_action"] == "reboot"
        assert status["scheduled_reason"] == "patch"

    def test_uptime_nested_keys(self):
        pm = PowerManager(_sim_config())
        uptime = pm.power_status()["uptime"]
        assert "seconds" in uptime
        assert "human_readable" in uptime

    def test_power_draw_uses_node_count_from_config(self):
        pm = PowerManager({"simulate": True, "node_count": 5})
        draw = pm.power_status()["power_draw"]
        assert draw["node_count"] == 5

    def test_power_draw_nested_keys(self):
        pm = PowerManager(_sim_config())
        draw = pm.power_status()["power_draw"]
        assert "idle_watts" in draw
        assert "load_watts" in draw
