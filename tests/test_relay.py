"""
test_relay.py — Tests for RelayController.

All tests use simulate=True so no real GPIO or nmcli calls are made.
"""

import asyncio

import pytest

from hzl_cluster.relay import RelayController, RelayState


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_controller(**kwargs) -> RelayController:
    config = {"relay": {}}
    return RelayController(config, simulate=True, **kwargs)


def _run(coro):
    """Run a coroutine in a fresh event loop."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_initial_state_is_core_connected():
    ctrl = _make_controller()
    assert ctrl.state == RelayState.CORE_CONNECTED


def test_enter_internet_mode():
    ctrl = _make_controller()
    result = _run(ctrl.enter_internet_mode())
    assert result is True
    assert ctrl.state == RelayState.INTERNET_CONNECTED


def test_enter_core_mode():
    ctrl = _make_controller()
    _run(ctrl.enter_internet_mode())
    result = _run(ctrl.enter_core_mode())
    assert result is True
    assert ctrl.state == RelayState.CORE_CONNECTED


def test_lock_prevents_transitions():
    ctrl = _make_controller()
    ctrl.lock()
    result = _run(ctrl.enter_internet_mode())
    assert result is False
    assert ctrl.state == RelayState.LOCKED


def test_unlock_restores_previous_state():
    ctrl = _make_controller()
    # Lock while in CORE_CONNECTED
    ctrl.lock()
    assert ctrl.state == RelayState.LOCKED
    ctrl.unlock()
    assert ctrl.state == RelayState.CORE_CONNECTED


def test_emergency_disconnect():
    ctrl = _make_controller()
    _run(ctrl.enter_internet_mode())
    assert ctrl.state == RelayState.INTERNET_CONNECTED
    _run(ctrl.emergency_disconnect())
    assert ctrl.state == RelayState.CORE_CONNECTED


def test_cannot_enter_internet_from_internet():
    ctrl = _make_controller()
    _run(ctrl.enter_internet_mode())
    result = _run(ctrl.enter_internet_mode())
    assert result is False
    assert ctrl.state == RelayState.INTERNET_CONNECTED


def test_audit_log_records_events():
    ctrl = _make_controller()
    _run(ctrl.enter_internet_mode(reason="test"))
    _run(ctrl.enter_core_mode(reason="test"))

    log = ctrl.get_audit_log()
    events = [entry.split()[1] for entry in log]

    assert "RELAY_OPEN" in events
    assert "RELAY_CLOSE" in events


def test_get_state_dict():
    ctrl = _make_controller()
    d = ctrl.state_dict()
    assert "state" in d
    assert "last_sync" in d
    assert "uptime" in d
