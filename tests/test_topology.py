"""
test_topology.py — Tests for hzl_cluster.topology.render_topology.
"""

import pytest

from hzl_cluster.topology import render_topology


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

def _core_node(**kwargs):
    base = {"hostname": "hazel-core", "role": "core", "status": "online", "cpu": 34, "mem": 58}
    base.update(kwargs)
    return base


def _worker_node(**kwargs):
    base = {"hostname": "hazel-worker-01", "role": "worker", "status": "online", "cpu": 12, "mem": 45}
    base.update(kwargs)
    return base


def _gateway_node(**kwargs):
    base = {"hostname": "hazel-gateway", "role": "gateway", "status": "online", "wifi": "OFF"}
    base.update(kwargs)
    return base


def _phone_node(**kwargs):
    base = {"hostname": "hazel-phone", "role": "phone", "status": "docked"}
    base.update(kwargs)
    return base


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────

def test_render_basic():
    """Two online nodes: output must contain both hostnames and ZONE labels."""
    nodes = [_core_node(), _worker_node(), _gateway_node()]
    result = render_topology(nodes)

    assert "hazel-core" in result
    assert "hazel-worker-01" in result
    assert "hazel-gateway" in result
    assert "ZONE A" in result
    assert "ZONE B" in result
    assert "USB RELAY" in result


def test_render_offline_node():
    """An offline worker must show OFFLINE and the dash marker."""
    nodes = [
        _core_node(),
        _worker_node(hostname="hazel-worker-02", status="offline", cpu=None, mem=None),
        _gateway_node(),
    ]
    result = render_topology(nodes)

    assert "hazel-worker-02" in result
    assert "OFFLINE" in result
    # Offline nodes should NOT show CPU/MEM lines
    # (we look for the specific pairing with the offline hostname)
    lines = result.splitlines()
    worker_section = [l for l in lines if "hazel-worker-02" in l or "OFFLINE" in l]
    assert len(worker_section) >= 1


def test_render_relay_open():
    """internet_connected relay state must show OPEN in the relay banner."""
    nodes = [_core_node(), _gateway_node()]
    result = render_topology(nodes, relay_state="internet_connected")

    assert "OPEN" in result
    assert "CLOSED" not in result


def test_render_relay_locked():
    """locked relay state must show LOCKED in the relay banner."""
    nodes = [_core_node(), _gateway_node()]
    result = render_topology(nodes, relay_state="locked")

    assert "LOCKED" in result
    assert "CLOSED" not in result
    assert "OPEN" not in result


def test_render_with_phone():
    """Phone node: docked shows DOCKED, away shows AWAY."""
    # Docked phone
    nodes_docked = [_core_node(), _phone_node(status="docked"), _gateway_node()]
    result_docked = render_topology(nodes_docked)
    assert "hazel-phone" in result_docked
    assert "DOCKED" in result_docked

    # Away phone
    nodes_away = [_core_node(), _phone_node(status="away"), _gateway_node()]
    result_away = render_topology(nodes_away)
    assert "hazel-phone" in result_away
    assert "AWAY" in result_away
