"""
Tests for hzl_cluster.config_validator
"""

import pytest
from hzl_cluster.config_validator import validate_config


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _minimal_config():
    """Return a minimal config that passes all checks."""
    return {
        "cluster": {
            "name": "HZL-test",
            "core_node": "hazel-core",
        },
        "nodes": {
            "hazel-core": {
                "role": "core",
                "capabilities": ["orchestrator", "voice"],
            },
            "hazel-worker-01": {
                "role": "worker",
                "capabilities": ["inference"],
            },
        },
        "routing": {
            "task_map": {
                "reasoning": {
                    "model": "claude-sonnet-4-6",
                    "preferred_node": "any_worker",
                    "max_tokens": 2000,
                    "timeout": 30,
                },
            },
        },
        "network": {
            "discovery_port": 9099,
            "heartbeat_interval": 5,
        },
    }


def _has_error_matching(errors, fragment):
    """Return True if any error string contains *fragment* (case-insensitive)."""
    fragment_lower = fragment.lower()
    return any(fragment_lower in e.lower() for e in errors)


# ─────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────

def test_valid_config():
    """Minimal valid config returns no errors."""
    errors = validate_config(_minimal_config())
    assert errors == [], "Expected no errors, got: {}".format(errors)


def test_missing_cluster():
    """Missing 'cluster' key returns an error mentioning 'cluster'."""
    cfg = _minimal_config()
    del cfg["cluster"]
    errors = validate_config(cfg)
    assert _has_error_matching(errors, "cluster"), (
        "Expected an error about 'cluster', got: {}".format(errors)
    )


def test_missing_nodes():
    """Missing 'nodes' key returns an error mentioning 'nodes'."""
    cfg = _minimal_config()
    del cfg["nodes"]
    errors = validate_config(cfg)
    assert _has_error_matching(errors, "nodes"), (
        "Expected an error about 'nodes', got: {}".format(errors)
    )


def test_invalid_node_role():
    """A node with role 'banana' returns an error about the invalid role."""
    cfg = _minimal_config()
    cfg["nodes"]["hazel-worker-01"]["role"] = "banana"
    errors = validate_config(cfg)
    assert _has_error_matching(errors, "banana") or _has_error_matching(errors, "invalid role"), (
        "Expected an error about invalid role 'banana', got: {}".format(errors)
    )


def test_core_node_not_in_nodes():
    """core_node referencing a nonexistent node name returns an error."""
    cfg = _minimal_config()
    cfg["cluster"]["core_node"] = "ghost-node"
    errors = validate_config(cfg)
    assert _has_error_matching(errors, "ghost-node"), (
        "Expected an error mentioning 'ghost-node', got: {}".format(errors)
    )


def test_invalid_discovery_port():
    """discovery_port of 80 (below 1024) returns a port-range error."""
    cfg = _minimal_config()
    cfg["network"]["discovery_port"] = 80
    errors = validate_config(cfg)
    assert _has_error_matching(errors, "discovery_port") or _has_error_matching(errors, "out of range"), (
        "Expected a port range error, got: {}".format(errors)
    )


def test_valid_gateway_config():
    """Gateway node with a relay section validates without errors."""
    cfg = _minimal_config()
    cfg["nodes"]["hazel-gateway"] = {
        "role": "gateway",
        "capabilities": ["gateway", "fetch", "relay_control"],
    }
    cfg["routing"]["task_map"]["gateway_fetch"] = {
        "model": None,
        "preferred_node": "hazel-gateway",
        "max_tokens": 0,
        "timeout": 5,
    }
    cfg["relay"] = {
        "gpio_pin": 17,
        "max_internet_duration": 600,
    }
    errors = validate_config(cfg)
    assert errors == [], "Expected no errors for gateway config, got: {}".format(errors)


def test_preferred_node_references_valid():
    """task_map entry referencing an unknown node name returns an error."""
    cfg = _minimal_config()
    cfg["routing"]["task_map"]["bad_task"] = {
        "model": "claude-haiku",
        "preferred_node": "nonexistent-node",
        "max_tokens": 500,
        "timeout": 10,
    }
    errors = validate_config(cfg)
    assert _has_error_matching(errors, "nonexistent-node"), (
        "Expected an error about 'nonexistent-node', got: {}".format(errors)
    )
