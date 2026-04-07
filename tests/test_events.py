"""Tests for the cluster event bus (hzl_cluster/events.py)."""

import asyncio
import pytest

from hzl_cluster.events import (
    EventBus,
    EVENT_NODE_JOINED,
    EVENT_NODE_LOST,
    EVENT_PHONE_DOCKED,
    EVENT_EMERGENCY,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run(coro):
    """Run a coroutine in a fresh event loop (pytest-asyncio not required)."""
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# 1. Register and emit
# ---------------------------------------------------------------------------

def test_register_and_emit():
    bus = EventBus()
    called = []

    def handler(**kwargs):
        called.append(kwargs)

    bus.on(EVENT_NODE_JOINED, handler)
    run(bus.emit(EVENT_NODE_JOINED, hostname="pi-1", role="worker", ip="10.0.0.2"))

    assert len(called) == 1


# ---------------------------------------------------------------------------
# 2. Multiple handlers for the same event
# ---------------------------------------------------------------------------

def test_multiple_handlers():
    bus = EventBus()
    log = []

    bus.on(EVENT_NODE_JOINED, lambda **kw: log.append("a"))
    bus.on(EVENT_NODE_JOINED, lambda **kw: log.append("b"))
    run(bus.emit(EVENT_NODE_JOINED, hostname="pi-2", role="worker", ip="10.0.0.3"))

    assert log == ["a", "b"]


# ---------------------------------------------------------------------------
# 3. Async handler
# ---------------------------------------------------------------------------

def test_async_handler():
    bus = EventBus()
    results = []

    async def async_handler(**kwargs):
        results.append(kwargs.get("hostname"))

    bus.on(EVENT_NODE_JOINED, async_handler)
    run(bus.emit(EVENT_NODE_JOINED, hostname="pi-async", role="worker", ip="10.0.0.9"))

    assert results == ["pi-async"]


# ---------------------------------------------------------------------------
# 4. kwargs are passed through to the handler
# ---------------------------------------------------------------------------

def test_kwargs_passed():
    bus = EventBus()
    received = {}

    def capture(**kwargs):
        received.update(kwargs)

    bus.on(EVENT_PHONE_DOCKED, capture)
    run(bus.emit(EVENT_PHONE_DOCKED, hostname="hazel-phone", ip="10.0.0.50"))

    assert received == {"hostname": "hazel-phone", "ip": "10.0.0.50"}


# ---------------------------------------------------------------------------
# 5. off() removes a handler
# ---------------------------------------------------------------------------

def test_off_removes_handler():
    bus = EventBus()
    called = []

    def handler(**kwargs):
        called.append(True)

    bus.on(EVENT_NODE_LOST, handler)
    bus.off(EVENT_NODE_LOST, handler)
    run(bus.emit(EVENT_NODE_LOST, hostname="pi-1", role="worker"))

    assert called == []


# ---------------------------------------------------------------------------
# 6. clear() removes all handlers
# ---------------------------------------------------------------------------

def test_clear_removes_all():
    bus = EventBus()
    called = []

    bus.on(EVENT_NODE_JOINED, lambda **kw: called.append("joined"))
    bus.on(EVENT_EMERGENCY,   lambda **kw: called.append("emergency"))
    bus.clear()

    run(bus.emit(EVENT_NODE_JOINED, hostname="x", role="y", ip="z"))
    run(bus.emit(EVENT_EMERGENCY, reason="fire"))

    assert called == []


# ---------------------------------------------------------------------------
# 7. Emitting an unregistered event type does not raise
# ---------------------------------------------------------------------------

def test_unknown_event_no_crash():
    bus = EventBus()
    # No handlers registered -- must not raise
    run(bus.emit("completely.unknown.event", foo="bar"))
