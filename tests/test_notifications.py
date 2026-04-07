"""Tests for the notification manager (hzl_cluster/notifications.py)."""

import time

import pytest

from hzl_cluster.notifications import NotificationManager, PRIORITY_CRITICAL, PRIORITY_LOW


# ---------------------------------------------------------------------------
# 1. add() creates a well-formed notification
# ---------------------------------------------------------------------------

def test_add_notification():
    mgr = NotificationManager()
    n = mgr.add(
        title="Gateway sync failed",
        body="Could not reach pi-gateway after 3 attempts.",
        source="gateway",
        priority=PRIORITY_CRITICAL,
    )

    assert n["title"]    == "Gateway sync failed"
    assert n["body"]     == "Could not reach pi-gateway after 3 attempts."
    assert n["source"]   == "gateway"
    assert n["priority"] == PRIORITY_CRITICAL
    assert n["read"]     is False
    assert isinstance(n["id"], str) and len(n["id"]) > 0
    assert isinstance(n["timestamp"], float)


# ---------------------------------------------------------------------------
# 2. get_unread() returns only unread, newest first
# ---------------------------------------------------------------------------

def test_get_unread():
    mgr = NotificationManager()
    a = mgr.add("First",  "body a", "health")
    b = mgr.add("Second", "body b", "queue")
    c = mgr.add("Third",  "body c", "sync")

    # Mark the middle one read -- it should disappear from unread results.
    mgr.mark_read(b["id"])

    unread = mgr.get_unread()
    ids = [n["id"] for n in unread]

    assert b["id"] not in ids
    assert ids == [c["id"], a["id"]]   # newest first


# ---------------------------------------------------------------------------
# 3. mark_read() marks a single notification
# ---------------------------------------------------------------------------

def test_mark_read():
    mgr = NotificationManager()
    n = mgr.add("Node lost", "pi-2 went offline.", "health")

    assert n["read"] is False
    mgr.mark_read(n["id"])
    assert n["read"] is True

    # Unread list should now be empty.
    assert mgr.get_unread() == []


# ---------------------------------------------------------------------------
# 4. mark_all_read() marks every notification
# ---------------------------------------------------------------------------

def test_mark_all_read():
    mgr = NotificationManager()
    mgr.add("A", "body", "sync")
    mgr.add("B", "body", "sync")
    mgr.add("C", "body", "sync")

    assert len(mgr.get_unread()) == 3

    mgr.mark_all_read()
    assert mgr.get_unread() == []


# ---------------------------------------------------------------------------
# 5. summary() produces the expected human-readable string
# ---------------------------------------------------------------------------

def test_summary_format():
    mgr = NotificationManager()

    # Empty store.
    assert mgr.summary() == "You have no unread notifications."

    # One normal notification.
    mgr.add("Queue depth high", "Depth reached 50.", "queue")
    assert mgr.summary() == "You have 1 unread notification."

    # Add a critical -- summary must mention it.
    mgr.add("Gateway sync failed", "Could not reach pi-gateway.", "gateway", PRIORITY_CRITICAL)
    s = mgr.summary()
    assert s.startswith("You have 2 unread notifications.")
    assert "1 critical" in s
    assert "Gateway sync failed" in s

    # Mark all read.
    mgr.mark_all_read()
    assert mgr.summary() == "You have no unread notifications."


# ---------------------------------------------------------------------------
# 6. clear_old() removes notifications older than N hours
# ---------------------------------------------------------------------------

def test_clear_old():
    mgr = NotificationManager()

    old = mgr.add("Old alert", "This is stale.", "health")
    recent = mgr.add("Recent alert", "This is fresh.", "sync")

    # Backdate the old notification by 25 hours.
    old["timestamp"] = time.time() - 25 * 3600

    mgr.clear_old(hours=24)

    ids = [n["id"] for n in mgr._store]
    assert old["id"]    not in ids
    assert recent["id"] in ids


# ---------------------------------------------------------------------------
# 7. count() returns correct totals
# ---------------------------------------------------------------------------

def test_count():
    mgr = NotificationManager()

    mgr.add("A", "body", "sync", PRIORITY_CRITICAL)
    mgr.add("B", "body", "queue", PRIORITY_LOW)
    mgr.add("C", "body", "health")

    counts = mgr.count()
    assert counts["total"]    == 3
    assert counts["unread"]   == 3
    assert counts["critical"] == 1

    mgr.mark_all_read()
    counts = mgr.count()
    assert counts["unread"]   == 0
    assert counts["total"]    == 3   # total doesn't change on read
    assert counts["critical"] == 1   # critical is total, not just unread
