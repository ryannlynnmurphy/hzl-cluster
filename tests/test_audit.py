"""
test_audit.py — Tests for AuditReporter.

Builds synthetic relay logs to verify parsing, scoring, and summary generation
without touching the filesystem or any real cluster state.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pytest

from hzl_cluster.audit import AuditReporter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(dt: datetime) -> str:
    """Format a datetime as a relay log timestamp string."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _today_ts(hour: int = 6, minute: int = 0, second: int = 0) -> str:
    """Return a timestamp string for today (UTC) at the given time."""
    now = datetime.now(timezone.utc)
    dt = now.replace(hour=hour, minute=minute, second=second, microsecond=0)
    return _ts(dt)


def _make_relay_log(*lines: str) -> list[str]:
    return list(lines)


# ---------------------------------------------------------------------------
# 1. test_online_time_with_sessions
#    Two open/close pairs -- verify each session is captured and total is correct.
# ---------------------------------------------------------------------------

def test_online_time_with_sessions():
    now = datetime.now(timezone.utc)
    s1_open  = now.replace(hour=6,  minute=0,  second=0,  microsecond=0)
    s1_close = now.replace(hour=6,  minute=5,  second=12, microsecond=0)
    s2_open  = now.replace(hour=14, minute=30, second=0,  microsecond=0)
    s2_close = now.replace(hour=14, minute=32, second=45, microsecond=0)

    log = _make_relay_log(
        f"{_ts(s1_open)}  RELAY_OPEN  reason=scheduled_sync",
        f"{_ts(s1_close)} RELAY_CLOSE reason=sync_complete",
        f"{_ts(s2_open)}  RELAY_OPEN  reason=manual",
        f"{_ts(s2_close)} RELAY_CLOSE reason=manual",
    )

    reporter = AuditReporter(relay_log=log)
    result = reporter.online_time_today()

    assert len(result["sessions"]) == 2

    expected_s1 = (s1_close - s1_open).total_seconds()  # 312.0
    expected_s2 = (s2_close - s2_open).total_seconds()  # 165.0
    expected_total = expected_s1 + expected_s2

    assert result["total_seconds"] == pytest.approx(expected_total, abs=1.0)

    # Verify reasons are preserved
    reasons = {s["reason"] for s in result["sessions"]}
    assert "scheduled_sync" in reasons
    assert "manual" in reasons


# ---------------------------------------------------------------------------
# 2. test_online_time_empty_log
#    Empty log -- total_seconds must be 0 and sessions must be empty.
# ---------------------------------------------------------------------------

def test_online_time_empty_log():
    reporter = AuditReporter(relay_log=[])
    result = reporter.online_time_today()

    assert result["total_seconds"] == 0.0
    assert result["sessions"] == []


# ---------------------------------------------------------------------------
# 3. test_daily_summary_format
#    Verify the summary is a non-empty string containing key phrases.
# ---------------------------------------------------------------------------

def test_daily_summary_format():
    now = datetime.now(timezone.utc)
    open_ts  = now.replace(hour=6, minute=0, second=0,  microsecond=0)
    close_ts = now.replace(hour=6, minute=5, second=12, microsecond=0)

    sync_start = open_ts
    sync_end   = close_ts

    log = _make_relay_log(
        f"{_ts(open_ts)}   RELAY_OPEN  reason=scheduled_sync",
        f"{_ts(sync_start)} SYNC_START  reason=scheduled_sync",
        f"{_ts(sync_end)}   SYNC_END    items_fetched=12 items_quarantined=0",
        f"{_ts(close_ts)}  RELAY_CLOSE reason=sync_complete",
    )

    reporter = AuditReporter(relay_log=log)
    summary = reporter.daily_summary()

    assert isinstance(summary, str)
    assert len(summary) > 0
    # Must mention online time
    assert "online" in summary.lower()
    # Must mention quarantine status
    assert "quarantine" in summary.lower()
    # Must end with a security verdict
    assert summary.endswith("System secure.") or summary.endswith("Review quarantine log.")


# ---------------------------------------------------------------------------
# 4. test_security_score_perfect
#    A clean log with no online time, no quarantine events, no auth failures
#    should yield a score of 100.
# ---------------------------------------------------------------------------

def test_security_score_perfect():
    reporter = AuditReporter(relay_log=[])
    result = reporter.security_score()

    assert result["score"] == 100
    assert isinstance(result["factors"], list)
    assert len(result["factors"]) == 4

    factor_names = {f["name"] for f in result["factors"]}
    assert factor_names == {"time_online", "quarantined_files", "failed_auths", "queue_overflow"}


# ---------------------------------------------------------------------------
# 5. test_security_score_with_quarantine
#    Quarantined files must lower the score below 100.
# ---------------------------------------------------------------------------

def test_security_score_with_quarantine():
    now = datetime.now(timezone.utc)
    ts  = _ts(now.replace(hour=9, minute=0, second=5, microsecond=0))

    log = _make_relay_log(
        f"{ts} QUARANTINE file=malware.exe reason=blocked_extension",
        f"{ts} QUARANTINE file=virus2.exe  reason=pe_magic_bytes",
    )

    clean_reporter = AuditReporter(relay_log=[])
    dirty_reporter = AuditReporter(relay_log=log)

    clean_score = clean_reporter.security_score()["score"]
    dirty_score = dirty_reporter.security_score()["score"]

    assert dirty_score < clean_score
    assert dirty_score < 100


# ---------------------------------------------------------------------------
# 6. test_sync_history_parsing
#    SYNC_START / SYNC_END pairs within the window are extracted correctly.
# ---------------------------------------------------------------------------

def test_sync_history_parsing():
    now = datetime.now(timezone.utc)

    # Two syncs within the past 7 days
    s1_start = now - timedelta(days=1, hours=2)
    s1_end   = s1_start + timedelta(seconds=44)

    s2_start = now - timedelta(hours=3)
    s2_end   = s2_start + timedelta(seconds=120)

    # One sync older than 7 days (should be excluded)
    old_start = now - timedelta(days=8)
    old_end   = old_start + timedelta(seconds=30)

    log = _make_relay_log(
        f"{_ts(old_start)} SYNC_START  reason=scheduled_sync",
        f"{_ts(old_end)}   SYNC_END    items_fetched=5 items_quarantined=0",
        f"{_ts(s1_start)}  SYNC_START  reason=scheduled_sync",
        f"{_ts(s1_end)}    SYNC_END    items_fetched=12 items_quarantined=0",
        f"{_ts(s2_start)}  SYNC_START  reason=manual",
        f"{_ts(s2_end)}    SYNC_END    items_fetched=7 items_quarantined=1",
    )

    reporter = AuditReporter(relay_log=log)
    history = reporter.sync_history(days=7)

    assert len(history) == 2

    # Entries are in log order
    assert history[0]["items_fetched"] == 12
    assert history[0]["items_quarantined"] == 0
    assert history[0]["reason"] == "scheduled_sync"

    assert history[1]["items_fetched"] == 7
    assert history[1]["items_quarantined"] == 1
    assert history[1]["reason"] == "manual"

    # Duration sanity check
    assert abs(history[0]["duration"] - 44.0) < 2.0
    assert abs(history[1]["duration"] - 120.0) < 2.0
