"""
test_scheduler.py — Tests for SyncScheduler and CronExpression.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from hzl_cluster.scheduler import CronExpression, SyncScheduler


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _make_config(schedule: str = "0 6 * * *", max_batch_size: int = 50) -> dict:
    return {
        "sync": {
            "schedule": schedule,
            "max_batch_size": max_batch_size,
        },
        "queue": {
            "queue_threshold": 25,
        },
    }


def _make_gateway(pending: int = 0):
    """Return a minimal mock GatewayDaemon."""
    gw = MagicMock()
    gw.queue.status.return_value = {"total_pending": pending}
    gw.run_sync_cycle = AsyncMock(return_value={
        "fetched": 1, "scanned": 1, "quarantined": 0, "delivered": 1
    })
    return gw


# ─────────────────────────────────────────────────────────────
# 1. test_parse_daily_schedule
# ─────────────────────────────────────────────────────────────

class TestParseDailySchedule:
    """CronExpression correctly handles a fixed daily schedule."""

    def test_fires_at_exact_time(self):
        expr = CronExpression("0 6 * * *")
        assert expr.should_fire(datetime(2026, 4, 6, 6, 0)) is True

    def test_does_not_fire_off_minute(self):
        expr = CronExpression("0 6 * * *")
        assert expr.should_fire(datetime(2026, 4, 6, 6, 1)) is False

    def test_does_not_fire_wrong_hour(self):
        expr = CronExpression("0 6 * * *")
        assert expr.should_fire(datetime(2026, 4, 6, 7, 0)) is False

    def test_fires_non_zero_minute(self):
        expr = CronExpression("30 14 * * *")
        assert expr.should_fire(datetime(2026, 4, 6, 14, 30)) is True
        assert expr.should_fire(datetime(2026, 4, 6, 14, 29)) is False

    def test_next_fire_time_advances_one_day_when_past(self):
        expr = CronExpression("0 6 * * *")
        # If current time is 06:01 the next fire should be tomorrow at 06:00.
        nxt = expr.next_fire_time(datetime(2026, 4, 6, 6, 1))
        assert nxt == datetime(2026, 4, 7, 6, 0)

    def test_next_fire_time_same_day_when_before(self):
        expr = CronExpression("0 6 * * *")
        nxt = expr.next_fire_time(datetime(2026, 4, 6, 5, 45))
        assert nxt == datetime(2026, 4, 6, 6, 0)


# ─────────────────────────────────────────────────────────────
# 2. test_parse_interval_schedule
# ─────────────────────────────────────────────────────────────

class TestParseIntervalSchedule:
    """CronExpression handles */N minute and */N hour patterns."""

    def test_every_30_minutes_fires_on_boundary(self):
        expr = CronExpression("*/30 * * * *")
        assert expr.should_fire(datetime(2026, 4, 6, 10, 0))  is True
        assert expr.should_fire(datetime(2026, 4, 6, 10, 30)) is True

    def test_every_30_minutes_does_not_fire_off_boundary(self):
        expr = CronExpression("*/30 * * * *")
        assert expr.should_fire(datetime(2026, 4, 6, 10, 15)) is False
        assert expr.should_fire(datetime(2026, 4, 6, 10, 29)) is False

    def test_every_15_minutes_fires_at_correct_intervals(self):
        expr = CronExpression("*/15 * * * *")
        for minute in (0, 15, 30, 45):
            assert expr.should_fire(datetime(2026, 4, 6, 9, minute)) is True
        for minute in (1, 14, 16, 31):
            assert expr.should_fire(datetime(2026, 4, 6, 9, minute)) is False

    def test_every_2_hours_fires_correctly(self):
        expr = CronExpression("0 */2 * * *")
        assert expr.should_fire(datetime(2026, 4, 6, 0, 0))  is True
        assert expr.should_fire(datetime(2026, 4, 6, 2, 0))  is True
        assert expr.should_fire(datetime(2026, 4, 6, 4, 0))  is True
        assert expr.should_fire(datetime(2026, 4, 6, 1, 0))  is False
        assert expr.should_fire(datetime(2026, 4, 6, 2, 1))  is False

    def test_next_fire_time_interval_minutes(self):
        expr = CronExpression("*/30 * * * *")
        # At 10:05 the next 30-min boundary is 10:30.
        nxt = expr.next_fire_time(datetime(2026, 4, 6, 10, 5))
        assert nxt == datetime(2026, 4, 6, 10, 30)

    def test_next_fire_time_on_boundary_advances(self):
        expr = CronExpression("*/30 * * * *")
        # Exactly on a boundary — next is 30 minutes later.
        nxt = expr.next_fire_time(datetime(2026, 4, 6, 10, 0))
        assert nxt == datetime(2026, 4, 6, 10, 30)

    def test_invalid_expression_raises(self):
        with pytest.raises(ValueError):
            CronExpression("not a cron")


# ─────────────────────────────────────────────────────────────
# 3. test_queue_threshold_triggers_sync
# ─────────────────────────────────────────────────────────────

class TestQueueThresholdTriggersSync:
    """should_sync_now() fires when pending queue depth >= threshold."""

    def setup_method(self):
        self.config = _make_config()   # threshold = 25

    def test_below_threshold_no_trigger(self):
        gw = _make_gateway(pending=24)
        scheduler = SyncScheduler(self.config, gw)
        # Use a time that won't match the cron ("0 6 * * *")
        fire, reason = scheduler.should_sync_now.__func__(
            scheduler
        ) if False else (False, "")
        # Drive manually with a safe time.
        scheduler.schedule._fixed_hour = 3   # point cron away from now
        scheduler.schedule._fixed_minute = 0
        scheduler.schedule._kind = "fixed"
        fire, reason = scheduler.should_sync_now()
        assert fire is False
        assert reason == ""

    def test_at_threshold_triggers(self):
        gw = _make_gateway(pending=25)
        scheduler = SyncScheduler(self.config, gw)
        # Deflect the cron so only the queue condition can fire.
        scheduler.schedule._kind = "fixed"
        scheduler.schedule._fixed_hour = 3
        scheduler.schedule._fixed_minute = 0
        fire, reason = scheduler.should_sync_now()
        assert fire is True
        assert "queue_threshold" in reason
        assert "25 pending" in reason

    def test_above_threshold_triggers(self):
        gw = _make_gateway(pending=100)
        scheduler = SyncScheduler(self.config, gw)
        scheduler.schedule._kind = "fixed"
        scheduler.schedule._fixed_hour = 3
        scheduler.schedule._fixed_minute = 0
        fire, reason = scheduler.should_sync_now()
        assert fire is True
        assert "queue_threshold" in reason

    def test_reason_includes_pending_count(self):
        gw = _make_gateway(pending=42)
        scheduler = SyncScheduler(self.config, gw)
        scheduler.schedule._kind = "fixed"
        scheduler.schedule._fixed_hour = 3
        scheduler.schedule._fixed_minute = 0
        fire, reason = scheduler.should_sync_now()
        assert "42 pending" in reason

    def test_scheduled_trigger_reason_includes_expression(self):
        """should_sync_now() fired by cron contains the cron expression."""
        gw = _make_gateway(pending=0)
        config = _make_config(schedule="0 6 * * *")
        scheduler = SyncScheduler(config, gw)
        # Simulate the scheduler being called exactly at 06:00 with no prior sync.
        scheduler._last_sync = None
        scheduler.schedule._kind = "fixed"
        scheduler.schedule._fixed_hour = datetime.now().hour
        scheduler.schedule._fixed_minute = datetime.now().minute
        fire, reason = scheduler.should_sync_now()
        assert fire is True
        assert "scheduled:" in reason

    def test_run_calls_sync_cycle(self):
        """run() calls gateway.run_sync_cycle() when threshold is met."""
        gw = _make_gateway(pending=50)
        config = _make_config()
        scheduler = SyncScheduler(config, gw)
        # Deflect cron so only queue fires.
        scheduler.schedule._kind = "fixed"
        scheduler.schedule._fixed_hour = 3
        scheduler.schedule._fixed_minute = 0

        async def _run_once():
            # Replace sleep so the loop exits immediately after one iteration.
            async def fake_sleep(_):
                scheduler._running = False

            original_sleep = asyncio.sleep
            asyncio.sleep = fake_sleep
            try:
                await scheduler.run()
            finally:
                asyncio.sleep = original_sleep

        asyncio.run(_run_once())
        gw.run_sync_cycle.assert_called_once()


# ─────────────────────────────────────────────────────────────
# 4. test_status_dict
# ─────────────────────────────────────────────────────────────

class TestStatusDict:
    """status() returns the expected keys and types."""

    def test_status_keys_present(self):
        gw = _make_gateway()
        scheduler = SyncScheduler(_make_config(), gw)
        s = scheduler.status()
        assert "last_sync"       in s
        assert "next_sync"       in s
        assert "schedule"        in s
        assert "queue_threshold" in s

    def test_last_sync_none_initially(self):
        gw = _make_gateway()
        scheduler = SyncScheduler(_make_config(), gw)
        assert scheduler.status()["last_sync"] is None

    def test_last_sync_updates_after_trigger(self):
        gw = _make_gateway(pending=100)
        config = _make_config()
        scheduler = SyncScheduler(config, gw)
        scheduler.schedule._kind = "fixed"
        scheduler.schedule._fixed_hour = 3
        scheduler.schedule._fixed_minute = 0

        # Manually simulate what run() does after a trigger.
        from datetime import datetime as dt
        scheduler._last_sync = dt(2026, 4, 6, 6, 0)
        s = scheduler.status()
        assert s["last_sync"] == "2026-04-06T06:00:00"

    def test_schedule_reflects_config(self):
        gw = _make_gateway()
        scheduler = SyncScheduler(_make_config(schedule="*/30 * * * *"), gw)
        assert scheduler.status()["schedule"] == "*/30 * * * *"

    def test_queue_threshold_reflects_config(self):
        gw = _make_gateway()
        scheduler = SyncScheduler(_make_config(), gw)
        assert scheduler.status()["queue_threshold"] == 25

    def test_next_sync_is_string(self):
        gw = _make_gateway()
        scheduler = SyncScheduler(_make_config(), gw)
        nxt = scheduler.status()["next_sync"]
        assert isinstance(nxt, str)
        # Should be parseable as a datetime string in YYYY-MM-DD HH:MM format.
        datetime.strptime(nxt, "%Y-%m-%d %H:%M")


# ─────────────────────────────────────────────────────────────
# 5. test_next_sync_time
# ─────────────────────────────────────────────────────────────

class TestNextSyncTime:
    """next_sync_time() returns a correct, human-readable string."""

    def test_format_is_datetime_string(self):
        gw = _make_gateway()
        scheduler = SyncScheduler(_make_config(schedule="0 6 * * *"), gw)
        result = scheduler.next_sync_time()
        # Validates both format and parseability.
        parsed = datetime.strptime(result, "%Y-%m-%d %H:%M")
        assert parsed.hour == 6
        assert parsed.minute == 0

    def test_interval_schedule_next_time_within_interval(self):
        gw = _make_gateway()
        scheduler = SyncScheduler(_make_config(schedule="*/30 * * * *"), gw)
        result = scheduler.next_sync_time()
        parsed = datetime.strptime(result, "%Y-%m-%d %H:%M")
        now = datetime.now().replace(second=0, microsecond=0)
        delta_minutes = (parsed - now).total_seconds() / 60
        # Next fire must be within the next 30-minute window.
        assert 0 < delta_minutes <= 30

    def test_hourly_schedule_next_time_within_interval(self):
        gw = _make_gateway()
        scheduler = SyncScheduler(_make_config(schedule="0 */2 * * *"), gw)
        result = scheduler.next_sync_time()
        parsed = datetime.strptime(result, "%Y-%m-%d %H:%M")
        now = datetime.now().replace(second=0, microsecond=0)
        delta_hours = (parsed - now).total_seconds() / 3600
        assert 0 < delta_hours <= 2

    def test_next_sync_is_in_the_future(self):
        gw = _make_gateway()
        for expr in ("0 6 * * *", "*/15 * * * *", "0 */3 * * *"):
            scheduler = SyncScheduler(_make_config(schedule=expr), gw)
            result = scheduler.next_sync_time()
            parsed = datetime.strptime(result, "%Y-%m-%d %H:%M")
            assert parsed > datetime.now().replace(second=0, microsecond=0)

    def test_next_sync_time_matches_status(self):
        """next_sync_time() and status()['next_sync'] agree."""
        gw = _make_gateway()
        scheduler = SyncScheduler(_make_config(schedule="0 6 * * *"), gw)
        assert scheduler.next_sync_time() == scheduler.status()["next_sync"]
