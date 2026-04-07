"""
scheduler.py — SyncScheduler for the Gateway Pi.

Runs as an asyncio background task alongside GatewayDaemon. Triggers sync
cycles on a cron-like schedule and/or when the pending queue depth exceeds
a configured threshold.

Supported cron patterns:
  "0 6 * * *"     — daily at a specific hour:minute
  "*/N * * * *"   — every N minutes
  "0 */N * * *"   — every N hours
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple

logger = logging.getLogger("hzl.scheduler")

# How often the background loop wakes to check conditions (seconds).
_POLL_INTERVAL = 60


# ─────────────────────────────────────────────────────────────
# Cron parser
# ─────────────────────────────────────────────────────────────

class CronExpression:
    """
    Minimal cron parser supporting three patterns:

      "0 6 * * *"     — fixed hour/minute each day
      "*/N * * * *"   — every N minutes
      "0 */N * * *"   — every N hours (at minute 0)
    """

    def __init__(self, expression: str) -> None:
        self.expression = expression
        parts = expression.strip().split()
        if len(parts) != 5:
            raise ValueError(
                f"Invalid cron expression (need 5 fields): {expression!r}"
            )

        self._minute_raw  = parts[0]
        self._hour_raw    = parts[1]
        self._day_raw     = parts[2]
        self._month_raw   = parts[3]
        self._weekday_raw = parts[4]

        # Detect pattern type.
        if self._minute_raw.startswith("*/"):
            # Every N minutes — "*/N * * * *"
            self._kind = "interval_minutes"
            self._interval = int(self._minute_raw[2:])
        elif self._hour_raw.startswith("*/") and self._minute_raw == "0":
            # Every N hours — "0 */N * * *"
            self._kind = "interval_hours"
            self._interval = int(self._hour_raw[2:])
        else:
            # Fixed time — "M H * * *"
            self._kind = "fixed"
            self._fixed_minute = int(self._minute_raw)
            self._fixed_hour   = int(self._hour_raw)

    # ------------------------------------------------------------------
    # should_fire
    # ------------------------------------------------------------------

    def should_fire(self, now: Optional[datetime] = None) -> bool:
        """
        Return True if the expression matches *now* (or the given datetime).

        For interval expressions the check is whether the current minute /
        hour is an exact multiple of the interval.  For fixed expressions
        the check is whether hour and minute both match exactly.
        """
        if now is None:
            now = datetime.now()

        if self._kind == "interval_minutes":
            return now.minute % self._interval == 0

        if self._kind == "interval_hours":
            return now.hour % self._interval == 0 and now.minute == 0

        # fixed
        return now.hour == self._fixed_hour and now.minute == self._fixed_minute

    # ------------------------------------------------------------------
    # next_fire_time
    # ------------------------------------------------------------------

    def next_fire_time(self, now: Optional[datetime] = None) -> datetime:
        """Return the next datetime when this expression will fire."""
        if now is None:
            now = datetime.now()

        # Truncate to the current minute (ignore seconds/microseconds).
        base = now.replace(second=0, microsecond=0)

        if self._kind == "interval_minutes":
            minutes_past = base.minute % self._interval
            if minutes_past == 0:
                # We're exactly on a boundary — next one is in _interval min.
                return base + timedelta(minutes=self._interval)
            return base + timedelta(minutes=self._interval - minutes_past)

        if self._kind == "interval_hours":
            hours_past = base.hour % self._interval
            if hours_past == 0 and base.minute == 0:
                return base + timedelta(hours=self._interval)
            if hours_past == 0:
                # In the right hour but past minute 0 — next is next cycle.
                return base.replace(minute=0) + timedelta(hours=self._interval)
            return base.replace(minute=0) + timedelta(
                hours=self._interval - hours_past
            )

        # fixed
        candidate = base.replace(
            hour=self._fixed_hour, minute=self._fixed_minute
        )
        if candidate <= base:
            candidate += timedelta(days=1)
        return candidate

    def __repr__(self) -> str:
        return f"CronExpression({self.expression!r})"


# ─────────────────────────────────────────────────────────────
# SyncScheduler
# ─────────────────────────────────────────────────────────────

class SyncScheduler:
    """
    Decides *when* to call gateway.run_sync_cycle() and drives the call.

    Two trigger conditions (checked every _POLL_INTERVAL seconds):
      1. Scheduled — the cron expression matches the current time.
      2. Queue threshold — pending queue depth >= queue_threshold.
    """

    def __init__(self, config: dict, gateway_daemon) -> None:
        sync_cfg  = config.get("sync", {})
        queue_cfg = config.get("queue", {})

        schedule_expr = sync_cfg.get("schedule", "0 6 * * *")
        self.schedule     = CronExpression(schedule_expr)
        self.max_batch_size: int = int(sync_cfg.get("max_batch_size", 50))

        # Default threshold: half of max_batch_size, minimum 10.
        default_threshold = max(10, self.max_batch_size // 2)
        self.queue_threshold: int = int(
            queue_cfg.get("queue_threshold", default_threshold)
        )

        self.gateway = gateway_daemon

        self._last_sync: Optional[datetime] = None
        self._running = False

    # ------------------------------------------------------------------
    # should_sync_now
    # ------------------------------------------------------------------

    def should_sync_now(self) -> Tuple[bool, str]:
        """
        Check whether a sync should fire right now.

        Returns:
            (True,  "scheduled: <expression>")       if cron fires
            (True,  "queue_threshold: <N> pending")  if queue is deep
            (False, "")                              otherwise
        """
        now = datetime.now()

        # 1. Cron check — but only fire once per minute window.
        if self.schedule.should_fire(now):
            # Guard against firing multiple times within the same minute.
            if (
                self._last_sync is None
                or (now - self._last_sync).total_seconds() >= 55
            ):
                return True, f"scheduled: {self.schedule.expression}"

        # 2. Queue depth check.
        try:
            pending = self.gateway.queue.status().get("total_pending", 0)
        except Exception:
            pending = 0

        if pending >= self.queue_threshold:
            return True, f"queue_threshold: {pending} pending"

        return False, ""

    # ------------------------------------------------------------------
    # next_sync_time
    # ------------------------------------------------------------------

    def next_sync_time(self) -> str:
        """Human-readable string for the next scheduled sync time."""
        nxt = self.schedule.next_fire_time()
        return nxt.strftime("%Y-%m-%d %H:%M")

    # ------------------------------------------------------------------
    # status
    # ------------------------------------------------------------------

    def status(self) -> dict:
        """Return scheduler state for monitoring / REST endpoints."""
        return {
            "last_sync":        self._last_sync.isoformat() if self._last_sync else None,
            "next_sync":        self.next_sync_time(),
            "schedule":         self.schedule.expression,
            "queue_threshold":  self.queue_threshold,
        }

    # ------------------------------------------------------------------
    # run  (asyncio background task)
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """
        Infinite loop.  Checks should_sync_now() every _POLL_INTERVAL
        seconds and calls gateway.run_sync_cycle() when triggered.
        """
        self._running = True
        logger.info(
            "[SyncScheduler] Started — schedule=%r  queue_threshold=%d",
            self.schedule.expression,
            self.queue_threshold,
        )

        while self._running:
            try:
                fire, reason = self.should_sync_now()
                if fire:
                    logger.info("[SyncScheduler] Triggering sync — %s", reason)
                    self._last_sync = datetime.now()
                    try:
                        result = await self.gateway.run_sync_cycle()
                        logger.info(
                            "[SyncScheduler] Sync complete — reason=%s result=%s",
                            reason,
                            result,
                        )
                    except Exception as exc:
                        logger.error(
                            "[SyncScheduler] Sync cycle error — %s", exc, exc_info=True
                        )
            except Exception as exc:
                logger.error(
                    "[SyncScheduler] Unexpected error in scheduler loop — %s",
                    exc,
                    exc_info=True,
                )

            await asyncio.sleep(_POLL_INTERVAL)
