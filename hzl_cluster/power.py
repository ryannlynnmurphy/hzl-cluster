"""
power.py — PowerManager for Hazel cluster nodes.

Manages power states: graceful shutdown, reboot, sleep scheduling.
Important for a cluster running 24/7 on limited Pi power budgets.

Pi 5 power estimates:
  ~5W  idle
  ~12W under load
  ~2W  with USB peripherals standby overhead
"""

from __future__ import annotations

import logging
import subprocess
import time
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger("hzl.power")

# Pi 5 spec-based constants (watts)
_PI5_IDLE_WATTS = 5.0
_PI5_LOAD_WATTS = 12.0
_PI5_OVERHEAD_WATTS = 2.0   # USB / peripheral standby per node


class PowerManager:
    """
    Manages power states for a cluster node (or the cluster as a whole).

    Pass ``simulate=True`` in config to run in dry-run mode — shutdown and
    reboot commands are logged but never executed. Useful for testing and
    development on non-Pi hardware.

    Config keys:
        simulate (bool):     If True, skip actual subprocess calls. Default False.
        node_count (int):    Number of nodes in the cluster. Default 1.
    """

    def __init__(self, config: dict) -> None:
        self._simulate: bool = bool(config.get("simulate", False))
        self._node_count: int = int(config.get("node_count", 1))

        # Pending scheduled action state.
        self._scheduled_action: Optional[str] = None   # "shutdown" | "reboot"
        self._scheduled_at: Optional[datetime] = None
        self._scheduled_fire: Optional[datetime] = None
        self._scheduled_reason: str = ""

    # ------------------------------------------------------------------
    # schedule_shutdown
    # ------------------------------------------------------------------

    def schedule_shutdown(self, delay_minutes: int, reason: str = "") -> dict:
        """
        Schedule a graceful shutdown after *delay_minutes*.

        In simulate mode the subprocess call is skipped and the schedule is
        recorded in memory only.

        Returns:
            {
                "action":      "shutdown",
                "delay_minutes": int,
                "fires_at":    ISO-8601 string,
                "reason":      str,
                "simulated":   bool,
            }
        """
        fires_at = datetime.now() + timedelta(minutes=delay_minutes)
        self._scheduled_action = "shutdown"
        self._scheduled_at = datetime.now()
        self._scheduled_fire = fires_at
        self._scheduled_reason = reason

        if not self._simulate:
            try:
                subprocess.run(
                    ["sudo", "shutdown", "-h", f"+{delay_minutes}"],
                    check=True,
                    capture_output=True,
                )
            except subprocess.CalledProcessError as exc:
                logger.error("[PowerManager] shutdown command failed: %s", exc)
                raise
        else:
            logger.info(
                "[PowerManager] SIMULATE shutdown in %d min — %s",
                delay_minutes,
                reason,
            )

        return {
            "action": "shutdown",
            "delay_minutes": delay_minutes,
            "fires_at": fires_at.isoformat(timespec="seconds"),
            "reason": reason,
            "simulated": self._simulate,
        }

    # ------------------------------------------------------------------
    # schedule_reboot
    # ------------------------------------------------------------------

    def schedule_reboot(self, delay_minutes: int, reason: str = "") -> dict:
        """
        Schedule a reboot after *delay_minutes*.

        Returns:
            {
                "action":      "reboot",
                "delay_minutes": int,
                "fires_at":    ISO-8601 string,
                "reason":      str,
                "simulated":   bool,
            }
        """
        fires_at = datetime.now() + timedelta(minutes=delay_minutes)
        self._scheduled_action = "reboot"
        self._scheduled_at = datetime.now()
        self._scheduled_fire = fires_at
        self._scheduled_reason = reason

        if not self._simulate:
            try:
                subprocess.run(
                    ["sudo", "shutdown", "-r", f"+{delay_minutes}"],
                    check=True,
                    capture_output=True,
                )
            except subprocess.CalledProcessError as exc:
                logger.error("[PowerManager] reboot command failed: %s", exc)
                raise
        else:
            logger.info(
                "[PowerManager] SIMULATE reboot in %d min — %s",
                delay_minutes,
                reason,
            )

        return {
            "action": "reboot",
            "delay_minutes": delay_minutes,
            "fires_at": fires_at.isoformat(timespec="seconds"),
            "reason": reason,
            "simulated": self._simulate,
        }

    # ------------------------------------------------------------------
    # cancel_scheduled
    # ------------------------------------------------------------------

    def cancel_scheduled(self) -> bool:
        """
        Cancel any pending shutdown or reboot.

        Returns True if there was something to cancel, False if the queue
        was already empty.
        """
        if self._scheduled_action is None:
            return False

        self._scheduled_action = None
        self._scheduled_at = None
        self._scheduled_fire = None
        self._scheduled_reason = ""

        if not self._simulate:
            try:
                subprocess.run(
                    ["sudo", "shutdown", "-c"],
                    check=True,
                    capture_output=True,
                )
            except subprocess.CalledProcessError as exc:
                logger.error("[PowerManager] cancel command failed: %s", exc)
                # Still return True — we cleared internal state.

        logger.info("[PowerManager] Scheduled action cancelled.")
        return True

    # ------------------------------------------------------------------
    # get_uptime
    # ------------------------------------------------------------------

    def get_uptime(self) -> dict:
        """
        Return system uptime.

        Returns:
            {
                "seconds":       float,
                "human_readable": str,   e.g. "2 days, 14 hours, 5 minutes"
            }
        """
        uptime_seconds = time.time() - self._boot_time()
        human = self._format_uptime(uptime_seconds)
        return {
            "seconds": uptime_seconds,
            "human_readable": human,
        }

    # ------------------------------------------------------------------
    # power_status
    # ------------------------------------------------------------------

    def power_status(self) -> dict:
        """
        Full power status snapshot.

        Returns:
            {
                "uptime":            dict (from get_uptime()),
                "scheduled_action":  str | None,
                "scheduled_fires_at": str | None,
                "scheduled_reason":  str,
                "power_draw":        dict (from estimate_power_draw()),
            }
        """
        uptime = self.get_uptime()
        power = self.estimate_power_draw(self._node_count)

        return {
            "uptime": uptime,
            "scheduled_action": self._scheduled_action,
            "scheduled_fires_at": (
                self._scheduled_fire.isoformat(timespec="seconds")
                if self._scheduled_fire else None
            ),
            "scheduled_reason": self._scheduled_reason,
            "power_draw": power,
        }

    # ------------------------------------------------------------------
    # estimate_power_draw
    # ------------------------------------------------------------------

    def estimate_power_draw(self, node_count: int) -> dict:
        """
        Estimate cluster power draw based on Pi 5 specs.

        Assumes:
          - idle: ~5W per node
          - load: ~12W per node
          - overhead: ~2W per node (USB / peripheral standby)

        Returns:
            {
                "node_count":       int,
                "idle_watts":       float,
                "load_watts":       float,
                "idle_amps_at_5v":  float,
                "load_amps_at_5v":  float,
                "note":             str,
            }
        """
        idle_total = (_PI5_IDLE_WATTS + _PI5_OVERHEAD_WATTS) * node_count
        load_total = (_PI5_LOAD_WATTS + _PI5_OVERHEAD_WATTS) * node_count

        return {
            "node_count": node_count,
            "idle_watts": round(idle_total, 2),
            "load_watts": round(load_total, 2),
            "idle_amps_at_5v": round(idle_total / 5.0, 3),
            "load_amps_at_5v": round(load_total / 5.0, 3),
            "note": "Based on Pi 5 specs: ~5W idle, ~12W load, ~2W overhead per node.",
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _boot_time(self) -> float:
        """Return system boot time as a Unix timestamp. Overrideable in tests."""
        try:
            import psutil
            return psutil.boot_time()
        except Exception:
            # Fallback: assume booted 0 seconds ago (safe for test environments).
            return time.time()

    @staticmethod
    def _format_uptime(seconds: float) -> str:
        """Convert a raw seconds value into a human-readable uptime string."""
        total = int(seconds)
        days, remainder = divmod(total, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, secs = divmod(remainder, 60)

        parts: list[str] = []
        if days:
            parts.append(f"{days} day{'s' if days != 1 else ''}")
        if hours:
            parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
        if minutes:
            parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
        if not parts:
            parts.append(f"{secs} second{'s' if secs != 1 else ''}")

        return ", ".join(parts)
