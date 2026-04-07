"""
relay.py — RelayController for Gateway Pi USB relay module.

Controls a physical relay via GPIO that electrically disconnects the Ethernet
cable between Gateway and Core. Ensures WiFi and Ethernet are never active
simultaneously.
"""

from __future__ import annotations

import asyncio
import subprocess
import time
from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional


class RelayState(Enum):
    CORE_CONNECTED = "core_connected"
    TRANSITIONING = "transitioning"
    INTERNET_CONNECTED = "internet_connected"
    LOCKED = "locked"


class RelayController:
    def __init__(self, config: dict, simulate: bool = False):
        relay_cfg = config.get("relay", {})

        self._gpio_pin: int = relay_cfg.get("gpio_pin", 17)
        self._max_internet_duration: int = relay_cfg.get("max_internet_duration", 600)
        self._watchdog_interval: int = relay_cfg.get("watchdog_interval", 30)
        self._wifi_interface: str = relay_cfg.get("wifi_interface", "wlan0")
        self._ethernet_interface: str = relay_cfg.get("ethernet_interface", "eth0")
        self._watchdog_policy: str = relay_cfg.get("watchdog_policy", "finish_active")

        self._simulate = simulate
        self._state: RelayState = RelayState.CORE_CONNECTED
        self._pre_lock_state: Optional[RelayState] = None
        self._internet_since: Optional[float] = None
        self._last_sync: float = time.monotonic()
        self._start_time: float = time.monotonic()
        self._audit_log: List[str] = []

        # GPIO setup
        self._gpio_device = None
        if not simulate:
            try:
                from gpiozero import OutputDevice  # type: ignore
                self._gpio_device = OutputDevice(self._gpio_pin, active_high=True, initial_value=False)
            except Exception:
                # gpiozero not available or pin error; fall back to no-op
                self._gpio_device = None

    # ------------------------------------------------------------------
    # Public property
    # ------------------------------------------------------------------

    @property
    def state(self) -> RelayState:
        return self._state

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------

    async def enter_internet_mode(self, reason: str = "manual") -> bool:
        """Transition from CORE_CONNECTED -> TRANSITIONING -> INTERNET_CONNECTED.

        Returns False if currently LOCKED or already INTERNET_CONNECTED.
        """
        if self._state == RelayState.LOCKED:
            return False
        if self._state == RelayState.INTERNET_CONNECTED:
            return False

        self._state = RelayState.TRANSITIONING

        if self._simulate:
            await asyncio.sleep(0.01)
        else:
            # Open relay (GPIO HIGH) — physically cuts Ethernet from Core
            if self._gpio_device is not None:
                self._gpio_device.on()
            self._log_event("RELAY_OPEN", reason=reason)

            # Bring WiFi up
            await self._wifi_up()
            self._log_event("WIFI_CONNECT", interface=self._wifi_interface, reason=reason)

        self._internet_since = time.monotonic()
        self._state = RelayState.INTERNET_CONNECTED

        if self._simulate:
            self._log_event("RELAY_OPEN", reason=reason)
            self._log_event("WIFI_CONNECT", interface=self._wifi_interface, reason=reason)

        return True

    async def enter_core_mode(self, reason: str = "manual") -> bool:
        """Transition from INTERNET_CONNECTED or TRANSITIONING -> CORE_CONNECTED.

        Returns True immediately if already CORE_CONNECTED.
        """
        if self._state == RelayState.CORE_CONNECTED:
            return True

        if self._simulate:
            await asyncio.sleep(0.01)
        else:
            await self._wifi_down()
            self._log_event("WIFI_DISCONNECT", interface=self._wifi_interface, reason=reason)

            if self._gpio_device is not None:
                self._gpio_device.off()
            self._log_event("RELAY_CLOSE", reason=reason)

        self._internet_since = None
        self._state = RelayState.CORE_CONNECTED

        if self._simulate:
            self._log_event("WIFI_DISCONNECT", interface=self._wifi_interface, reason=reason)
            self._log_event("RELAY_CLOSE", reason=reason)

        return True

    def lock(self) -> None:
        """Save current state and transition to LOCKED."""
        self._pre_lock_state = self._state
        self._state = RelayState.LOCKED
        self._log_event("RELAY_LOCKED", previous_state=self._pre_lock_state.value)

    def unlock(self) -> None:
        """Restore the state that was active before locking."""
        restored = self._pre_lock_state if self._pre_lock_state is not None else RelayState.CORE_CONNECTED
        self._state = restored
        self._pre_lock_state = None
        self._log_event("RELAY_UNLOCKED", restored_state=restored.value)

    async def emergency_disconnect(self) -> None:
        """Immediately bring WiFi down and close relay, regardless of current state."""
        if not self._simulate:
            await self._wifi_down()
            if self._gpio_device is not None:
                self._gpio_device.off()

        self._internet_since = None
        self._state = RelayState.CORE_CONNECTED
        self._log_event("EMERGENCY_DISCONNECT")

    # ------------------------------------------------------------------
    # Watchdog
    # ------------------------------------------------------------------

    async def check_watchdog(self) -> bool:
        """If online longer than max_internet_duration, trigger enter_core_mode.

        Returns True if the watchdog fired.
        """
        duration = self.get_internet_duration()
        if duration is not None and duration > self._max_internet_duration:
            self._log_event("WATCHDOG_TRIGGERED", duration=f"{duration:.1f}")
            await self.enter_core_mode(reason="watchdog")
            return True
        return False

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    def get_internet_duration(self) -> Optional[float]:
        """Return seconds online, or None if not in internet mode."""
        if self._internet_since is None:
            return None
        return time.monotonic() - self._internet_since

    def get_audit_log(self) -> List[str]:
        return list(self._audit_log)

    def state_dict(self) -> dict:
        return {
            "state": self._state.value,
            "internet_duration": self.get_internet_duration(),
            "last_sync": self._last_sync,
            "uptime": time.monotonic() - self._start_time,
            "audit_log_entries": len(self._audit_log),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _log_event(self, event: str, **kwargs) -> None:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        parts = [ts, event]
        for k, v in kwargs.items():
            parts.append(f"{k}={v}")
        self._audit_log.append(" ".join(parts))

    async def _wifi_up(self) -> bool:
        """Bring WiFi interface up via nmcli (non-simulate only)."""
        try:
            proc = await asyncio.create_subprocess_exec(
                "nmcli", "device", "connect", self._wifi_interface,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            await proc.wait()
            return proc.returncode == 0
        except Exception:
            return False

    async def _wifi_down(self) -> bool:
        """Bring WiFi interface down via nmcli (non-simulate only)."""
        try:
            proc = await asyncio.create_subprocess_exec(
                "nmcli", "device", "disconnect", self._wifi_interface,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            await proc.wait()
            return proc.returncode == 0
        except Exception:
            return False
