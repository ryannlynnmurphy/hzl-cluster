"""
Cluster health monitoring -- deep checks for node and system health.
Goes beyond the basic /health liveness probe to check disk, memory,
temperature, connectivity, and service status.
"""

from __future__ import annotations

import os
import time
from typing import Optional

import psutil

try:
    import aiohttp
    _AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover
    _AIOHTTP_AVAILABLE = False

# Thresholds used when none are supplied via config
_DEFAULT_DISK_THRESHOLD = 90.0    # percent
_DEFAULT_MEMORY_THRESHOLD = 90.0  # percent
_DEFAULT_TEMP_THRESHOLD = 80.0    # celsius
_DEFAULT_LOAD_MULTIPLIER = 2.0    # load1 > cpu_count * multiplier = unhealthy

_THERMAL_PATH = "/sys/class/thermal/thermal_zone0/temp"


class HealthMonitor:
    """Deep health checks for a single cluster node."""

    def __init__(self, config: dict):
        thresholds = config.get("thresholds", {})
        self._disk_threshold: float = float(
            thresholds.get("disk_percent", _DEFAULT_DISK_THRESHOLD)
        )
        self._memory_threshold: float = float(
            thresholds.get("memory_percent", _DEFAULT_MEMORY_THRESHOLD)
        )
        self._temp_threshold: float = float(
            thresholds.get("cpu_temp_celsius", _DEFAULT_TEMP_THRESHOLD)
        )
        self._load_multiplier: float = float(
            thresholds.get("load_multiplier", _DEFAULT_LOAD_MULTIPLIER)
        )

    # ------------------------------------------------------------------
    # Local checks
    # ------------------------------------------------------------------

    def check_disk(self, path: str = "/") -> dict:
        """Return disk usage stats for *path*.

        Returns:
            {
                "path": str,
                "total_gb": float,
                "used_gb": float,
                "free_gb": float,
                "percent": float,
                "healthy": bool,
            }
        Unhealthy when used percent exceeds the configured threshold (default 90%).
        """
        usage = psutil.disk_usage(path)
        _gb = 1024 ** 3
        percent = usage.percent
        return {
            "path": path,
            "total_gb": round(usage.total / _gb, 2),
            "used_gb": round(usage.used / _gb, 2),
            "free_gb": round(usage.free / _gb, 2),
            "percent": percent,
            "healthy": percent < self._disk_threshold,
        }

    def check_memory(self) -> dict:
        """Return virtual memory stats.

        Returns:
            {
                "total_mb": float,
                "used_mb": float,
                "available_mb": float,
                "percent": float,
                "healthy": bool,
            }
        Unhealthy when used percent exceeds the configured threshold (default 90%).
        """
        vm = psutil.virtual_memory()
        _mb = 1024 ** 2
        return {
            "total_mb": round(vm.total / _mb, 2),
            "used_mb": round(vm.used / _mb, 2),
            "available_mb": round(vm.available / _mb, 2),
            "percent": vm.percent,
            "healthy": vm.percent < self._memory_threshold,
        }

    def check_cpu_temperature(self) -> Optional[dict]:
        """Read CPU temperature from the Pi thermal sysfs interface.

        Returns None on non-Pi systems or when the thermal node is absent.

        Returns:
            {"celsius": float, "healthy": bool}  or  None
        """
        if not os.path.exists(_THERMAL_PATH):
            return None
        try:
            with open(_THERMAL_PATH, "r") as fh:
                raw = fh.read().strip()
            celsius = int(raw) / 1000.0
        except (OSError, ValueError):
            return None
        return {
            "celsius": round(celsius, 1),
            "healthy": celsius < self._temp_threshold,
        }

    def check_load(self) -> dict:
        """Return system load averages.

        Returns:
            {
                "load1": float,
                "load5": float,
                "load15": float,
                "cpu_count": int,
                "healthy": bool,
            }
        Unhealthy when load1 exceeds cpu_count * load_multiplier (default 2x).
        """
        load1, load5, load15 = psutil.getloadavg()
        cpu_count = psutil.cpu_count(logical=True) or 1
        return {
            "load1": round(load1, 2),
            "load5": round(load5, 2),
            "load15": round(load15, 2),
            "cpu_count": cpu_count,
            "healthy": load1 <= cpu_count * self._load_multiplier,
        }

    # ------------------------------------------------------------------
    # Network / connectivity checks
    # ------------------------------------------------------------------

    async def check_node_connectivity(
        self,
        host: str,
        port: int,
        timeout: float = 2.0,
    ) -> dict:
        """Attempt an HTTP GET to http://host:port/health.

        Returns:
            {
                "host": str,
                "port": int,
                "reachable": bool,
                "latency_ms": float | None,
            }
        """
        url = f"http://{host}:{port}/health"
        start = time.monotonic()
        latency_ms: Optional[float] = None
        reachable = False

        if _AIOHTTP_AVAILABLE:
            try:
                connector = aiohttp.TCPConnector()
                timeout_obj = aiohttp.ClientTimeout(total=timeout)
                async with aiohttp.ClientSession(
                    connector=connector, timeout=timeout_obj
                ) as session:
                    async with session.get(url) as resp:
                        if resp.status < 500:
                            reachable = True
                        latency_ms = round((time.monotonic() - start) * 1000, 2)
            except Exception:
                latency_ms = round((time.monotonic() - start) * 1000, 2)

        return {
            "host": host,
            "port": port,
            "reachable": reachable,
            "latency_ms": latency_ms,
        }

    async def check_all_nodes(self, nodes_config: list[dict]) -> list[dict]:
        """Check connectivity to every node in *nodes_config*.

        Each entry in nodes_config should have at least "host" and "port" keys.
        An optional "timeout" key overrides the default per-node.

        Returns a list of connectivity result dicts.
        """
        import asyncio

        tasks = [
            self.check_node_connectivity(
                host=node["host"],
                port=int(node["port"]),
                timeout=float(node.get("timeout", 2.0)),
            )
            for node in nodes_config
        ]
        return list(await asyncio.gather(*tasks))

    # ------------------------------------------------------------------
    # Aggregated report
    # ------------------------------------------------------------------

    def full_report(self) -> dict:
        """Run all local checks and return a combined report.

        Returns:
            {
                "disk": dict,
                "memory": dict,
                "load": dict,
                "temperature": dict | None,
                "healthy": bool,
            }
        The top-level "healthy" flag is True only when every available
        sub-check is also healthy.
        """
        disk = self.check_disk()
        memory = self.check_memory()
        load = self.check_load()
        temperature = self.check_cpu_temperature()

        sub_checks = [disk["healthy"], memory["healthy"], load["healthy"]]
        if temperature is not None:
            sub_checks.append(temperature["healthy"])

        return {
            "disk": disk,
            "memory": memory,
            "load": load,
            "temperature": temperature,
            "healthy": all(sub_checks),
        }

    def alerts(self) -> list[str]:
        """Return human-readable alert strings for every unhealthy condition.

        Returns an empty list when the node is fully healthy.
        """
        report = self.full_report()
        messages: list[str] = []

        disk = report["disk"]
        if not disk["healthy"]:
            messages.append(
                f"Disk {disk['path']} is {disk['percent']:.1f}% used "
                f"(threshold {self._disk_threshold:.0f}%): "
                f"{disk['free_gb']:.2f} GB free of {disk['total_gb']:.2f} GB"
            )

        memory = report["memory"]
        if not memory["healthy"]:
            messages.append(
                f"Memory pressure: {memory['percent']:.1f}% used "
                f"(threshold {self._memory_threshold:.0f}%): "
                f"{memory['available_mb']:.0f} MB available of {memory['total_mb']:.0f} MB"
            )

        load = report["load"]
        if not load["healthy"]:
            messages.append(
                f"High CPU load: load1={load['load1']} exceeds "
                f"{load['cpu_count']} CPUs * {self._load_multiplier} "
                f"= {load['cpu_count'] * self._load_multiplier}"
            )

        temp = report["temperature"]
        if temp is not None and not temp["healthy"]:
            messages.append(
                f"CPU temperature critical: {temp['celsius']}C "
                f"(threshold {self._temp_threshold:.0f}C)"
            )

        return messages
