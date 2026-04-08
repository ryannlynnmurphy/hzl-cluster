"""
Cluster network diagnostics -- troubleshooting tools for when nodes can't communicate.

Covers HTTP reachability, DNS resolution, raw TCP port checks, and a full
diagnostic sweep that produces a human-readable fix list.
"""

from __future__ import annotations

import asyncio
import socket
import time
from typing import Optional

try:
    import aiohttp
    _AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover
    _AIOHTTP_AVAILABLE = False


class ClusterDiagnostics:
    """Network troubleshooting tools for a configured HZL cluster."""

    def __init__(self, config: dict):
        nodes_cfg = config.get("nodes", {})
        # Normalise nodes into a flat list of {"host": str, "port": int} dicts.
        self._nodes: list[dict] = []
        for name, node in nodes_cfg.items():
            host = node.get("ip") or name
            port = node.get("orchestrator_port") or node.get("port")
            if host and port:
                self._nodes.append({"host": host, "port": int(port), "name": name})

    # ------------------------------------------------------------------
    # HTTP reachability
    # ------------------------------------------------------------------

    async def ping_node(
        self,
        host: str,
        port: int,
        timeout: float = 2.0,
    ) -> dict:
        """HTTP GET /health on host:port.

        Returns:
            {
                "host": str,
                "port": int,
                "reachable": bool,
                "latency_ms": float | None,
                "error": str | None,
            }
        """
        url = f"http://{host}:{port}/health"
        start = time.monotonic()
        reachable = False
        latency_ms: Optional[float] = None
        error: Optional[str] = None

        if not _AIOHTTP_AVAILABLE:
            error = "aiohttp not installed"
            return {
                "host": host,
                "port": port,
                "reachable": False,
                "latency_ms": None,
                "error": error,
            }

        try:
            connector = aiohttp.TCPConnector()
            timeout_obj = aiohttp.ClientTimeout(total=timeout)
            async with aiohttp.ClientSession(
                connector=connector, timeout=timeout_obj
            ) as session:
                async with session.get(url) as resp:
                    reachable = resp.status < 500
                    latency_ms = round((time.monotonic() - start) * 1000, 2)
        except aiohttp.ClientConnectorError as exc:
            latency_ms = round((time.monotonic() - start) * 1000, 2)
            error = f"Connection refused: {exc}"
        except asyncio.TimeoutError:
            latency_ms = round((time.monotonic() - start) * 1000, 2)
            error = f"Timed out after {timeout}s"
        except Exception as exc:  # noqa: BLE001
            latency_ms = round((time.monotonic() - start) * 1000, 2)
            error = str(exc)

        return {
            "host": host,
            "port": port,
            "reachable": reachable,
            "latency_ms": latency_ms,
            "error": error,
        }

    async def ping_all_nodes(self) -> list[dict]:
        """Ping every node in the config concurrently.

        Returns a list of ping result dicts (one per configured node).
        """
        if not self._nodes:
            return []
        tasks = [
            self.ping_node(node["host"], node["port"])
            for node in self._nodes
        ]
        return list(await asyncio.gather(*tasks))

    # ------------------------------------------------------------------
    # DNS
    # ------------------------------------------------------------------

    def check_dns(self, hostname: str) -> dict:
        """Resolve *hostname* via the system DNS resolver.

        Returns:
            {
                "hostname": str,
                "resolved": bool,
                "ip": str | None,
                "error": str | None,
            }
        """
        try:
            ip = socket.gethostbyname(hostname)
            return {"hostname": hostname, "resolved": True, "ip": ip, "error": None}
        except socket.gaierror as exc:
            return {
                "hostname": hostname,
                "resolved": False,
                "ip": None,
                "error": str(exc),
            }

    # ------------------------------------------------------------------
    # Raw TCP port check
    # ------------------------------------------------------------------

    def check_port_open(
        self,
        host: str,
        port: int,
        timeout: float = 2.0,
    ) -> dict:
        """Attempt a TCP connection to host:port.

        Returns:
            {
                "host": str,
                "port": int,
                "open": bool,
                "latency_ms": float | None,
                "error": str | None,
            }
        """
        start = time.monotonic()
        try:
            with socket.create_connection((host, port), timeout=timeout):
                latency_ms = round((time.monotonic() - start) * 1000, 2)
                return {
                    "host": host,
                    "port": port,
                    "open": True,
                    "latency_ms": latency_ms,
                    "error": None,
                }
        except (ConnectionRefusedError, OSError) as exc:
            latency_ms = round((time.monotonic() - start) * 1000, 2)
            return {
                "host": host,
                "port": port,
                "open": False,
                "latency_ms": latency_ms,
                "error": str(exc),
            }

    # ------------------------------------------------------------------
    # Full diagnostic sweep
    # ------------------------------------------------------------------

    async def full_diagnostic(self) -> dict:
        """Run all checks against every configured node.

        Returns:
            {
                "nodes": list[dict],          # ping results
                "dns": list[dict],            # DNS checks per node hostname
                "ports": list[dict],          # TCP checks per node
                "reachable_count": int,
                "unreachable_count": int,
                "dns_failures": list[str],
                "closed_ports": list[str],
                "issues": list[str],          # plain-English issue summary
                "healthy": bool,
            }
        """
        node_pings = await self.ping_all_nodes()
        dns_results: list[dict] = []
        port_results: list[dict] = []

        for node in self._nodes:
            dns_results.append(self.check_dns(node["host"]))
            port_results.append(self.check_port_open(node["host"], node["port"]))

        reachable = [r for r in node_pings if r["reachable"]]
        unreachable = [r for r in node_pings if not r["reachable"]]
        dns_failures = [r["hostname"] for r in dns_results if not r["resolved"]]
        closed_ports = [
            f"{r['host']}:{r['port']}" for r in port_results if not r["open"]
        ]

        issues: list[str] = []

        for r in unreachable:
            issues.append(
                f"Node {r['host']}:{r['port']} is unreachable"
                + (f" -- {r['error']}" if r["error"] else "")
            )

        for hostname in dns_failures:
            issues.append(f"DNS resolution failed for {hostname}")

        for addr in closed_ports:
            issues.append(f"TCP port closed: {addr}")

        return {
            "nodes": node_pings,
            "dns": dns_results,
            "ports": port_results,
            "reachable_count": len(reachable),
            "unreachable_count": len(unreachable),
            "dns_failures": dns_failures,
            "closed_ports": closed_ports,
            "issues": issues,
            "healthy": len(issues) == 0,
        }

    # ------------------------------------------------------------------
    # Fix suggestions
    # ------------------------------------------------------------------

    def suggest_fixes(self, report: dict) -> list[str]:
        """Translate a diagnostic report into human-readable fix suggestions.

        Accepts the dict returned by :meth:`full_diagnostic`.
        """
        suggestions: list[str] = []

        dns_failures: list[str] = report.get("dns_failures", [])
        closed_ports: list[str] = report.get("closed_ports", [])
        nodes: list[dict] = report.get("nodes", [])

        unreachable_nodes = [n for n in nodes if not n.get("reachable")]

        if dns_failures:
            suggestions.append(
                "DNS failures detected for: "
                + ", ".join(dns_failures)
                + ". Check /etc/hosts or your local DNS server. "
                "On the Pi cluster, confirm each node hostname resolves "
                "or switch to direct IP addresses in hzl_config.yaml."
            )

        if closed_ports:
            suggestions.append(
                "Closed TCP ports: "
                + ", ".join(closed_ports)
                + ". Verify the node process is running (systemctl status hazel) "
                "and that no firewall (ufw/iptables) is blocking the port."
            )

        for node in unreachable_nodes:
            host = node.get("host", "unknown")
            port = node.get("port", "?")
            error = node.get("error") or ""
            base = f"Node {host}:{port} did not respond to /health."
            if "refused" in error.lower():
                suggestions.append(
                    f"{base} Connection was refused -- the service is likely "
                    "not running. SSH to the node and start it: "
                    "`sudo systemctl start hazel`."
                )
            elif "timed out" in error.lower() or "timeout" in error.lower():
                suggestions.append(
                    f"{base} Connection timed out -- the node may be powered "
                    "off, on a different subnet, or blocked by a firewall. "
                    "Ping it manually: `ping " + host + "`."
                )
            else:
                suggestions.append(
                    f"{base} Check that the node is powered on and connected "
                    "to the cluster network. Error: {error}"
                )

        if not suggestions:
            suggestions.append("All nodes are reachable. No fixes required.")

        return suggestions
