#!/usr/bin/env python3
"""
hazel-dashboard — Real-time cluster status in the terminal.

Usage:
    python -m hzl_cluster.dashboard
    python -m hzl_cluster.dashboard --host 192.168.10.1 --port 9000
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from urllib.request import urlopen
from urllib.error import URLError


class C:
    """ANSI color codes."""
    GOLD    = "\033[33m"
    GREEN   = "\033[32m"
    RED     = "\033[31m"
    CYAN    = "\033[36m"
    WHITE   = "\033[97m"
    DIM     = "\033[2m"
    BOLD    = "\033[1m"
    RESET   = "\033[0m"
    BG_DARK = "\033[48;5;235m"


def clear():
    os.system("cls" if os.name == "nt" else "clear")


def bar(value: float, width: int = 20, warn: float = 70, crit: float = 85) -> str:
    """Render a percentage bar with color."""
    filled = int(value / 100 * width)
    empty = width - filled
    if value >= crit:
        color = C.RED
    elif value >= warn:
        color = C.GOLD
    else:
        color = C.GREEN
    return f"{color}{'█' * filled}{'░' * empty}{C.RESET} {value:5.1f}%"


def fetch_json(url: str, timeout: float = 2.0):
    """Fetch JSON from URL, return None on failure."""
    try:
        with urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except (URLError, OSError, json.JSONDecodeError):
        return None


def render_header() -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"""
{C.GOLD}{C.BOLD}  ╔═══════════════════════════════════════════════════════════╗
  ║              H A Z E L   O S   C L U S T E R            ║
  ║                  Air-Gapped Dashboard                    ║
  ╚═══════════════════════════════════════════════════════════╝{C.RESET}
  {C.DIM}{now}{C.RESET}
"""


def render_node(node: dict) -> str:
    """Render a single node status block."""
    hostname = node.get("hostname", "unknown")
    role = node.get("role", "?")
    alive = node.get("alive", False)
    healthy = node.get("healthy", False)
    cpu = node.get("cpu", 0)
    mem = node.get("memory", 0)
    caps = node.get("capabilities", [])
    cb = node.get("circuit_breaker", {})
    cb_state = cb.get("state", "closed")

    # Status indicator
    if not alive:
        status = f"{C.RED}● OFFLINE{C.RESET}"
    elif not healthy:
        status = f"{C.GOLD}● DEGRADED{C.RESET}"
    else:
        status = f"{C.GREEN}● ONLINE{C.RESET}"

    # Circuit breaker
    if cb_state == "open":
        cb_display = f"{C.RED}OPEN{C.RESET}"
    elif cb_state == "half-open":
        cb_display = f"{C.GOLD}HALF-OPEN{C.RESET}"
    else:
        cb_display = f"{C.GREEN}CLOSED{C.RESET}"

    # Role badge
    role_colors = {"core": C.GOLD, "worker": C.CYAN, "gateway": C.GREEN, "mobile": C.WHITE}
    role_color = role_colors.get(role, C.DIM)

    lines = [
        f"  {C.BOLD}{hostname}{C.RESET}  {role_color}[{role}]{C.RESET}  {status}",
        f"    CPU: {bar(cpu)}",
        f"    MEM: {bar(mem)}",
        f"    CB:  {cb_display}   Caps: {C.DIM}{', '.join(caps[:4])}{C.RESET}",
    ]
    return "\n".join(lines)


def render_queue(queue_data: dict) -> str:
    """Render queue status."""
    if not queue_data:
        return f"  {C.DIM}Queue: not available{C.RESET}"

    total = queue_data.get("total_pending", 0)
    by_dest = queue_data.get("by_destination", {})

    lines = [f"  {C.BOLD}Message Queue{C.RESET}  {C.DIM}({total} pending){C.RESET}"]
    if by_dest:
        for dest, count in by_dest.items():
            lines.append(f"    → {dest}: {C.CYAN}{count}{C.RESET}")
    elif total == 0:
        lines.append(f"    {C.DIM}Empty — all messages delivered{C.RESET}")

    return "\n".join(lines)


def render_relay(gateway_data: dict) -> str:
    """Render relay/gateway status."""
    if not gateway_data:
        return f"  {C.DIM}Gateway: not available{C.RESET}"

    relay = gateway_data.get("relay", {})
    state = relay.get("state", "unknown")
    duration = relay.get("internet_duration")
    last_sync = relay.get("last_sync")

    state_colors = {
        "core_connected": C.GREEN,
        "internet_connected": C.GOLD,
        "transitioning": C.CYAN,
        "locked": C.RED,
    }
    color = state_colors.get(state, C.DIM)

    lines = [f"  {C.BOLD}Air-Gap Relay{C.RESET}  {color}● {state.upper()}{C.RESET}"]

    if state == "internet_connected" and duration is not None:
        lines.append(f"    Online for: {C.GOLD}{duration:.0f}s{C.RESET}")
    if last_sync:
        sync_time = datetime.fromtimestamp(last_sync).strftime("%H:%M:%S")
        lines.append(f"    Last sync: {sync_time}")

    staging = gateway_data.get("staging", [])
    if staging:
        lines.append(f"    Staging: {len(staging)} files")

    return "\n".join(lines)


def render_metrics(metrics: dict) -> str:
    """Render task routing metrics."""
    if not metrics:
        return ""

    lines = [f"  {C.BOLD}Routing Metrics{C.RESET}"]
    for task_type, data in metrics.items():
        reqs = data.get("requests", 0)
        if reqs == 0:
            continue
        local = data.get("local_hits", 0)
        cloud = data.get("cloud_fallbacks", 0)
        latency = data.get("latency_ms", {})
        p50 = latency.get("p50")
        p50_str = f"{p50:.0f}ms" if p50 else "—"
        lines.append(
            f"    {task_type:20s}  "
            f"reqs={C.CYAN}{reqs}{C.RESET}  "
            f"local={C.GREEN}{local}{C.RESET}  "
            f"cloud={C.RED}{cloud}{C.RESET}  "
            f"p50={p50_str}"
        )

    return "\n".join(lines) if len(lines) > 1 else ""


def render_footer() -> str:
    return f"\n  {C.DIM}Press Ctrl+C to exit  |  Refreshes every 2s  |  hazel-dashboard v1.0{C.RESET}\n"


def run(host: str, port: int, gateway_host: str = None, gateway_port: int = 9010) -> None:
    """Main dashboard loop."""
    base_url = f"http://{host}:{port}"
    gw_url = f"http://{gateway_host or host}:{gateway_port}" if gateway_host else None

    # Fix encoding for Windows
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except AttributeError:
            pass

    try:
        while True:
            clear()

            # Fetch data
            status = fetch_json(f"{base_url}/status")
            queue = fetch_json(f"{base_url}/queue")
            gateway = fetch_json(f"{gw_url}/state") if gw_url else None

            # Render
            output = [render_header()]

            if status:
                nodes = status.get("nodes", [])
                output.append(f"  {C.BOLD}Nodes{C.RESET}  {C.DIM}({len(nodes)} total){C.RESET}\n")
                for node in nodes:
                    output.append(render_node(node))
                    output.append("")

                metrics = status.get("metrics", {})
                metrics_str = render_metrics(metrics)
                if metrics_str:
                    output.append(metrics_str)
                    output.append("")
            else:
                output.append(f"  {C.RED}Cannot reach orchestrator at {base_url}{C.RESET}")
                output.append(f"  {C.DIM}Is hazel-core running?{C.RESET}")
                output.append("")

            output.append(render_queue(queue))
            output.append("")
            output.append(render_relay(gateway))
            output.append(render_footer())

            print("\n".join(output))
            time.sleep(2)

    except KeyboardInterrupt:
        clear()
        print(f"\n  {C.GOLD}Dashboard closed.{C.RESET}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Hazel cluster dashboard")
    parser.add_argument("--host", default="localhost", help="Orchestrator host")
    parser.add_argument("--port", type=int, default=9000, help="Orchestrator port")
    parser.add_argument("--gateway-host", default=None, help="Gateway host (if different)")
    parser.add_argument("--gateway-port", type=int, default=9010, help="Gateway port")
    args = parser.parse_args()
    run(args.host, args.port, args.gateway_host, args.gateway_port)


if __name__ == "__main__":
    main()
