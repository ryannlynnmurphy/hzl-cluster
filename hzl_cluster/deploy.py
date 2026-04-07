#!/usr/bin/env python3
"""
hazel-deploy — Set up a Raspberry Pi as a Hazel cluster node.

Usage:
    python -m hzl_cluster.deploy --role core
    python -m hzl_cluster.deploy --role worker --name hazel-worker-01
    python -m hzl_cluster.deploy --role gateway
    python -m hzl_cluster.deploy status
"""

import argparse
import io
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path


def _fix_stdout_encoding() -> None:
    """Reconfigure stdout for UTF-8 on Windows so box-drawing chars render."""
    if sys.stdout and hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
        except Exception:
            pass
    elif sys.stdout and hasattr(sys.stdout, "buffer"):
        try:
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        except Exception:
            pass

# Colors for terminal output
class C:
    GOLD    = "\033[33m"
    GREEN   = "\033[32m"
    RED     = "\033[31m"
    CYAN    = "\033[36m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    RESET   = "\033[0m"

HAZEL_DIR = Path("/var/hazel")
CONFIG_DIR = Path("/etc/hazel")
LOG_DIR = HAZEL_DIR / "logs"
STAGING_DIR = HAZEL_DIR / "staging"
QUARANTINE_DIR = HAZEL_DIR / "quarantine"

BANNER = f"""
{C.GOLD}{C.BOLD}
    ╔═══════════════════════════════════════╗
    ║          H A Z E L   O S              ║
    ║    Air-Gapped Cluster Deployment      ║
    ╚═══════════════════════════════════════╝
{C.RESET}"""


def print_step(msg: str) -> None:
    print(f"  {C.CYAN}>{C.RESET} {msg}")


def print_ok(msg: str) -> None:
    print(f"  {C.GREEN}+{C.RESET} {msg}")


def print_err(msg: str) -> None:
    print(f"  {C.RED}!{C.RESET} {msg}")


def print_header(msg: str) -> None:
    print(f"\n{C.BOLD}{C.GOLD}  {msg}{C.RESET}")


def check_root() -> bool:
    """Check if running as root (needed for systemd, hostname changes)."""
    return os.geteuid() == 0 if hasattr(os, 'geteuid') else True


def get_ip() -> str:
    """Get local IP address."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


def setup_directories() -> None:
    """Create Hazel directory structure."""
    print_header("Creating directories")
    for d in [HAZEL_DIR, CONFIG_DIR, LOG_DIR, STAGING_DIR, QUARANTINE_DIR]:
        d.mkdir(parents=True, exist_ok=True)
        print_ok(f"{d}")


def set_hostname(name: str) -> None:
    """Set the Pi's hostname."""
    print_header(f"Setting hostname: {name}")
    try:
        subprocess.run(["hostnamectl", "set-hostname", name], check=True)
        print_ok(f"Hostname set to {name}")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print_err("Could not set hostname (hostnamectl not available)")


def install_package() -> None:
    """Install hzl-cluster package."""
    print_header("Installing hzl-cluster")
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-e", "."],
            check=True, cwd=str(Path(__file__).parent.parent),
        )
        print_ok("hzl-cluster installed")
    except subprocess.CalledProcessError:
        print_err("pip install failed")


def write_config(role: str, name: str) -> None:
    """Generate node-specific config."""
    print_header("Writing configuration")

    config = {
        "schema_version": 2,
        "cluster": {"name": "HZL", "core_node": "hazel-core", "environment": "production"},
        "nodes": {
            name: {
                "role": role,
                "ip": None,
                "port": {"core": 8765, "worker": 8766, "gateway": 9010}.get(role, 8765),
                "orchestrator_port": 9000 if role == "core" else None,
                "capabilities": {
                    "core": ["orchestrator", "voice", "ui", "search", "home_control", "memory", "queue_hub"],
                    "worker": ["inference", "reasoning"],
                    "gateway": ["gateway", "fetch", "scan", "relay_control"],
                    "mobile": ["voice", "camera", "location", "queue"],
                }.get(role, []),
            }
        },
        "routing": {
            "task_map": {
                "voice_response": {"model": "claude-haiku-4-5-20251001", "preferred_node": "hazel-core", "capability": "voice", "max_tokens": 500, "timeout": 8},
                "reasoning": {"model": "claude-sonnet-4-6", "preferred_node": "any_worker", "capability": "reasoning", "max_tokens": 2000, "timeout": 30},
                "heavy_inference": {"model": "claude-sonnet-4-6", "preferred_node": "any_worker", "capability": "inference", "max_tokens": 4000, "timeout": 60},
                "gateway_fetch": {"model": None, "preferred_node": "hazel-gateway", "capability": "fetch", "max_tokens": 0, "timeout": 5},
                "gateway_sync": {"model": None, "preferred_node": "hazel-gateway", "capability": "relay_control", "max_tokens": 0, "timeout": 300},
            },
            "fallback_chain": ["preferred_node", "any_capable_node", "core", "cloud_direct"],
        },
        "network": {"discovery_port": 9099, "broadcast_addr": "255.255.255.255", "heartbeat_interval": 5, "heartbeat_jitter": 1.0, "node_timeout": 30, "beacon_version": 2},
        "thresholds": {"cpu_overload": 85, "memory_overload": 90, "cpu_weight": 0.70, "memory_weight": 0.30},
        "circuit_breaker": {"failure_threshold": 4, "recovery_timeout": 45},
        "paths": {"db": str(HAZEL_DIR / "hazel.db"), "log_dir": str(LOG_DIR), "config": str(CONFIG_DIR / "hzl_config.yaml")},
        "logging": {"level": "INFO", "format": "json"},
    }

    # Add role-specific sections
    if role == "gateway":
        config["relay"] = {
            "gpio_pin": 17, "max_internet_duration": 600, "watchdog_interval": 30,
            "boot_state": "core_connected", "wifi_interface": "wlan0",
            "ethernet_interface": "eth0", "watchdog_policy": "finish_active",
        }
        config["sync"] = {
            "schedule": "0 6 * * *", "max_batch_size": 50, "content_scan": True,
            "staging_dir": str(STAGING_DIR), "quarantine_dir": str(QUARANTINE_DIR),
            "max_staging_size_mb": 500,
        }

    if role in ("core", "gateway"):
        config["queue"] = {
            "db_path": str(HAZEL_DIR / "queue.db"), "max_retries": 3,
            "retry_backoff_base": 2.0, "default_ttl": 86400, "ack_timeout": 30,
        }

    import yaml
    config_path = CONFIG_DIR / "hzl_config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    print_ok(f"Config written to {config_path}")


def write_systemd_service(role: str, name: str) -> None:
    """Create systemd service file."""
    print_header("Creating systemd service")

    if role == "core":
        exec_start = f"{sys.executable} -m hzl_cluster.orchestrator"
        description = "Hazel Core Orchestrator"
    elif role == "gateway":
        exec_start = f"{sys.executable} -m hzl_cluster.gateway"
        description = "Hazel Gateway Daemon"
    elif role == "worker":
        exec_start = f"{sys.executable} -m hzl_cluster.orchestrator"
        description = "Hazel Worker Node"
    else:
        return

    service = f"""[Unit]
Description={description}
After=network.target

[Service]
Type=simple
User=pi
Environment=HZL_CONFIG=/etc/hazel/hzl_config.yaml
Environment=HZL_ORCH_HOST=0.0.0.0
ExecStart={exec_start}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
"""

    service_path = Path(f"/etc/systemd/system/hazel-{role}.service")
    try:
        with open(service_path, "w") as f:
            f.write(service)
        subprocess.run(["systemctl", "daemon-reload"], check=True)
        subprocess.run(["systemctl", "enable", f"hazel-{role}"], check=True)
        print_ok(f"Service created: hazel-{role}")
        print_ok(f"Run: sudo systemctl start hazel-{role}")
    except (PermissionError, subprocess.CalledProcessError, FileNotFoundError):
        print_err("Could not create systemd service (need root or not on Linux)")
        print_step(f"Manual start: HZL_CONFIG=/etc/hazel/hzl_config.yaml {exec_start}")


def show_status() -> None:
    """Show cluster status from this node's perspective."""
    print(BANNER)
    print_header("Node Status")

    hostname = socket.gethostname()
    ip = get_ip()
    print_ok(f"Hostname: {hostname}")
    print_ok(f"IP: {ip}")

    # Check if config exists
    config_path = CONFIG_DIR / "hzl_config.yaml"
    if config_path.exists():
        print_ok(f"Config: {config_path}")
    else:
        print_err("No config found. Run: hazel-deploy --role <role>")
        return

    # Check services
    print_header("Services")
    for role in ["core", "worker", "gateway"]:
        try:
            result = subprocess.run(
                ["systemctl", "is-active", f"hazel-{role}"],
                capture_output=True, text=True,
            )
            status = result.stdout.strip()
            if status == "active":
                print_ok(f"hazel-{role}: {C.GREEN}running{C.RESET}")
            else:
                print_step(f"hazel-{role}: {C.DIM}{status}{C.RESET}")
        except FileNotFoundError:
            print_step(f"hazel-{role}: {C.DIM}systemctl not available{C.RESET}")

    # Check cluster connectivity
    print_header("Cluster")
    try:
        from urllib.request import urlopen
        resp = urlopen("http://localhost:9000/health", timeout=2)
        data = json.loads(resp.read())
        print_ok(f"Orchestrator: {C.GREEN}healthy{C.RESET}")
    except Exception:
        print_step(f"Orchestrator: {C.DIM}not reachable{C.RESET}")

    try:
        from urllib.request import urlopen
        resp = urlopen("http://localhost:9000/nodes", timeout=2)
        nodes = json.loads(resp.read())
        for hostname, info in nodes.items():
            alive = f"{C.GREEN}alive{C.RESET}" if info.get("alive") else f"{C.RED}dead{C.RESET}"
            print_ok(f"  {hostname} ({info.get('role', '?')}): {alive}")
    except Exception:
        pass

    # Queue status
    try:
        from urllib.request import urlopen
        resp = urlopen("http://localhost:9000/queue", timeout=2)
        queue = json.loads(resp.read())
        total = queue.get("total_pending", 0)
        print_header("Queue")
        print_ok(f"Pending messages: {total}")
        for dest, count in queue.get("by_destination", {}).items():
            print_step(f"  -> {dest}: {count}")
    except Exception:
        pass

    print()


def deploy(role: str, name: str) -> None:
    """Full deployment sequence."""
    print(BANNER)
    print_header(f"Deploying as: {C.GOLD}{role}{C.RESET} ({name})")
    print()

    setup_directories()
    set_hostname(name)
    write_config(role, name)
    write_systemd_service(role, name)

    print_header("Deployment Complete")
    print()
    print(f"  {C.BOLD}Your node is ready.{C.RESET}")
    print()
    print(f"  Start the service:")
    print(f"    {C.CYAN}sudo systemctl start hazel-{role}{C.RESET}")
    print()
    print(f"  Check status:")
    print(f"    {C.CYAN}python -m hzl_cluster.deploy status{C.RESET}")
    print()
    print(f"  View logs:")
    print(f"    {C.CYAN}journalctl -u hazel-{role} -f{C.RESET}")
    print()


def main() -> None:
    _fix_stdout_encoding()
    parser = argparse.ArgumentParser(
        description="Deploy a Hazel cluster node",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m hzl_cluster.deploy --role core
  python -m hzl_cluster.deploy --role worker --name hazel-worker-01
  python -m hzl_cluster.deploy --role gateway
  python -m hzl_cluster.deploy status
        """,
    )
    parser.add_argument("command", nargs="?", default="deploy", choices=["deploy", "status"],
                       help="Command to run (default: deploy)")
    parser.add_argument("--role", choices=["core", "worker", "gateway"],
                       help="Node role")
    parser.add_argument("--name", help="Node hostname (default: hazel-<role>)")

    args = parser.parse_args()

    if args.command == "status":
        show_status()
        return

    if not args.role:
        parser.error("--role is required for deployment")

    name = args.name or f"hazel-{args.role}"
    if args.role == "worker" and not args.name:
        name = "hazel-worker-01"

    deploy(args.role, name)


if __name__ == "__main__":
    main()
