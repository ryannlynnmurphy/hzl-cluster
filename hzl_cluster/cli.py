#!/usr/bin/env python3
"""
cli.py — Unified hazel CLI entry point.

Usage:
    hazel status
    hazel dashboard
    hazel deploy --role <core|worker|gateway> [--name <hostname>]
    hazel sync
    hazel queue
    hazel queue send --destination <dest> --action <action> [--payload <json>]
    hazel fetch weather
    hazel fetch email
    hazel fetch news
    hazel relay state
    hazel relay lock
    hazel relay unlock
    hazel relay emergency
    hazel version
"""

import argparse
import io
import json
import sys
from urllib.request import urlopen, Request
from urllib.error import URLError


# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

from hzl_cluster import __version__

GATEWAY_BASE = "http://localhost:9010"
ORCH_BASE    = "http://localhost:9000"


# ---------------------------------------------------------------------------
# Stdout encoding fix (Windows box-drawing chars)
# ---------------------------------------------------------------------------

def _fix_stdout_encoding() -> None:
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


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get(path: str, base: str = GATEWAY_BASE, timeout: float = 4.0) -> dict:
    url = f"{base}{path}"
    try:
        resp = urlopen(url, timeout=timeout)
        return json.loads(resp.read())
    except URLError as exc:
        print(f"  ! Gateway unreachable ({url}): {exc.reason}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"  ! Request failed ({url}): {exc}", file=sys.stderr)
        sys.exit(1)


def _post(path: str, data: dict, base: str = GATEWAY_BASE, timeout: float = 10.0) -> dict:
    url = f"{base}{path}"
    body = json.dumps(data).encode()
    req = Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    try:
        resp = urlopen(req, timeout=timeout)
        return json.loads(resp.read())
    except URLError as exc:
        print(f"  ! Gateway unreachable ({url}): {exc.reason}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"  ! Request failed ({url}): {exc}", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------

def cmd_status(_args: argparse.Namespace) -> None:
    """Delegate to deploy.show_status()."""
    from hzl_cluster.deploy import show_status
    show_status()


def cmd_dashboard(args: argparse.Namespace) -> None:
    """Launch the real-time terminal dashboard."""
    # dashboard.main() reads sys.argv so we rebuild it.
    host = getattr(args, "host", "localhost")
    port = getattr(args, "port", 9000)
    sys.argv = ["hazel-dashboard", "--host", str(host), "--port", str(port)]
    from hzl_cluster.dashboard import main as dashboard_main
    dashboard_main()


def cmd_deploy(args: argparse.Namespace) -> None:
    """Deploy a Pi node (delegates to deploy.deploy())."""
    from hzl_cluster.deploy import deploy, BANNER, C
    if not args.role:
        print("  ! --role is required. Choose: core | worker | gateway", file=sys.stderr)
        sys.exit(1)
    name = args.name or ("hazel-worker-01" if args.role == "worker" else f"hazel-{args.role}")
    deploy(args.role, name)


def cmd_sync(_args: argparse.Namespace) -> None:
    """Trigger a sync cycle on the gateway."""
    print("  > Triggering sync cycle on gateway...")
    result = _post("/sync", {})
    status = result.get("status", result)
    print(f"  + {status}")


def cmd_queue(args: argparse.Namespace) -> None:
    """Show message queue status, or send a message."""
    if getattr(args, "queue_cmd", None) == "send":
        _cmd_queue_send(args)
        return

    # Default: show queue status
    data = _get("/queue")
    total = data.get("total_pending", data.get("queued", 0))
    print(f"  Queue status")
    print(f"    Pending: {total}")
    by_dest = data.get("by_destination", {})
    if by_dest:
        for dest, count in by_dest.items():
            print(f"      -> {dest}: {count}")
    by_status = data.get("by_status", {})
    if by_status:
        for st, count in by_status.items():
            print(f"    {st}: {count}")


def _cmd_queue_send(args: argparse.Namespace) -> None:
    """Send a message to the queue via the gateway REST API."""
    try:
        payload = json.loads(args.payload) if args.payload else {}
    except json.JSONDecodeError as exc:
        print(f"  ! Invalid JSON for --payload: {exc}", file=sys.stderr)
        sys.exit(1)

    msg = {
        "source":      "cli",
        "destination": args.destination,
        "msg_type":    getattr(args, "msg_type", "command"),
        "action":      args.action,
        "payload":     payload,
        "priority":    getattr(args, "priority", "normal"),
        "ttl":         86400,
    }
    result = _post("/request", msg)
    msg_id = result.get("id", result.get("message_id", "?"))
    print(f"  + Message queued: {msg_id}")


def _fetch_cmd(fetch_type: str) -> None:
    """Queue a fetch request of the given type (weather | email | news)."""
    msg = {
        "source":      "cli",
        "destination": "hazel-gateway",
        "msg_type":    "fetch_request",
        "action":      f"fetch_{fetch_type}",
        "payload":     {},
        "priority":    "normal",
        "ttl":         3600,
    }
    print(f"  > Queuing {fetch_type} fetch...")
    result = _post("/request", msg)
    msg_id = result.get("id", result.get("message_id", "?"))
    print(f"  + Queued: {msg_id}")


def cmd_fetch(args: argparse.Namespace) -> None:
    fetch_map = {
        "weather": "weather",
        "email":   "email",
        "news":    "news",
    }
    target = args.fetch_target
    if target not in fetch_map:
        print(f"  ! Unknown fetch target '{target}'. Choose: weather | email | news", file=sys.stderr)
        sys.exit(1)
    _fetch_cmd(fetch_map[target])


def cmd_relay(args: argparse.Namespace) -> None:
    relay_cmd = args.relay_cmd

    if relay_cmd == "state":
        data = _get("/state")
        relay = data.get("relay", data)
        state = relay.get("state", relay) if isinstance(relay, dict) else relay
        print(f"  Relay state: {state}")
        return

    endpoint_map = {
        "lock":      "/lock",
        "unlock":    "/unlock",
        "emergency": "/emergency",
    }
    if relay_cmd not in endpoint_map:
        print(f"  ! Unknown relay command '{relay_cmd}'.", file=sys.stderr)
        sys.exit(1)

    print(f"  > Sending relay {relay_cmd}...")
    result = _post(endpoint_map[relay_cmd], {})
    status = result.get("status", result)
    print(f"  + {status}")


def cmd_version(_args: argparse.Namespace) -> None:
    print(f"hazel {__version__}")


# ---------------------------------------------------------------------------
# Parser construction
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hazel",
        description="Hazel cluster CLI — manage and monitor your Pi cluster.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  status              Show cluster health
  dashboard           Launch real-time terminal dashboard
  deploy              Deploy a Pi node
  sync                Trigger a sync cycle on the gateway
  queue               Show message queue status
  queue send          Send a message to the queue
  fetch weather       Queue a weather fetch
  fetch email         Queue an email fetch
  fetch news          Queue a news fetch
  relay state         Show relay state
  relay lock          Lock the air-gap relay
  relay unlock        Unlock the air-gap relay
  relay emergency     Emergency disconnect
  version             Show version
        """,
    )

    sub = parser.add_subparsers(dest="command", metavar="<command>")
    sub.required = True

    # -- status --
    sub.add_parser("status", help="Show cluster health")

    # -- dashboard --
    dash_p = sub.add_parser("dashboard", help="Launch real-time terminal dashboard")
    dash_p.add_argument("--host", default="localhost", help="Orchestrator host (default: localhost)")
    dash_p.add_argument("--port", type=int, default=9000, help="Orchestrator port (default: 9000)")

    # -- deploy --
    deploy_p = sub.add_parser("deploy", help="Deploy a Pi node")
    deploy_p.add_argument("--role", choices=["core", "worker", "gateway"],
                          required=True, help="Node role")
    deploy_p.add_argument("--name", help="Hostname for the node")

    # -- sync --
    sub.add_parser("sync", help="Trigger a sync cycle on the gateway")

    # -- queue / queue send --
    queue_p = sub.add_parser("queue", help="Message queue commands")
    queue_sub = queue_p.add_subparsers(dest="queue_cmd", metavar="<subcommand>")
    send_p = queue_sub.add_parser("send", help="Send a message to the queue")
    send_p.add_argument("--destination", required=True, help="Destination node name")
    send_p.add_argument("--action", required=True, help="Action name")
    send_p.add_argument("--payload", default=None, help="JSON payload string")
    send_p.add_argument("--msg-type", dest="msg_type", default="command",
                        help="Message type (default: command)")
    send_p.add_argument("--priority", default="normal",
                        choices=["critical", "normal", "low"],
                        help="Message priority (default: normal)")

    # -- fetch --
    fetch_p = sub.add_parser("fetch", help="Queue a data fetch")
    fetch_p.add_argument("fetch_target", choices=["weather", "email", "news"],
                         metavar="<weather|email|news>", help="Type of fetch to queue")

    # -- relay --
    relay_p = sub.add_parser("relay", help="Relay / air-gap controls")
    relay_sub = relay_p.add_subparsers(dest="relay_cmd", metavar="<subcommand>")
    relay_sub.required = True
    relay_sub.add_parser("state",     help="Show current relay state")
    relay_sub.add_parser("lock",      help="Lock the air-gap relay")
    relay_sub.add_parser("unlock",    help="Unlock the air-gap relay")
    relay_sub.add_parser("emergency", help="Emergency disconnect")

    # -- version --
    sub.add_parser("version", help="Show version")

    return parser


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

_DISPATCH = {
    "status":    cmd_status,
    "dashboard": cmd_dashboard,
    "deploy":    cmd_deploy,
    "sync":      cmd_sync,
    "queue":     cmd_queue,
    "fetch":     cmd_fetch,
    "relay":     cmd_relay,
    "version":   cmd_version,
}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    _fix_stdout_encoding()
    parser = build_parser()
    args = parser.parse_args()
    handler = _DISPATCH.get(args.command)
    if handler is None:
        parser.error(f"Unknown command: {args.command}")
    handler(args)


if __name__ == "__main__":
    main()
