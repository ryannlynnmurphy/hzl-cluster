"""
topology.py — ASCII art cluster topology visualizer.

Used by `hazel status --detail` to render a snapshot of:
  - Zone A (air-gapped cluster): core + workers + phone
  - USB relay state
  - Zone B (gateway)

Public API
----------
    render_topology(nodes: list[dict], relay_state: str = "core_connected") -> str

Node dict keys (all optional except "hostname"):
    hostname    str   e.g. "hazel-core"
    role        str   "core" | "worker" | "gateway" | "phone"
    status      str   "online" | "offline" | "docked" | "away"
    cpu         int   0-100  (shown for online nodes, omitted otherwise)
    mem         int   0-100  (shown for online nodes, omitted otherwise)
    wifi        str   "ON" | "OFF"  (shown for gateway nodes)

Relay states
------------
    core_connected      USB relay CLOSED  (normal air-gap operation)
    internet_connected  USB relay OPEN    (internet sync window -- warning)
    locked              USB relay LOCKED  (manual override -- no transitions)
    transitioning       USB relay ---     (in flux, show transitioning marker)
"""

from __future__ import annotations

from typing import List, Dict

# Box width (inner content width, excluding the two border chars)
_BOX_WIDTH = 44

# How many node columns to display per row in Zone A
_NODES_PER_ROW = 2

# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _box_top() -> str:
    return "  +" + "-" * _BOX_WIDTH + "+"


def _box_bottom() -> str:
    return "  +" + "-" * _BOX_WIDTH + "+"


def _box_empty() -> str:
    return "  |" + " " * _BOX_WIDTH + "|"


def _box_line(text: str, indent: int = 2) -> str:
    """Pad *text* to fit inside a box row with *indent* leading spaces."""
    content = " " * indent + text
    # Truncate if somehow too long, then right-pad to box width
    content = content[: _BOX_WIDTH]
    return "  |" + content.ljust(_BOX_WIDTH) + "|"


def _status_marker(status: str) -> str:
    s = status.lower()
    if s == "online":
        return "*"
    if s in ("docked", "away"):
        return "*"
    return "-"


def _status_label(node: dict) -> str:
    status = node.get("status", "online").upper()
    return status  # ONLINE, OFFLINE, DOCKED, AWAY


def _node_lines(node: dict) -> List[str]:
    """Return the lines that represent one node block (no box borders yet)."""
    hostname = node.get("hostname", "unknown")
    status = node.get("status", "online").lower()
    marker = _status_marker(status)
    label = _status_label(node)

    lines = [
        f"[{hostname}]",
        f"{marker} {label}",
    ]

    if status == "online":
        cpu = node.get("cpu")
        mem = node.get("mem")
        if cpu is not None:
            lines.append(f"CPU: {cpu}%")
        if mem is not None:
            lines.append(f"MEM: {mem}%")

    role = node.get("role", "").lower()
    if role == "gateway":
        wifi = node.get("wifi", "OFF")
        lines.append(f"WiFi: {wifi}")

    return lines


def _relay_label(relay_state: str) -> str:
    s = relay_state.lower()
    if s == "core_connected":
        return "CLOSED"
    if s == "internet_connected":
        return "OPEN  [!]"
    if s == "locked":
        return "LOCKED"
    if s == "transitioning":
        return "---"
    return relay_state.upper()


# ─────────────────────────────────────────────────────────────────────────────
# Zone builders
# ─────────────────────────────────────────────────────────────────────────────

def _render_zone_a(cluster_nodes: List[dict]) -> List[str]:
    """Build the lines for Zone A box (header + nodes in rows)."""
    rows: List[str] = []
    rows.append(_box_top())
    rows.append(_box_empty())
    rows.append(_box_line("ZONE A: AIR-GAPPED CLUSTER"))
    rows.append(_box_empty())

    # Lay out nodes in columns of _NODES_PER_ROW
    for i in range(0, max(len(cluster_nodes), 1), _NODES_PER_ROW):
        chunk = cluster_nodes[i: i + _NODES_PER_ROW]
        node_line_sets = [_node_lines(n) for n in chunk]
        max_lines = max(len(ls) for ls in node_line_sets)

        # Pad each node's line list to the same height
        for ls in node_line_sets:
            while len(ls) < max_lines:
                ls.append("")

        # Column width: split box interior equally
        col_w = _BOX_WIDTH // _NODES_PER_ROW  # e.g. 22

        for line_idx in range(max_lines):
            parts = []
            for ls in node_line_sets:
                parts.append(ls[line_idx].ljust(col_w))
            # Join and trim to box width
            content = "  " + "".join(parts)
            content = content[: _BOX_WIDTH]
            rows.append("  |" + content.ljust(_BOX_WIDTH) + "|")

        rows.append(_box_empty())

    rows.append(_box_bottom())
    return rows


def _render_relay(relay_state: str) -> List[str]:
    """Build the relay connector lines between Zone A and Zone B."""
    label = f"[USB RELAY: {_relay_label(relay_state)}]"
    # Centre the label in the box interior (44 chars), offset by box margin (2)
    total_width = _BOX_WIDTH + 4  # 48 chars to match boxes
    connector = "|"
    connector_line = connector.center(total_width)
    label_line = label.center(total_width)

    return [
        "  " + "+" + "-" * (_BOX_WIDTH // 2) + "+" + "-" * (_BOX_WIDTH - _BOX_WIDTH // 2 - 1) + "+",
        connector_line,
        label_line,
        connector_line,
    ]


def _render_zone_b(gateway_nodes: List[dict]) -> List[str]:
    """Build the lines for Zone B box (gateway nodes)."""
    rows: List[str] = []
    rows.append("  +" + "-" * (_BOX_WIDTH // 2) + "+" + "-" * (_BOX_WIDTH - _BOX_WIDTH // 2 - 1) + "+")
    rows.append(_box_empty())
    rows.append(_box_line("ZONE B: GATEWAY"))

    for node in gateway_nodes:
        rows.append(_box_empty())
        for line in _node_lines(node):
            rows.append(_box_line(line))

    rows.append(_box_empty())
    rows.append(_box_bottom())
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def render_topology(nodes: List[Dict], relay_state: str = "core_connected") -> str:
    """
    Render an ASCII art topology diagram for the Hazel cluster.

    Parameters
    ----------
    nodes:
        List of node dicts.  Each must have at least "hostname".
        Optional keys: role, status, cpu, mem, wifi.
    relay_state:
        One of "core_connected", "internet_connected", "locked",
        "transitioning".  Controls the relay banner between zones.

    Returns
    -------
    str
        Multi-line ASCII art string, ready to print.
    """
    cluster_nodes = [n for n in nodes if n.get("role", "").lower() != "gateway"]
    gateway_nodes = [n for n in nodes if n.get("role", "").lower() == "gateway"]

    lines: List[str] = []
    lines += _render_zone_a(cluster_nodes)
    lines += _render_relay(relay_state)
    lines += _render_zone_b(gateway_nodes)

    return "\n".join(lines)
