"""
Configuration validator -- catches misconfigurations before startup.
Validates schema, required fields, value ranges, and cross-references.
"""

from typing import Union

# ANSI color codes for terminal output
_RED    = "\033[91m"
_GREEN  = "\033[92m"
_YELLOW = "\033[93m"
_RESET  = "\033[0m"

VALID_ROLES = {"core", "worker", "gateway", "mobile"}
WILDCARD_NODES = {"any_worker", "any_node"}

REQUIRED_TOP_LEVEL = {"cluster", "nodes", "routing", "network"}

REQUIRED_TASK_MAP_FIELDS = {"model", "preferred_node", "max_tokens", "timeout"}


def validate_config(config: dict) -> list:
    """
    Validate the HZL cluster config dict.

    Returns a list of error strings. An empty list means the config is valid.
    """
    errors = []

    if not isinstance(config, dict):
        errors.append("Config must be a mapping (dict), got: {}".format(type(config).__name__))
        return errors

    # ── 1. Required top-level keys ────────────────────────────────
    for key in sorted(REQUIRED_TOP_LEVEL):
        if key not in config:
            errors.append("Missing required top-level key: '{}'".format(key))

    # Bail early if the structural keys are missing — later checks depend on them.
    if errors:
        return errors

    nodes_cfg   = config.get("nodes", {}) or {}
    cluster_cfg = config.get("cluster", {}) or {}
    routing_cfg = config.get("routing", {}) or {}
    network_cfg = config.get("network", {}) or {}

    node_names = set(nodes_cfg.keys())

    # ── 2. cluster.core_node must reference an existing node ──────
    core_node = cluster_cfg.get("core_node")
    if not core_node:
        errors.append("cluster.core_node is missing or empty")
    elif core_node not in node_names:
        errors.append(
            "cluster.core_node '{}' does not match any node defined in nodes "
            "(known nodes: {})".format(core_node, sorted(node_names))
        )

    # ── 3. Every node: valid role + capabilities list ─────────────
    hostnames_seen: dict = {}
    for node_name, node_cfg in nodes_cfg.items():
        if not isinstance(node_cfg, dict):
            errors.append("nodes.{}: must be a mapping, got {}".format(
                node_name, type(node_cfg).__name__))
            continue

        role = node_cfg.get("role")
        if role is None:
            errors.append("nodes.{}: missing required field 'role'".format(node_name))
        elif role not in VALID_ROLES:
            errors.append(
                "nodes.{}: invalid role '{}' -- must be one of {}".format(
                    node_name, role, sorted(VALID_ROLES))
            )

        caps = node_cfg.get("capabilities")
        if caps is None:
            errors.append("nodes.{}: missing required field 'capabilities'".format(node_name))
        elif not isinstance(caps, list):
            errors.append(
                "nodes.{}: 'capabilities' must be a list, got {}".format(
                    node_name, type(caps).__name__)
            )

        # Track hostnames for duplicate check (node name is the hostname here)
        hostname = node_cfg.get("hostname", node_name)
        if hostname in hostnames_seen:
            errors.append(
                "Duplicate hostname '{}' found on nodes '{}' and '{}'".format(
                    hostname, hostnames_seen[hostname], node_name)
            )
        else:
            hostnames_seen[hostname] = node_name

    # ── 4 & 5. routing.task_map entries ───────────────────────────
    task_map = routing_cfg.get("task_map", {}) or {}
    for task_name, task_cfg in task_map.items():
        if not isinstance(task_cfg, dict):
            errors.append(
                "routing.task_map.{}: must be a mapping, got {}".format(
                    task_name, type(task_cfg).__name__)
            )
            continue

        # Required fields present
        for field in sorted(REQUIRED_TASK_MAP_FIELDS):
            if field not in task_cfg:
                errors.append(
                    "routing.task_map.{}: missing required field '{}'".format(task_name, field)
                )

        # model must be str or null
        if "model" in task_cfg:
            model_val = task_cfg["model"]
            if model_val is not None and not isinstance(model_val, str):
                errors.append(
                    "routing.task_map.{}: 'model' must be a string or null, got {}".format(
                        task_name, type(model_val).__name__)
                )

        # preferred_node must be a valid node name or wildcard
        if "preferred_node" in task_cfg:
            preferred = task_cfg["preferred_node"]
            if preferred not in node_names and preferred not in WILDCARD_NODES:
                errors.append(
                    "routing.task_map.{}: preferred_node '{}' is not a known node name "
                    "or wildcard ('any_worker', 'any_node') -- known nodes: {}".format(
                        task_name, preferred, sorted(node_names))
                )

    # ── 6. network.discovery_port ─────────────────────────────────
    discovery_port = network_cfg.get("discovery_port")
    if discovery_port is None:
        errors.append("network.discovery_port is missing")
    elif not isinstance(discovery_port, int):
        errors.append(
            "network.discovery_port must be an integer, got {}".format(
                type(discovery_port).__name__)
        )
    elif not (1024 <= discovery_port <= 65535):
        errors.append(
            "network.discovery_port {} is out of range -- must be 1024-65535".format(
                discovery_port)
        )

    # ── 7. network.heartbeat_interval must be positive ────────────
    heartbeat = network_cfg.get("heartbeat_interval")
    if heartbeat is None:
        errors.append("network.heartbeat_interval is missing")
    elif not isinstance(heartbeat, (int, float)):
        errors.append(
            "network.heartbeat_interval must be a number, got {}".format(
                type(heartbeat).__name__)
        )
    elif heartbeat <= 0:
        errors.append(
            "network.heartbeat_interval must be a positive number, got {}".format(heartbeat)
        )

    # ── 8. relay section (optional) ───────────────────────────────
    relay_cfg = config.get("relay")
    if relay_cfg is not None:
        if not isinstance(relay_cfg, dict):
            errors.append("relay: must be a mapping if present")
        else:
            gpio_pin = relay_cfg.get("gpio_pin")
            if gpio_pin is None:
                errors.append("relay.gpio_pin is missing")
            elif not isinstance(gpio_pin, int):
                errors.append(
                    "relay.gpio_pin must be an integer, got {}".format(
                        type(gpio_pin).__name__)
                )

            max_dur = relay_cfg.get("max_internet_duration")
            if max_dur is None:
                errors.append("relay.max_internet_duration is missing")
            elif not isinstance(max_dur, (int, float)):
                errors.append(
                    "relay.max_internet_duration must be a number, got {}".format(
                        type(max_dur).__name__)
                )
            elif max_dur <= 0:
                errors.append(
                    "relay.max_internet_duration must be positive, got {}".format(max_dur)
                )

    # ── 9. queue section (optional) ───────────────────────────────
    queue_cfg = config.get("queue")
    if queue_cfg is not None:
        if not isinstance(queue_cfg, dict):
            errors.append("queue: must be a mapping if present")
        else:
            db_path = queue_cfg.get("db_path")
            if db_path is None:
                errors.append("queue.db_path is missing")
            elif not isinstance(db_path, str):
                errors.append(
                    "queue.db_path must be a string, got {}".format(type(db_path).__name__)
                )

            max_retries = queue_cfg.get("max_retries")
            if max_retries is None:
                errors.append("queue.max_retries is missing")
            elif not isinstance(max_retries, int):
                errors.append(
                    "queue.max_retries must be an integer, got {}".format(
                        type(max_retries).__name__)
                )
            elif max_retries <= 0:
                errors.append(
                    "queue.max_retries must be a positive integer, got {}".format(max_retries)
                )

    # ── 10. No duplicate node hostnames ───────────────────────────
    # (handled inside node loop above -- hostnames_seen tracks this)

    return errors


def validate_and_report(config: dict) -> bool:
    """
    Run validate_config, print colored results, and return True if valid.
    """
    errors = validate_config(config)

    if not errors:
        print("{}Config valid -- no errors found.{}".format(_GREEN, _RESET))
        return True

    print("{}Config validation failed -- {} error(s):{}\n".format(
        _RED, len(errors), _RESET))
    for i, err in enumerate(errors, 1):
        print("  {}{}. {}{}".format(_YELLOW, i, err, _RESET))
    print()
    return False
