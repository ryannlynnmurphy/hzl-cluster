"""
Structured logging for the Hazel cluster.

Provides JSON-formatted logs with hostname, request_id, and module context.
Call setup_logging(config) once at startup.
"""
import json
import logging
import os
import socket
from datetime import datetime


class HazelFormatter(logging.Formatter):
    """JSON log formatter with hostname and timestamp."""

    _hostname = None

    def format(self, record):
        if not self._hostname:
            HazelFormatter._hostname = socket.gethostname()

        entry = {
            "ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "host": self._hostname,
            "module": record.module,
        }

        # Add extra fields if present
        if hasattr(record, "request_id"):
            entry["request_id"] = record.request_id
        if hasattr(record, "node"):
            entry["node"] = record.node
        if hasattr(record, "action"):
            entry["action"] = record.action
        if hasattr(record, "relay_state"):
            entry["relay_state"] = record.relay_state

        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)

        return json.dumps(entry)


def setup_logging(config: dict = None, level: str = "INFO") -> None:
    """Configure structured logging for the cluster."""
    config = config or {}
    log_cfg = config.get("logging", {})
    level_str = log_cfg.get("level", level).upper()
    fmt = log_cfg.get("format", "json")
    log_dir = config.get("paths", {}).get("log_dir", "./logs")

    os.makedirs(log_dir, exist_ok=True)

    if fmt == "json":
        formatter = HazelFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
        )

    # File handler
    log_file = os.path.join(log_dir, "hazel.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Configure root logger
    root = logging.getLogger()
    root.setLevel(getattr(logging, level_str, logging.INFO))

    # Remove existing handlers to avoid duplicates
    root.handlers.clear()
    root.addHandler(file_handler)
    root.addHandler(console_handler)


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the hzl prefix."""
    return logging.getLogger(f"hzl.{name}")
