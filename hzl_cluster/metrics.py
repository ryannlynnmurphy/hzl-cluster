"""
Cluster metrics -- time-series operational data.
Persists to SQLite for historical analysis. Powers the dashboard trends.
"""

import json
import logging
import sqlite3
import time
from typing import Dict, List, Optional

logger = logging.getLogger("hzl.metrics")

# ─────────────────────────────────────────────────────────────
# Predefined metric names
# ─────────────────────────────────────────────────────────────

METRIC_SYNC_DURATION        = "sync.duration_ms"
METRIC_SYNC_FETCHED         = "sync.items_fetched"
METRIC_SYNC_QUARANTINED     = "sync.items_quarantined"
METRIC_QUEUE_DEPTH          = "queue.depth"
METRIC_QUEUE_DELIVERED      = "queue.delivered"
METRIC_RELAY_ONLINE_SECONDS = "relay.online_seconds"
METRIC_FETCHER_SUCCESS      = "fetcher.success"
METRIC_FETCHER_FAILURE      = "fetcher.failure"
METRIC_NODE_CPU             = "node.cpu_percent"
METRIC_NODE_MEMORY          = "node.memory_percent"

# ─────────────────────────────────────────────────────────────
# Schema
# ─────────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS metrics (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_name TEXT    NOT NULL,
    value       REAL    NOT NULL,
    tags        TEXT,
    timestamp   REAL    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_metrics_name_time
    ON metrics(metric_name, timestamp);
"""


# ─────────────────────────────────────────────────────────────
# MetricsCollector
# ─────────────────────────────────────────────────────────────

class MetricsCollector:
    """Collects, persists, and queries operational metrics for the HZL cluster."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_DDL)
        self._conn.commit()
        logger.debug("MetricsCollector initialised — db=%s", db_path)

    # ── write ──────────────────────────────────────────────────

    def record(
        self,
        metric_name: str,
        value: float,
        tags: Optional[Dict] = None,
    ) -> None:
        """Insert a single metric data point with the current timestamp."""
        tags_json = json.dumps(tags) if tags is not None else None
        self._conn.execute(
            "INSERT INTO metrics (metric_name, value, tags, timestamp) VALUES (?, ?, ?, ?)",
            (metric_name, float(value), tags_json, time.time()),
        )
        self._conn.commit()

    # ── read ───────────────────────────────────────────────────

    def query(
        self,
        metric_name: str,
        since_hours: float = 24,
    ) -> List[Dict]:
        """Return data points for *metric_name* within the last *since_hours* hours.

        Each entry: {"timestamp": float, "value": float, "tags": dict or None}
        """
        cutoff = time.time() - since_hours * 3600
        rows = self._conn.execute(
            "SELECT timestamp, value, tags FROM metrics "
            "WHERE metric_name = ? AND timestamp >= ? "
            "ORDER BY timestamp ASC",
            (metric_name, cutoff),
        ).fetchall()
        return [
            {
                "timestamp": row["timestamp"],
                "value": row["value"],
                "tags": json.loads(row["tags"]) if row["tags"] else None,
            }
            for row in rows
        ]

    def summary(
        self,
        metric_name: str,
        since_hours: float = 24,
    ) -> Dict:
        """Return aggregate statistics for *metric_name* within *since_hours*.

        Returns {"count", "min", "max", "avg", "latest"}.
        Returns all-None values when there is no matching data.
        """
        cutoff = time.time() - since_hours * 3600
        row = self._conn.execute(
            "SELECT COUNT(*) AS cnt, MIN(value) AS mn, MAX(value) AS mx, "
            "       AVG(value) AS av "
            "FROM metrics "
            "WHERE metric_name = ? AND timestamp >= ?",
            (metric_name, cutoff),
        ).fetchone()

        count = row["cnt"]
        if count == 0:
            return {"count": 0, "min": None, "max": None, "avg": None, "latest": None}

        latest_row = self._conn.execute(
            "SELECT value FROM metrics "
            "WHERE metric_name = ? AND timestamp >= ? "
            "ORDER BY timestamp DESC LIMIT 1",
            (metric_name, cutoff),
        ).fetchone()

        return {
            "count":  count,
            "min":    row["mn"],
            "max":    row["mx"],
            "avg":    row["av"],
            "latest": latest_row["value"],
        }

    def all_metrics(self) -> List[str]:
        """Return a sorted list of all unique metric names stored in the database."""
        rows = self._conn.execute(
            "SELECT DISTINCT metric_name FROM metrics ORDER BY metric_name"
        ).fetchall()
        return [r["metric_name"] for r in rows]

    # ── maintenance ────────────────────────────────────────────

    def prune(self, older_than_days: int = 30) -> int:
        """Delete data points older than *older_than_days* days.

        Returns the number of rows deleted.
        """
        cutoff = time.time() - older_than_days * 86400
        cursor = self._conn.execute(
            "DELETE FROM metrics WHERE timestamp < ?",
            (cutoff,),
        )
        self._conn.commit()
        deleted = cursor.rowcount
        logger.debug("Pruned %d metric rows older than %d days", deleted, older_than_days)
        return deleted

    def close(self) -> None:
        """Close the underlying database connection."""
        self._conn.close()
        logger.debug("MetricsCollector closed — db=%s", self._db_path)
