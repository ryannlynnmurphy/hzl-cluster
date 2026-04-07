"""
queue_hub.py
------------
HazelMessage, QueueDB, and QueueHub — inter-node message queuing for the HZL cluster.

HazelMessage  — universal message dataclass
QueueDB       — SQLite-backed persistence
QueueHub      — central router / message broker (runs on Core)
"""

import logging
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger("hzl.queue")

# Priority ordering for SQL sorts (lower number = higher urgency)
_PRIORITY_ORDER = {"critical": 0, "normal": 1, "low": 2}


# ─────────────────────────────────────────────────────────────
# HazelMessage
# ─────────────────────────────────────────────────────────────

@dataclass
class HazelMessage:
    """Universal message format for inter-node communication."""

    id: str
    source: str
    destination: str
    msg_type: str
    action: str
    payload: dict
    priority: str          # "critical" | "normal" | "low"
    status: str            # "queued" | "in_transit" | "delivered" | "failed"
    created_at: float
    delivered_at: Optional[float]
    ttl: int               # seconds; 0 = never expires

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def create(
        cls,
        source: str,
        destination: str,
        msg_type: str,
        action: str,
        payload: Optional[dict] = None,
        priority: str = "normal",
        ttl: int = 86400,
        message_id: Optional[str] = None,
    ) -> "HazelMessage":
        return cls(
            id=message_id or str(uuid.uuid4()),
            source=source,
            destination=destination,
            msg_type=msg_type,
            action=action,
            payload=payload or {},
            priority=priority,
            status="queued",
            created_at=time.time(),
            delivered_at=None,
            ttl=ttl,
        )

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "source": self.source,
            "destination": self.destination,
            "msg_type": self.msg_type,
            "action": self.action,
            "payload": self.payload,
            "priority": self.priority,
            "status": self.status,
            "created_at": self.created_at,
            "delivered_at": self.delivered_at,
            "ttl": self.ttl,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "HazelMessage":
        return cls(
            id=data["id"],
            source=data["source"],
            destination=data["destination"],
            msg_type=data["msg_type"],
            action=data["action"],
            payload=data.get("payload", {}),
            priority=data.get("priority", "normal"),
            status=data.get("status", "queued"),
            created_at=data["created_at"],
            delivered_at=data.get("delivered_at"),
            ttl=data.get("ttl", 86400),
        )

    # ------------------------------------------------------------------
    # TTL check
    # ------------------------------------------------------------------

    def is_expired(self) -> bool:
        """Returns True if ttl > 0 and the message has outlived its ttl."""
        if self.ttl <= 0:
            return False
        return (time.time() - self.created_at) > self.ttl


# ─────────────────────────────────────────────────────────────
# QueueDB
# ─────────────────────────────────────────────────────────────

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS messages (
    id           TEXT PRIMARY KEY,
    source       TEXT NOT NULL,
    destination  TEXT NOT NULL,
    msg_type     TEXT NOT NULL,
    action       TEXT NOT NULL,
    payload      TEXT NOT NULL,
    priority     TEXT NOT NULL DEFAULT 'normal',
    status       TEXT NOT NULL DEFAULT 'queued',
    created_at   REAL NOT NULL,
    delivered_at REAL,
    ttl          INTEGER NOT NULL DEFAULT 86400
)
"""

_CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_dest_status
ON messages (destination, status)
"""

# Map priority label -> integer for ORDER BY
_PRIORITY_CASE = "CASE priority WHEN 'critical' THEN 0 WHEN 'normal' THEN 1 ELSE 2 END"


class QueueDB:
    """SQLite-backed message persistence for the queue hub."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(_CREATE_TABLE)
        self._conn.execute(_CREATE_INDEX)
        self._conn.commit()

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def store(self, msg: HazelMessage) -> None:
        """INSERT OR REPLACE a message."""
        import json
        self._conn.execute(
            """
            INSERT OR REPLACE INTO messages
                (id, source, destination, msg_type, action, payload,
                 priority, status, created_at, delivered_at, ttl)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                msg.id,
                msg.source,
                msg.destination,
                msg.msg_type,
                msg.action,
                json.dumps(msg.payload),
                msg.priority,
                msg.status,
                msg.created_at,
                msg.delivered_at,
                msg.ttl,
            ),
        )
        self._conn.commit()

    def update_status(self, message_id: str, status: str) -> None:
        """Update message status; sets delivered_at when status is 'delivered'."""
        delivered_at = time.time() if status == "delivered" else None
        self._conn.execute(
            "UPDATE messages SET status = ?, delivered_at = ? WHERE id = ?",
            (status, delivered_at, message_id),
        )
        self._conn.commit()

    def expire(self) -> int:
        """
        Delete messages where ttl > 0, elapsed > ttl, and status in (queued, failed).
        Returns count of deleted rows.
        """
        now = time.time()
        cur = self._conn.execute(
            """
            DELETE FROM messages
            WHERE ttl > 0
              AND (? - created_at) > ttl
              AND status IN ('queued', 'failed')
            """,
            (now,),
        )
        self._conn.commit()
        return cur.rowcount

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get(self, message_id: str) -> Optional[HazelMessage]:
        """Return HazelMessage by id, or None."""
        row = self._conn.execute(
            "SELECT * FROM messages WHERE id = ?", (message_id,)
        ).fetchone()
        return self._row_to_msg(row) if row else None

    def get_pending(self, destination: str) -> List[HazelMessage]:
        """
        Return queued messages for destination, ordered by priority
        (critical first) then created_at ASC.
        """
        rows = self._conn.execute(
            f"""
            SELECT * FROM messages
            WHERE destination = ? AND status = 'queued'
            ORDER BY {_PRIORITY_CASE}, created_at ASC
            """,
            (destination,),
        ).fetchall()
        return [self._row_to_msg(r) for r in rows]

    def count_pending(self, destination: Optional[str] = None) -> int:
        """Count queued messages, optionally filtered by destination."""
        if destination is not None:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM messages WHERE status = 'queued' AND destination = ?",
                (destination,),
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM messages WHERE status = 'queued'"
            ).fetchone()
        return row[0]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_msg(row: sqlite3.Row) -> HazelMessage:
        import json
        return HazelMessage(
            id=row["id"],
            source=row["source"],
            destination=row["destination"],
            msg_type=row["msg_type"],
            action=row["action"],
            payload=json.loads(row["payload"]),
            priority=row["priority"],
            status=row["status"],
            created_at=row["created_at"],
            delivered_at=row["delivered_at"],
            ttl=row["ttl"],
        )


# ─────────────────────────────────────────────────────────────
# QueueHub
# ─────────────────────────────────────────────────────────────

class QueueHub:
    """
    Central message router for the HZL cluster.
    Intended to run on the Core node.
    """

    def __init__(self, config: dict) -> None:
        queue_cfg = config.get("queue", {})
        db_path         = queue_cfg.get("db_path", "queue.db")
        self.max_retries        = int(queue_cfg.get("max_retries", 3))
        self.retry_backoff_base = float(queue_cfg.get("retry_backoff_base", 2.0))
        self.default_ttl        = int(queue_cfg.get("default_ttl", 86400))
        self.ack_timeout        = float(queue_cfg.get("ack_timeout", 30.0))

        self._db = QueueDB(db_path)
        logger.info(f"[QueueHub] Initialised — db={db_path}")

    # ------------------------------------------------------------------
    # Ingest
    # ------------------------------------------------------------------

    def ingest(self, messages: List[HazelMessage]) -> dict:
        """
        Accept messages into the queue.
        Rejects expired messages.
        Returns {"accepted": N, "rejected": N, "errors": [...]}.
        """
        accepted = 0
        rejected = 0
        errors: List[str] = []

        for msg in messages:
            if msg.is_expired():
                rejected += 1
                errors.append(f"{msg.id}: expired (ttl={msg.ttl})")
                logger.debug(f"[QueueHub] Rejected expired message {msg.id}")
                continue
            try:
                self._db.store(msg)
                accepted += 1
                logger.debug(f"[QueueHub] Accepted {msg.id} -> {msg.destination}")
            except Exception as exc:
                rejected += 1
                errors.append(f"{msg.id}: store error — {exc}")
                logger.error(f"[QueueHub] Store error for {msg.id}: {exc}")

        return {"accepted": accepted, "rejected": rejected, "errors": errors}

    # ------------------------------------------------------------------
    # Delivery
    # ------------------------------------------------------------------

    def get_outbound(self, destination: str) -> List[HazelMessage]:
        """Return pending messages for the given destination."""
        return self._db.get_pending(destination)

    def ack(self, message_id: str) -> None:
        """Mark a message as delivered."""
        self._db.update_status(message_id, "delivered")
        logger.debug(f"[QueueHub] ACK {message_id}")

    def fail(self, message_id: str, error: str) -> None:
        """Mark a message as failed and log the error."""
        self._db.update_status(message_id, "failed")
        logger.warning(f"[QueueHub] FAIL {message_id}: {error}")

    # ------------------------------------------------------------------
    # Status / maintenance
    # ------------------------------------------------------------------

    def status(self) -> dict:
        """
        Returns {"total_pending": N, "by_destination": {dest: count}}.
        """
        total = self._db.count_pending()

        rows = self._db._conn.execute(
            "SELECT destination, COUNT(*) as cnt FROM messages "
            "WHERE status = 'queued' GROUP BY destination"
        ).fetchall()
        by_dest = {r["destination"]: r["cnt"] for r in rows}

        return {"total_pending": total, "by_destination": by_dest}

    def expire_old(self) -> int:
        """Remove expired messages. Returns count deleted."""
        count = self._db.expire()
        if count:
            logger.info(f"[QueueHub] Expired {count} message(s)")
        return count

    def close(self) -> None:
        self._db.close()
