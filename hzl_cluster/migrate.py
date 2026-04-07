"""
Schema migration -- manages database versions and first-time setup.
Runs automatically on startup. Idempotent -- safe to run multiple times.
"""

import sqlite3
import time
from typing import Optional


MIGRATIONS = [
    (1, "create_messages_table", """
        CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            destination TEXT NOT NULL,
            msg_type TEXT NOT NULL,
            action TEXT NOT NULL,
            payload TEXT NOT NULL,
            priority TEXT NOT NULL DEFAULT 'normal',
            status TEXT NOT NULL DEFAULT 'queued',
            created_at REAL NOT NULL,
            delivered_at REAL,
            ttl INTEGER NOT NULL DEFAULT 86400
        );
        CREATE INDEX IF NOT EXISTS idx_messages_dest_status ON messages(destination, status);
    """),
    (2, "create_metrics_table", """
        CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            metric_name TEXT NOT NULL,
            value REAL NOT NULL,
            tags TEXT,
            timestamp REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_metrics_name_time ON metrics(metric_name, timestamp);
    """),
    (3, "create_audit_events_table", """
        CREATE TABLE IF NOT EXISTS audit_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            details TEXT,
            timestamp REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_audit_time ON audit_events(timestamp);
    """),
    (4, "add_retry_count_to_messages", """
        ALTER TABLE messages ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0;
    """),
]


class SchemaMigrator:
    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row
        self._create_tracking_table()

    def _create_tracking_table(self) -> None:
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at REAL NOT NULL
            );
        """)
        self._conn.commit()

    def current_version(self) -> int:
        row = self._conn.execute(
            "SELECT MAX(version) AS v FROM schema_migrations"
        ).fetchone()
        return row["v"] if row["v"] is not None else 0

    def apply_migrations(self) -> list[str]:
        applied: list[str] = []
        current = self.current_version()

        for version, name, sql in MIGRATIONS:
            if version <= current:
                continue

            # Some migration blocks contain multiple statements; execute each
            # statement separately so SQLite doesn't choke on the batch.
            for statement in sql.split(";"):
                statement = statement.strip()
                if statement:
                    self._conn.execute(statement)

            self._conn.execute(
                "INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)",
                (version, name, time.time()),
            )
            self._conn.commit()
            applied.append(name)

        return applied

    def migration_history(self) -> list[dict]:
        rows = self._conn.execute(
            "SELECT version, name, applied_at FROM schema_migrations ORDER BY version"
        ).fetchall()
        return [dict(row) for row in rows]

    def close(self) -> None:
        self._conn.close()


def ensure_schema(db_path: str) -> list[str]:
    """Convenience function: creates migrator, applies all pending, returns applied names."""
    migrator = SchemaMigrator(db_path)
    try:
        return migrator.apply_migrations()
    finally:
        migrator.close()
