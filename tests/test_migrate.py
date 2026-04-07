"""Tests for hzl_cluster.migrate -- schema migration system."""

import os
import tempfile

import pytest

from hzl_cluster.migrate import MIGRATIONS, SchemaMigrator, ensure_schema


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test.db")


# ---------------------------------------------------------------------------
# 1. Fresh database starts at version 0
# ---------------------------------------------------------------------------

def test_fresh_database_version_zero(db_path):
    migrator = SchemaMigrator(db_path)
    try:
        assert migrator.current_version() == 0
    finally:
        migrator.close()


# ---------------------------------------------------------------------------
# 2. Apply all migrations -- version equals max migration number
# ---------------------------------------------------------------------------

def test_apply_all_migrations(db_path):
    migrator = SchemaMigrator(db_path)
    try:
        applied = migrator.apply_migrations()
        expected_max = max(v for v, _, _ in MIGRATIONS)
        assert migrator.current_version() == expected_max
        assert len(applied) == len(MIGRATIONS)
        assert applied == [name for _, name, _ in MIGRATIONS]
    finally:
        migrator.close()


# ---------------------------------------------------------------------------
# 3. Idempotent -- second run returns empty list
# ---------------------------------------------------------------------------

def test_idempotent(db_path):
    migrator = SchemaMigrator(db_path)
    try:
        migrator.apply_migrations()
        second_run = migrator.apply_migrations()
        assert second_run == []
    finally:
        migrator.close()


# ---------------------------------------------------------------------------
# 4. Migration history includes all entries with timestamps
# ---------------------------------------------------------------------------

def test_migration_history(db_path):
    migrator = SchemaMigrator(db_path)
    try:
        migrator.apply_migrations()
        history = migrator.migration_history()

        assert len(history) == len(MIGRATIONS)

        for i, (version, name, _) in enumerate(MIGRATIONS):
            entry = history[i]
            assert entry["version"] == version
            assert entry["name"] == name
            assert isinstance(entry["applied_at"], float)
            assert entry["applied_at"] > 0
    finally:
        migrator.close()


# ---------------------------------------------------------------------------
# 5. Partial migration -- apply first 2, then apply the rest
# ---------------------------------------------------------------------------

def test_partial_migration(db_path):
    # First pass: apply only migrations up through version 2 by monkey-patching
    # the migrator's internal list via a subclass approach -- simpler to just
    # directly insert the first two migration records and apply from there.
    migrator = SchemaMigrator(db_path)
    try:
        # Manually run only the first two migrations
        partial = [(v, n, s) for v, n, s in MIGRATIONS if v <= 2]
        for version, name, sql in partial:
            for statement in sql.split(";"):
                statement = statement.strip()
                if statement:
                    migrator._conn.execute(statement)
            migrator._conn.execute(
                "INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)",
                (version, name, 0.0),
            )
        migrator._conn.commit()

        assert migrator.current_version() == 2

        # Now let apply_migrations pick up the rest
        applied = migrator.apply_migrations()
        remaining = [name for v, name, _ in MIGRATIONS if v > 2]
        assert applied == remaining

        expected_max = max(v for v, _, _ in MIGRATIONS)
        assert migrator.current_version() == expected_max
    finally:
        migrator.close()


# ---------------------------------------------------------------------------
# 6. ensure_schema convenience function works on a fresh database
# ---------------------------------------------------------------------------

def test_ensure_schema_convenience(db_path):
    applied = ensure_schema(db_path)

    assert len(applied) == len(MIGRATIONS)
    assert applied == [name for _, name, _ in MIGRATIONS]

    # Calling again on the same path is idempotent
    second = ensure_schema(db_path)
    assert second == []
