"""
test_backup.py — Tests for BackupManager.

All tests use a temporary directory so no real cluster data is touched.
"""

import tarfile
import time
from pathlib import Path

import pytest

from hzl_cluster.backup import BackupManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_manager(tmp_path: Path, **overrides) -> BackupManager:
    cfg = {"backup": {"target_dir": str(tmp_path / "backups"), **overrides}}
    return BackupManager(cfg)


def _write_sources(tmp_path: Path, names: list[str]) -> list[str]:
    """Write small temp files and return their paths."""
    paths = []
    for name in names:
        p = tmp_path / name
        p.write_text(f"data for {name}")
        paths.append(str(p))
    return paths


def _make_tarball(target_dir: Path, name: str, members: list[tuple[str, str]]) -> Path:
    """Create a .tar.gz with given (arcname, content) pairs. Returns path."""
    target_dir.mkdir(parents=True, exist_ok=True)
    p = target_dir / name
    with tarfile.open(p, "w:gz") as tar:
        for arcname, content in members:
            import io
            data = content.encode()
            info = tarfile.TarInfo(name=arcname)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return p


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_create_backup(tmp_path):
    """create_backup should produce a .tar.gz file in target_dir."""
    sources = _write_sources(tmp_path, ["queue.db", "hzl_config.yaml"])
    mgr = _make_manager(tmp_path)

    result = mgr.create_backup(sources=sources)

    assert result["success"] is True
    assert result["file"].endswith(".tar.gz")
    assert Path(result["file"]).exists()
    assert result["size_bytes"] > 0
    assert result["sources"] == 2
    assert result["encrypted"] is False


def test_backup_contains_sources(tmp_path):
    """The tarball should contain each source file that was backed up."""
    sources = _write_sources(tmp_path, ["queue.db", "relay_audit.log"])
    mgr = _make_manager(tmp_path)

    result = mgr.create_backup(sources=sources)
    assert result["success"] is True

    names = mgr.restore_preview(result["file"])
    # Each source basename should appear somewhere in the archive member names
    for src in sources:
        basename = Path(src).name
        assert any(basename in n for n in names), f"{basename} not found in {names}"


def test_list_backups(tmp_path):
    """list_backups should return one entry per backup file created."""
    sources = _write_sources(tmp_path, ["hazel.db"])
    mgr = _make_manager(tmp_path)

    mgr.create_backup(sources=sources)
    # Small sleep to guarantee different timestamps in the filenames
    time.sleep(1.1)
    mgr.create_backup(sources=sources)

    backups = mgr.list_backups()
    assert len(backups) == 2
    for entry in backups:
        assert "name" in entry
        assert "size_bytes" in entry
        assert "date" in entry


def test_verify_backup(tmp_path):
    """verify_backup should return success=True for a valid tarball."""
    sources = _write_sources(tmp_path, ["queue.db"])
    mgr = _make_manager(tmp_path)
    result = mgr.create_backup(sources=sources)
    assert result["success"] is True

    verification = mgr.verify_backup(result["file"])
    assert verification["success"] is True
    assert verification["file_count"] >= 1
    assert verification["error"] is None


def test_prune_keeps_recent(tmp_path):
    """prune_old_backups(keep=2) with 5 backups should delete 3."""
    sources = _write_sources(tmp_path, ["hazel.db"])
    mgr = _make_manager(tmp_path)

    for _ in range(5):
        mgr.create_backup(sources=sources)
        time.sleep(1.1)

    assert len(mgr.list_backups()) == 5

    deleted = mgr.prune_old_backups(keep=2)

    assert deleted == 3
    assert len(mgr.list_backups()) == 2


def test_restore_preview(tmp_path):
    """restore_preview should return the member filenames without extracting."""
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    archive = _make_tarball(
        backup_dir,
        "hazel-backup-2026-01-01-000000.tar.gz",
        [("queue.db", "db data"), ("hzl_config.yaml", "config data")],
    )

    mgr = _make_manager(tmp_path)
    names = mgr.restore_preview(str(archive))

    assert "queue.db" in names
    assert "hzl_config.yaml" in names
    assert len(names) == 2
