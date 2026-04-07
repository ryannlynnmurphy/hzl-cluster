"""
test_file_sync.py — Tests for FileSyncManager.

All tests use pytest's tmp_path fixture so nothing touches real cluster data.
"""

import hashlib
import json
from pathlib import Path

import pytest

from hzl_cluster.file_sync import FileSyncManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _make_manager(tmp_path: Path, subdir: str = "sync") -> FileSyncManager:
    return FileSyncManager(str(tmp_path / subdir))


def _write(base: Path, rel: str, content: str) -> Path:
    """Write a file inside base and return its absolute path."""
    target = base / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    return target


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_scan_directory(tmp_path):
    """scan() returns a manifest with correct SHA-256 hashes for every file."""
    mgr = _make_manager(tmp_path)
    sync = tmp_path / "sync"

    _write(sync, "notes/hello.txt", "hello world")
    _write(sync, "photos/pic.jpg", "fake jpeg bytes")
    _write(sync, "music/track.mp3", "fake mp3 bytes")

    manifest = mgr.scan()

    assert len(manifest) == 3
    assert "notes/hello.txt" in manifest
    assert "photos/pic.jpg" in manifest
    assert "music/track.mp3" in manifest

    for rel, digest in manifest.items():
        file_path = sync / rel
        assert digest == _sha256(file_path.read_bytes()), (
            f"Hash mismatch for {rel}"
        )


def test_hash_file(tmp_path):
    """hash_file() produces the expected SHA-256 for known content."""
    known_content = b"HZL cluster sync test"
    expected = hashlib.sha256(known_content).hexdigest()

    target = tmp_path / "known.bin"
    target.write_bytes(known_content)

    mgr = FileSyncManager(str(tmp_path))
    result = mgr.hash_file(str(target))

    assert result == expected
    assert len(result) == 64  # SHA-256 hex is always 64 chars


def test_diff_new_local_files(tmp_path):
    """diff() puts files that only exist locally into to_push."""
    mgr = _make_manager(tmp_path)

    local = {
        "notes/journal.txt": "aaa",
        "docs/report.pdf": "bbb",
    }
    remote = {}

    result = mgr.diff(local, remote)

    assert sorted(result["to_push"]) == sorted(local.keys())
    assert result["to_pull"] == []
    assert result["conflicts"] == []


def test_diff_new_remote_files(tmp_path):
    """diff() puts files that only exist remotely into to_pull."""
    mgr = _make_manager(tmp_path)

    local = {}
    remote = {
        "photos/sunset.jpg": "ccc",
        "music/album.mp3": "ddd",
    }

    result = mgr.diff(local, remote)

    assert result["to_push"] == []
    assert sorted(result["to_pull"]) == sorted(remote.keys())
    assert result["conflicts"] == []


def test_diff_conflicts(tmp_path):
    """diff() flags files present on both sides with different hashes as conflicts."""
    mgr = _make_manager(tmp_path)

    local = {
        "notes/shared.txt": "hash-A",
        "docs/unchanged.pdf": "same-hash",
    }
    remote = {
        "notes/shared.txt": "hash-B",   # same path, different content
        "docs/unchanged.pdf": "same-hash",  # identical — not a conflict
    }

    result = mgr.diff(local, remote)

    assert result["to_push"] == []
    assert result["to_pull"] == []
    assert result["conflicts"] == ["notes/shared.txt"]


def test_save_and_load_manifest(tmp_path):
    """save_manifest() and load_manifest() round-trip correctly."""
    mgr = _make_manager(tmp_path)

    original = {
        "notes/a.txt": "deadbeef" * 8,
        "photos/b.jpg": "cafebabe" * 8,
    }

    mgr.save_manifest(original)

    # Verify the file actually exists on disk
    assert Path(mgr.manifest_path()).exists()

    loaded = mgr.load_manifest()
    assert loaded == original


def test_resolve_conflict_both(tmp_path):
    """resolve_conflict('both') keeps local and writes a .conflict copy."""
    mgr = _make_manager(tmp_path)
    sync = tmp_path / "sync"

    local_content = b"local version of the file"
    _write(sync, "notes/shared.txt", "local version of the file")

    outcome = mgr.resolve_conflict("notes/shared.txt", strategy="both")

    # The local file must still be intact
    local_file = sync / "notes/shared.txt"
    assert local_file.exists()
    assert local_file.read_bytes() == local_content

    # A .conflict sibling must exist
    conflict_file = sync / "notes/shared.txt.conflict"
    assert conflict_file.exists(), ".conflict file was not created"

    # Outcome string should mention both the original path and .conflict
    assert "notes/shared.txt" in outcome
    assert ".conflict" in outcome
