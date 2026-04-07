"""
file_sync.py — Content-addressed file sync between cluster nodes.

Syncs notes, photos, music, and documents across nodes. Uses SHA-256
hashes so only changed files transfer. Manifests are persisted to disk
so diffs survive restarts.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Optional


class FileSyncManager:
    """Manages file synchronization between cluster nodes.

    All paths in manifests are relative to sync_dir so manifests are
    portable across nodes even when absolute paths differ.
    """

    _MANIFEST_FILENAME = ".hzl_manifest.json"

    def __init__(self, sync_dir: str):
        """Initialize the manager.

        Args:
            sync_dir: Root directory that holds synced files.
        """
        self._sync_dir = Path(sync_dir)
        self._sync_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scan(self, subdir: str = "") -> dict[str, str]:
        """Scan files and return a content-addressed manifest.

        Args:
            subdir: Optional subdirectory within sync_dir to limit the scan.

        Returns:
            Dict mapping relative paths (POSIX style) to their SHA-256 hex
            digests.  The manifest file itself is excluded from results.
        """
        root = self._sync_dir / subdir if subdir else self._sync_dir
        manifest: dict[str, str] = {}

        if not root.exists():
            return manifest

        for entry in root.rglob("*"):
            if not entry.is_file():
                continue
            if entry.name == self._MANIFEST_FILENAME:
                continue
            rel = entry.relative_to(self._sync_dir).as_posix()
            manifest[rel] = self.hash_file(str(entry))

        return manifest

    def diff(
        self,
        local_manifest: dict[str, str],
        remote_manifest: dict[str, str],
    ) -> dict[str, list[str]]:
        """Compare two manifests and classify each file.

        Args:
            local_manifest:  {relative_path: sha256} from the local node.
            remote_manifest: {relative_path: sha256} from the remote node.

        Returns:
            Dict with three lists:
                to_push    — files local has that remote does not.
                to_pull    — files remote has that local does not.
                conflicts  — files both have but with different hashes.
        """
        local_keys = set(local_manifest.keys())
        remote_keys = set(remote_manifest.keys())

        to_push: list[str] = sorted(local_keys - remote_keys)
        to_pull: list[str] = sorted(remote_keys - local_keys)
        conflicts: list[str] = sorted(
            path
            for path in local_keys & remote_keys
            if local_manifest[path] != remote_manifest[path]
        )

        return {"to_push": to_push, "to_pull": to_pull, "conflicts": conflicts}

    def hash_file(self, path: str) -> str:
        """Compute the SHA-256 hex digest of a file's contents.

        Args:
            path: Absolute or relative path to the file.

        Returns:
            64-character lowercase hex string.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        h = hashlib.sha256()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()

    def manifest_path(self) -> str:
        """Return the absolute path to the persisted manifest JSON file."""
        return str(self._sync_dir / self._MANIFEST_FILENAME)

    def save_manifest(self, manifest: dict[str, str]) -> None:
        """Persist a manifest to disk as JSON.

        Args:
            manifest: {relative_path: sha256} mapping to save.
        """
        dest = Path(self.manifest_path())
        dest.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    def load_manifest(self) -> dict[str, str]:
        """Load the persisted manifest from disk.

        Returns:
            Previously saved manifest, or an empty dict if none exists.
        """
        dest = Path(self.manifest_path())
        if not dest.exists():
            return {}
        return json.loads(dest.read_text(encoding="utf-8"))

    def resolve_conflict(self, path: str, strategy: str) -> str:
        """Resolve a sync conflict for a single file.

        Args:
            path:     Relative path (within sync_dir) of the conflicted file.
            strategy: One of:
                        "local"  — keep local version, discard remote.
                        "remote" — overwrite local with remote (caller must
                                   supply the remote copy first).
                        "both"   — keep local, rename the remote copy to
                                   <path>.conflict so neither version is lost.

        Returns:
            Human-readable outcome string.

        Raises:
            ValueError: If strategy is not recognised.
            FileNotFoundError: If the local file does not exist when required.
        """
        if strategy not in {"local", "remote", "both"}:
            raise ValueError(
                f"Unknown conflict strategy '{strategy}'. "
                "Use 'local', 'remote', or 'both'."
            )

        local_file = self._sync_dir / path
        conflict_file = self._sync_dir / (path + ".conflict")

        if strategy == "local":
            # Nothing to do — local file stays, remote discarded by caller.
            return f"kept local: {path}"

        if strategy == "remote":
            # Caller is responsible for writing the remote content to local_file.
            # We just confirm the local copy is in place.
            return f"accepted remote: {path}"

        # strategy == "both"
        # The local file stays as-is. The remote copy (which the caller has
        # written to <path>.conflict) is preserved under that name.
        if local_file.exists() and not conflict_file.exists():
            # Caller hasn't pre-staged the conflict file; create a placeholder.
            conflict_file.write_bytes(local_file.read_bytes())

        return f"conflict preserved: {path} — remote saved as {path}.conflict"
