"""
backup.py — Encrypted snapshots of critical cluster data.

Backs up queue DB, config, audit logs, and Hazel memory to a target directory.
Uses tar + gzip. Optional GPG encryption.
"""

from __future__ import annotations

import os
import subprocess
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# Default sources relative to working directory (or absolute paths acceptable)
DEFAULT_SOURCES = [
    "queue.db",
    "hzl_config.yaml",
    "relay_audit.log",
    "hazel.db",
]


class BackupManager:
    def __init__(self, config: dict):
        backup_cfg = config.get("backup", {})
        self._target_dir = Path(backup_cfg.get("target_dir", "/tmp/hzl-backups"))
        self._encrypt: bool = backup_cfg.get("encrypt", False)
        self._gpg_recipient: Optional[str] = backup_cfg.get("gpg_recipient", None)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def create_backup(self, sources: list[str] = None) -> dict:
        """Create a compressed tarball of source files.

        Args:
            sources: Paths to include. Defaults to DEFAULT_SOURCES.

        Returns:
            dict with keys: success, file, size_bytes, sources, encrypted.
        """
        if sources is None:
            sources = DEFAULT_SOURCES

        self._target_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d-%H%M%S")
        archive_name = f"hazel-backup-{timestamp}.tar.gz"
        archive_path = self._target_dir / archive_name

        # Only include sources that actually exist
        existing = [s for s in sources if Path(s).exists()]

        try:
            cmd = ["tar", "-czf", str(archive_path)] + existing
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                return {
                    "success": False,
                    "file": "",
                    "size_bytes": 0,
                    "sources": 0,
                    "encrypted": False,
                }
        except FileNotFoundError:
            # tar not available — fall back to stdlib tarfile
            with tarfile.open(archive_path, "w:gz") as tar:
                for src in existing:
                    tar.add(src)

        encrypted = False
        final_path = archive_path

        if self._encrypt and self._gpg_recipient:
            encrypted_path = Path(str(archive_path) + ".gpg")
            gpg_cmd = [
                "gpg",
                "--batch",
                "--yes",
                "--recipient", self._gpg_recipient,
                "--output", str(encrypted_path),
                "--encrypt", str(archive_path),
            ]
            gpg_result = subprocess.run(gpg_cmd, capture_output=True, text=True)
            if gpg_result.returncode == 0:
                archive_path.unlink(missing_ok=True)
                final_path = encrypted_path
                encrypted = True

        size = final_path.stat().st_size if final_path.exists() else 0

        return {
            "success": final_path.exists(),
            "file": str(final_path),
            "size_bytes": size,
            "sources": len(existing),
            "encrypted": encrypted,
        }

    def list_backups(self) -> list[dict]:
        """List all backup files in target_dir.

        Returns:
            List of dicts with keys: name, path, size_bytes, date (ISO string).
        """
        if not self._target_dir.exists():
            return []

        backups = []
        for entry in sorted(self._target_dir.iterdir(), key=lambda p: p.stat().st_mtime):
            if entry.name.startswith("hazel-backup-") and entry.is_file():
                stat = entry.stat()
                backups.append({
                    "name": entry.name,
                    "path": str(entry),
                    "size_bytes": stat.st_size,
                    "date": datetime.fromtimestamp(
                        stat.st_mtime, tz=timezone.utc
                    ).isoformat(),
                })
        return backups

    def verify_backup(self, path: str) -> dict:
        """Verify integrity of a backup tarball.

        Attempts to open and list contents. Does not decrypt GPG archives.

        Returns:
            dict with keys: success, path, file_count, error.
        """
        p = Path(path)
        if not p.exists():
            return {"success": False, "path": path, "file_count": 0, "error": "file not found"}

        if str(path).endswith(".gpg"):
            # Cannot verify encrypted archives without a key present
            return {"success": True, "path": path, "file_count": 0, "error": "encrypted — skipped"}

        try:
            with tarfile.open(path, "r:gz") as tar:
                members = tar.getmembers()
            return {
                "success": True,
                "path": path,
                "file_count": len(members),
                "error": None,
            }
        except Exception as exc:
            return {
                "success": False,
                "path": path,
                "file_count": 0,
                "error": str(exc),
            }

    def prune_old_backups(self, keep: int = 7) -> int:
        """Delete oldest backups beyond the keep count.

        Args:
            keep: Number of most recent backups to retain.

        Returns:
            Number of deleted backup files.
        """
        backups = self.list_backups()
        to_delete = backups[: max(0, len(backups) - keep)]
        deleted = 0
        for entry in to_delete:
            try:
                Path(entry["path"]).unlink(missing_ok=True)
                deleted += 1
            except OSError:
                pass
        return deleted

    def restore_preview(self, path: str) -> list[str]:
        """List files inside a backup without extracting them.

        Args:
            path: Path to the .tar.gz backup file.

        Returns:
            List of member filenames, or empty list on error.
        """
        try:
            with tarfile.open(path, "r:gz") as tar:
                return tar.getnames()
        except Exception:
            return []
