"""
ContentScanner — Gateway Pi defense layer.

Validates files fetched from the internet before they are delivered
to the air-gapped Core cluster. Checks file extension, PE magic bytes,
and size. Files that fail are moved to a quarantine directory.
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass

BLOCKED_EXTENSIONS = {
    ".exe", ".msi", ".bat", ".cmd", ".com", ".scr", ".pif",
    ".vbs", ".vbe", ".js", ".jse", ".wsf", ".wsh", ".ps1",
    ".dll", ".sys", ".drv", ".cpl", ".sh", ".bash", ".zsh",
}

PE_MAGIC = b"MZ"


@dataclass
class ScanResult:
    path: str
    safe: bool
    reason: str


class ContentScanner:
    def __init__(self, staging_dir: str, quarantine_dir: str, max_file_size_mb: float = 500):
        self.staging_dir = staging_dir
        self.quarantine_dir = quarantine_dir
        self.max_file_size_bytes = int(max_file_size_mb * 1024 * 1024)

        os.makedirs(self.staging_dir, exist_ok=True)
        os.makedirs(self.quarantine_dir, exist_ok=True)

    def scan_file(self, path: str) -> ScanResult:
        _, ext = os.path.splitext(path)
        if ext.lower() in BLOCKED_EXTENSIONS:
            return ScanResult(path=path, safe=False, reason=f"blocked extension: {ext.lower()}")

        try:
            with open(path, "rb") as fh:
                magic = fh.read(2)
        except OSError as exc:
            return ScanResult(path=path, safe=False, reason=f"unreadable file: {exc}")

        if magic == PE_MAGIC:
            return ScanResult(path=path, safe=False, reason="blocked: PE executable magic bytes (MZ)")

        try:
            file_size = os.path.getsize(path)
        except OSError as exc:
            return ScanResult(path=path, safe=False, reason=f"cannot stat file: {exc}")

        if file_size > self.max_file_size_bytes:
            size_mb = file_size / (1024 * 1024)
            return ScanResult(
                path=path,
                safe=False,
                reason=f"blocked: file size {size_mb:.1f} MB exceeds limit of {self.max_file_size_bytes / (1024 * 1024):.0f} MB",
            )

        return ScanResult(path=path, safe=True, reason="clean")

    def scan_and_quarantine(self, path: str) -> ScanResult:
        result = self.scan_file(path)
        if not result.safe:
            filename = os.path.basename(path)
            dest = os.path.join(self.quarantine_dir, filename)
            shutil.move(path, dest)
        return result

    def scan_directory(self, directory: str) -> list[ScanResult]:
        results: list[ScanResult] = []
        for root, _dirs, files in os.walk(directory):
            for filename in files:
                full_path = os.path.join(root, filename)
                results.append(self.scan_file(full_path))
        return results
