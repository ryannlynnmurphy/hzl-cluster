"""Tests for ContentScanner (Task 6)."""

import os
import tempfile

import pytest

from hzl_cluster.scanner import ContentScanner


class TestContentScanner:
    def setup_method(self):
        self.staging_dir = tempfile.mkdtemp()
        self.quarantine_dir = tempfile.mkdtemp()
        self.scanner = ContentScanner(
            staging_dir=self.staging_dir,
            quarantine_dir=self.quarantine_dir,
        )

    # ------------------------------------------------------------------
    # 1. Clean text file passes
    # ------------------------------------------------------------------
    def test_scan_clean_text_file(self):
        path = os.path.join(self.staging_dir, "hello.txt")
        with open(path, "w") as fh:
            fh.write("Hello")

        result = self.scanner.scan_file(path)

        assert result.safe is True
        assert result.reason == "clean"

    # ------------------------------------------------------------------
    # 2. Executable blocked by extension + PE magic bytes
    # ------------------------------------------------------------------
    def test_block_executable(self):
        path = os.path.join(self.staging_dir, "evil.exe")
        with open(path, "wb") as fh:
            fh.write(b"MZ\x00\x00fake pe payload")

        result = self.scanner.scan_file(path)

        assert result.safe is False
        # Extension check fires first; "executable" appears in the reason
        assert "executable" in result.reason or "exe" in result.reason

    # ------------------------------------------------------------------
    # 3. Oversized file blocked
    # ------------------------------------------------------------------
    def test_block_oversized_file(self):
        scanner = ContentScanner(
            staging_dir=self.staging_dir,
            quarantine_dir=self.quarantine_dir,
            max_file_size_mb=10,
        )
        path = os.path.join(self.staging_dir, "bigfile.bin")
        # Write 11 MB of zeros
        with open(path, "wb") as fh:
            fh.write(b"\x00" * (11 * 1024 * 1024))

        result = scanner.scan_file(path)

        assert result.safe is False
        assert "size" in result.reason

    # ------------------------------------------------------------------
    # 4. Quarantine moves the unsafe file
    # ------------------------------------------------------------------
    def test_quarantine_moves_file(self):
        path = os.path.join(self.staging_dir, "malware.exe")
        with open(path, "wb") as fh:
            fh.write(b"MZ\x00\x00fake")

        result = self.scanner.scan_and_quarantine(path)

        assert result.safe is False
        # Original file should be gone
        assert not os.path.exists(path)
        # File should exist in quarantine
        quarantined = os.path.join(self.quarantine_dir, "malware.exe")
        assert os.path.exists(quarantined)

    # ------------------------------------------------------------------
    # 5. scan_directory returns correct mix of safe/unsafe results
    # ------------------------------------------------------------------
    def test_scan_directory(self):
        # Two clean files
        for name in ("doc1.txt", "doc2.pdf"):
            p = os.path.join(self.staging_dir, name)
            with open(p, "w") as fh:
                fh.write("safe content")

        # One blocked executable
        exe_path = os.path.join(self.staging_dir, "virus.exe")
        with open(exe_path, "wb") as fh:
            fh.write(b"MZ\x00\x00")

        results = self.scanner.scan_directory(self.staging_dir)

        safe_results = [r for r in results if r.safe]
        unsafe_results = [r for r in results if not r.safe]

        assert len(safe_results) == 2
        assert len(unsafe_results) == 1

    # ------------------------------------------------------------------
    # 6. .json files are allowed
    # ------------------------------------------------------------------
    def test_allow_json(self):
        path = os.path.join(self.staging_dir, "data.json")
        with open(path, "w") as fh:
            fh.write('{"key": "value"}')

        result = self.scanner.scan_file(path)

        assert result.safe is True

    # ------------------------------------------------------------------
    # 7. .eml files are allowed
    # ------------------------------------------------------------------
    def test_allow_eml(self):
        path = os.path.join(self.staging_dir, "message.eml")
        with open(path, "w") as fh:
            fh.write("From: test@example.com\nSubject: Hello\n\nBody text.")

        result = self.scanner.scan_file(path)

        assert result.safe is True
