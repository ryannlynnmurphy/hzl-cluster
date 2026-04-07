import json
import os
import tempfile
import pytest
from hzl_cluster.fetchers.url_fetcher import fetch_url, _sanitize_filename


class TestUrlFetcher:
    def setup_method(self):
        self.staging = tempfile.mkdtemp()

    # 1. simulate mode returns a well-formed result dict
    def test_simulate_returns_data(self):
        result = fetch_url("https://example.com/page.html", self.staging, simulate=True)
        assert result["success"] is True
        assert result["url"] == "https://example.com/page.html"
        assert result["content_type"] is not None
        assert result["size_bytes"] > 0
        assert result["path"] is not None
        assert result["metadata"] is not None
        assert os.path.exists(result["metadata"])

    # 2. simulate mode writes a valid metadata.json
    def test_simulate_writes_metadata(self):
        result = fetch_url("https://example.com/report.pdf", self.staging, simulate=True)
        with open(result["metadata"]) as f:
            meta = json.load(f)
        assert meta["url"] == "https://example.com/report.pdf"
        assert "content_type" in meta
        assert "size_bytes" in meta
        assert "fetch_time" in meta
        assert meta["simulate"] is True
        assert meta["size_bytes"] > 0

    # 3. staging/downloads sub-directory is created on demand
    def test_staging_dir_created(self):
        new_dir = os.path.join(self.staging, "deep", "nested")
        result = fetch_url("https://example.com/file.txt", new_dir, simulate=True)
        assert result["success"] is True
        downloads_dir = os.path.join(new_dir, "downloads")
        assert os.path.isdir(downloads_dir)

    # 4. filenames are sanitized — path traversal and special chars removed
    def test_filename_sanitization(self):
        assert _sanitize_filename("../../etc/passwd") == "passwd"
        assert _sanitize_filename("my file (2).pdf") == "my_file__2_.pdf"
        assert _sanitize_filename("../../") == "download"
        assert _sanitize_filename("normal-name_v2.tar.gz") == "normal-name_v2.tar.gz"
        assert _sanitize_filename("C:\\Windows\\System32\\evil.exe") == "evil.exe"

    # 5. simulate works with a custom URL and preserves it in metadata
    def test_simulate_custom_url(self):
        custom_url = "https://data.hzl.studio/assets/cluster-map.json"
        result = fetch_url(custom_url, self.staging, simulate=True)
        assert result["success"] is True
        assert result["url"] == custom_url
        with open(result["metadata"]) as f:
            meta = json.load(f)
        assert meta["url"] == custom_url
        assert meta["filename"] == "cluster-map.json"
