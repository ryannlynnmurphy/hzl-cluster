import json
import os
import tempfile
import pytest
from hzl_cluster.fetchers.package_fetcher import fetch_packages


class TestPackageFetcher:
    def setup_method(self):
        self.staging = tempfile.mkdtemp()

    # 1. simulate mode returns a well-formed result dict
    def test_simulate_returns_data(self):
        result = fetch_packages(self.staging, simulate=True)
        assert result["success"] is True
        assert result["manifest"] is not None
        assert os.path.exists(result["manifest"])
        assert isinstance(result["packages"], list)
        assert len(result["packages"]) > 0

    # 2. simulate mode writes a valid manifest.json
    def test_simulate_writes_manifest(self):
        result = fetch_packages(self.staging, simulate=True)
        with open(result["manifest"]) as f:
            manifest = json.load(f)
        assert "fetched_at" in manifest
        assert "packages" in manifest
        assert manifest["simulate"] is True
        assert manifest["total_packages"] == len(manifest["packages"])
        assert manifest["total_size_bytes"] > 0

    # 3. staging directory (including packages sub-dir) is created on demand
    def test_staging_dir_created(self):
        new_dir = os.path.join(self.staging, "subdir", "offline")
        result = fetch_packages(new_dir, simulate=True)
        assert result["success"] is True
        packages_dir = os.path.join(new_dir, "packages")
        assert os.path.isdir(packages_dir)

    # 4. each entry in packages list has the expected structure
    def test_simulate_package_structure(self):
        pkgs = ["requests", "pyyaml"]
        result = fetch_packages(self.staging, packages=pkgs, simulate=True)
        assert result["success"] is True
        assert len(result["packages"]) == len(pkgs)
        for entry in result["packages"]:
            assert "name" in entry
            assert "version" in entry
            assert "filename" in entry
            assert "size_bytes" in entry
            assert "path" in entry
            assert entry["filename"].endswith(".whl")
            assert entry["size_bytes"] > 0
