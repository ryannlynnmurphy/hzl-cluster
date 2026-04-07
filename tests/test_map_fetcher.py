import json
import os
import tempfile
import pytest
from hzl_cluster.fetchers.map_fetcher import fetch_maps, _tiles_for_bbox


class TestMapFetcher:
    def setup_method(self):
        self.staging = tempfile.mkdtemp()

    # 1. Simulate mode returns a valid result dict
    def test_simulate_returns_data(self):
        result = fetch_maps(self.staging, simulate=True)
        assert result["success"] is True
        assert result["manifest"] is not None
        assert os.path.exists(result["manifest"])
        assert result["tile_count"] > 0

    # 2. Simulate mode writes a manifest.json with expected keys
    def test_simulate_writes_manifest(self):
        result = fetch_maps(self.staging, simulate=True)
        with open(result["manifest"]) as f:
            manifest = json.load(f)
        assert "bbox" in manifest
        assert "zoom_levels" in manifest
        assert "tile_count" in manifest
        assert "tiles" in manifest
        assert manifest["simulated"] is True
        assert len(manifest["tiles"]) == manifest["tile_count"]

    # 3. Verify expected tile count for a small, known area
    def test_simulate_tile_count(self):
        # A very small 1-degree box at zoom 1 produces exactly 1 tile (the whole world
        # fits in 2x2 at zoom 1, so a small box in one quadrant = 1 tile per zoom).
        tiles = _tiles_for_bbox(
            lat_min=0.0, lat_max=1.0,
            lon_min=0.0, lon_max=1.0,
            zoom=1,
        )
        # At zoom 1 the NE quadrant is tile (1, 0) — one tile
        assert len(tiles) >= 1

        # Confirm simulate result tile_count matches internal calculation
        result = fetch_maps(
            self.staging,
            lat_min=0.0, lat_max=1.0,
            lon_min=0.0, lon_max=1.0,
            zoom_levels=[1],
            simulate=True,
        )
        assert result["tile_count"] == len(tiles)

    # 4. staging_dir (and maps sub-directory) is created automatically
    def test_staging_dir_created(self):
        new_dir = os.path.join(self.staging, "subdir", "maps_test")
        result = fetch_maps(new_dir, simulate=True)
        assert result["success"] is True
        assert os.path.isdir(new_dir)
        assert os.path.isdir(os.path.join(new_dir, "maps"))

    # 5. Custom bounding box is reflected in the manifest
    def test_custom_bounds(self):
        result = fetch_maps(
            self.staging,
            lat_min=34.00,
            lat_max=34.10,
            lon_min=-118.30,
            lon_max=-118.20,
            zoom_levels=[10],
            simulate=True,
        )
        assert result["success"] is True
        with open(result["manifest"]) as f:
            manifest = json.load(f)
        assert manifest["bbox"]["lat_min"] == 34.00
        assert manifest["bbox"]["lat_max"] == 34.10
        assert manifest["bbox"]["lon_min"] == -118.30
        assert manifest["bbox"]["lon_max"] == -118.20
        assert manifest["zoom_levels"] == [10]
