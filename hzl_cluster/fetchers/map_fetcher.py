"""
Map tile fetcher — downloads OpenStreetMap tiles for offline use.
Saves PNG tiles organized by zoom/x/y. Respects OSM tile usage policy.
"""
import json
import logging
import math
import os
import time
from datetime import datetime
from typing import List, Optional, Tuple
from urllib.request import urlopen, Request
from urllib.error import URLError

logger = logging.getLogger("hzl.fetcher.map")

OSM_TILE_URL = "https://tile.openstreetmap.org"
USER_AGENT = "HazelOS/1.0 (https://hzlstudio.com; offline-map-cache)"

# OSM tile usage policy: max 2 req/sec from a single IP, reasonable total load
REQUEST_DELAY_SECONDS = 0.6


def _lat_lon_to_tile(lat: float, lon: float, zoom: int) -> Tuple[int, int]:
    """Convert lat/lon to OSM slippy map tile x/y at a given zoom level."""
    lat_r = math.radians(lat)
    n = 2 ** zoom
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.log(math.tan(lat_r) + 1.0 / math.cos(lat_r)) / math.pi) / 2.0 * n)
    return x, y


def _tiles_for_bbox(
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    zoom: int,
) -> List[Tuple[int, int, int]]:
    """Return list of (zoom, x, y) tile tuples covering the bounding box."""
    x_min, y_max = _lat_lon_to_tile(lat_min, lon_min, zoom)  # SW corner (y flipped)
    x_max, y_min = _lat_lon_to_tile(lat_max, lon_max, zoom)  # NE corner

    # Clamp to valid tile range
    n = 2 ** zoom
    x_min = max(0, min(x_min, n - 1))
    x_max = max(0, min(x_max, n - 1))
    y_min = max(0, min(y_min, n - 1))
    y_max = max(0, min(y_max, n - 1))

    tiles = []
    for x in range(x_min, x_max + 1):
        for y in range(y_min, y_max + 1):
            tiles.append((zoom, x, y))
    return tiles


def fetch_maps(
    staging_dir: str,
    lat_min: float = 40.70,   # NYC default bounding box
    lat_max: float = 40.73,
    lon_min: float = -74.02,
    lon_max: float = -73.97,
    zoom_levels: Optional[List[int]] = None,
    simulate: bool = False,
) -> dict:
    """
    Download OSM tiles for a bounding box at specified zoom levels.

    Tiles saved to: {staging_dir}/maps/{zoom}/{x}/{y}.png
    Manifest saved to: {staging_dir}/maps/manifest.json

    Returns: {"success": bool, "manifest": str or None, "tile_count": int, "summary": str}
    """
    if zoom_levels is None:
        zoom_levels = [12, 14]

    maps_dir = os.path.join(staging_dir, "maps")
    os.makedirs(maps_dir, exist_ok=True)

    # Collect all tile coordinates across zoom levels
    all_tiles = []
    for zoom in zoom_levels:
        all_tiles.extend(_tiles_for_bbox(lat_min, lat_max, lon_min, lon_max, zoom))

    manifest = {
        "fetched_at": datetime.now().isoformat(),
        "bbox": {
            "lat_min": lat_min,
            "lat_max": lat_max,
            "lon_min": lon_min,
            "lon_max": lon_max,
        },
        "zoom_levels": zoom_levels,
        "tile_count": len(all_tiles),
        "tiles": [{"zoom": z, "x": x, "y": y} for z, x, y in all_tiles],
        "simulated": simulate,
    }

    if simulate:
        # Write manifest only — no real network requests
        manifest_path = os.path.join(maps_dir, "manifest.json")
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        summary = f"Simulated {len(all_tiles)} tiles across zoom levels {zoom_levels}"
        logger.info(summary)
        return {
            "success": True,
            "manifest": manifest_path,
            "tile_count": len(all_tiles),
            "summary": summary,
        }

    # Real download
    downloaded = 0
    failed = 0

    for zoom, x, y in all_tiles:
        tile_dir = os.path.join(maps_dir, str(zoom), str(x))
        os.makedirs(tile_dir, exist_ok=True)
        tile_path = os.path.join(tile_dir, f"{y}.png")

        if os.path.exists(tile_path):
            downloaded += 1
            continue

        url = f"{OSM_TILE_URL}/{zoom}/{x}/{y}.png"
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(req, timeout=15) as resp:
                tile_data = resp.read()
            with open(tile_path, "wb") as f:
                f.write(tile_data)
            downloaded += 1
            logger.debug(f"Tile saved: {zoom}/{x}/{y}")
        except (URLError, OSError) as e:
            logger.warning(f"Tile fetch failed {zoom}/{x}/{y}: {e}")
            failed += 1

        time.sleep(REQUEST_DELAY_SECONDS)

    manifest["downloaded"] = downloaded
    manifest["failed"] = failed

    manifest_path = os.path.join(maps_dir, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    success = failed == 0
    summary = f"Downloaded {downloaded}/{len(all_tiles)} tiles (zoom {zoom_levels})"
    if failed:
        summary += f", {failed} failed"
    logger.info(summary)

    return {
        "success": success,
        "manifest": manifest_path,
        "tile_count": len(all_tiles),
        "summary": summary,
    }
