"""
Package fetcher — downloads pip packages as wheel files for offline installation
on the air-gapped cluster. Uses `pip download` subprocess to pull wheels into
staging/packages/, then saves a manifest.json with package names, versions, and
file sizes.
"""
import json
import logging
import os
import subprocess
import sys
from datetime import datetime
from typing import Optional

logger = logging.getLogger("hzl.fetcher.package")

# Default packages needed on cluster nodes
DEFAULT_PACKAGES = [
    "requests",
    "pyyaml",
    "psutil",
    "flask",
    "pydantic",
]


def fetch_packages(
    staging_dir: str,
    packages: Optional[list] = None,
    simulate: bool = False,
) -> dict:
    """
    Download pip packages as wheels for offline cluster installation.

    In simulate mode, writes a realistic manifest without invoking pip.

    Returns: {"success": bool, "manifest": str or None, "packages": list, "summary": str}
    """
    if packages is None:
        packages = DEFAULT_PACKAGES

    packages_dir = os.path.join(staging_dir, "packages")
    os.makedirs(packages_dir, exist_ok=True)

    if simulate:
        # Produce realistic fake manifest without hitting the network or pip
        fake_packages = []
        for pkg in packages:
            fake_packages.append({
                "name": pkg,
                "version": "1.0.0",
                "filename": f"{pkg}-1.0.0-py3-none-any.whl",
                "size_bytes": 102400,
                "path": os.path.join(packages_dir, f"{pkg}-1.0.0-py3-none-any.whl"),
            })

        manifest = {
            "fetched_at": datetime.now().isoformat(),
            "simulate": True,
            "packages": fake_packages,
            "total_packages": len(fake_packages),
            "total_size_bytes": sum(p["size_bytes"] for p in fake_packages),
        }
        manifest_path = os.path.join(packages_dir, "manifest.json")
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        summary = f"{len(fake_packages)} packages staged (simulate)"
        logger.info(f"Package fetch simulated: {summary}")
        return {
            "success": True,
            "manifest": manifest_path,
            "packages": fake_packages,
            "summary": summary,
        }

    # Real download via pip download
    cmd = [
        sys.executable, "-m", "pip", "download",
        "--dest", packages_dir,
        "--no-deps",          # fetch explicit list only; caller controls transitive deps
        "--prefer-binary",    # prefer wheels over source tarballs
    ] + packages

    logger.info(f"Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
        )
    except subprocess.TimeoutExpired:
        logger.error("pip download timed out after 300s")
        return {"success": False, "manifest": None, "packages": [], "summary": "Timed out"}
    except OSError as e:
        logger.error(f"pip download failed to start: {e}")
        return {"success": False, "manifest": None, "packages": [], "summary": str(e)}

    if result.returncode != 0:
        logger.error(f"pip download exited {result.returncode}: {result.stderr.strip()}")
        return {
            "success": False,
            "manifest": None,
            "packages": [],
            "summary": f"pip exited {result.returncode}",
        }

    # Build manifest from what actually landed on disk
    downloaded = []
    for filename in sorted(os.listdir(packages_dir)):
        if not (filename.endswith(".whl") or filename.endswith(".tar.gz")):
            continue
        filepath = os.path.join(packages_dir, filename)
        size = os.path.getsize(filepath)
        # Parse name from wheel filename (name-version-...)
        parts = filename.split("-")
        name = parts[0] if parts else filename
        version = parts[1] if len(parts) > 1 else "unknown"
        downloaded.append({
            "name": name,
            "version": version,
            "filename": filename,
            "size_bytes": size,
            "path": filepath,
        })

    # Verify every requested package has at least one matching file
    missing = []
    for pkg in packages:
        pkg_lower = pkg.lower().replace("-", "_")
        found = any(
            p["name"].lower().replace("-", "_") == pkg_lower
            for p in downloaded
        )
        if not found:
            missing.append(pkg)
            logger.warning(f"Package not found after download: {pkg}")

    manifest = {
        "fetched_at": datetime.now().isoformat(),
        "simulate": False,
        "packages": downloaded,
        "missing": missing,
        "total_packages": len(downloaded),
        "total_size_bytes": sum(p["size_bytes"] for p in downloaded),
    }
    manifest_path = os.path.join(packages_dir, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    success = len(missing) == 0
    summary = (
        f"{len(downloaded)} packages downloaded"
        + (f", {len(missing)} missing" if missing else "")
    )
    logger.info(f"Package fetch complete: {summary}")
    return {
        "success": success,
        "manifest": manifest_path,
        "packages": downloaded,
        "summary": summary,
    }
