"""
gateway.py — GatewayDaemon for the Gateway Pi.

Ties together RelayController, QueueHub, and ContentScanner into a single
daemon with an aiohttp REST API. The Gateway Pi is the only node with WiFi
access; it fetches external content during a sync window and delivers it
to the air-gapped Core cluster over Ethernet.

REST API (default :9010):
  POST /request      — queue a fetch request (HazelMessage JSON body)
  GET  /queue        — view queue status
  POST /sync         — trigger a sync cycle
  GET  /state        — relay + queue + staging state
  GET  /staging      — list staged files
  GET  /health       — {"status": "ok", "role": "gateway"}
  POST /lock         — lock the relay
  POST /unlock       — unlock the relay
  POST /emergency    — emergency disconnect
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import List

from aiohttp import web

from hzl_cluster.queue_hub import HazelMessage, QueueHub
from hzl_cluster.relay import RelayController, RelayState
from hzl_cluster.scanner import ContentScanner


def _safe_import(module_path: str, func_name: str):
    """Import a fetcher function, return None if not available."""
    try:
        mod = __import__(module_path, fromlist=[func_name])
        return getattr(mod, func_name)
    except (ImportError, AttributeError):
        return None


# Core fetchers — always expected to exist; loaded at import time for
# performance, but guarded so a missing module doesn't crash the gateway.
fetch_weather  = _safe_import("hzl_cluster.fetchers.weather_fetcher", "fetch_weather")
fetch_news     = _safe_import("hzl_cluster.fetchers.news_fetcher",    "fetch_news")
fetch_email    = _safe_import("hzl_cluster.fetchers.email_fetcher",   "fetch_email")

logger = logging.getLogger("hzl.gateway")

GATEWAY_HOST = os.environ.get("GATEWAY_HOST", "0.0.0.0")
GATEWAY_PORT = int(os.environ.get("GATEWAY_PORT", "9010"))


class GatewayDaemon:
    """Main daemon for the Gateway Pi."""

    def __init__(self, config: dict, simulate: bool = False) -> None:
        self.relay = RelayController(config, simulate=simulate)
        self.queue = QueueHub(config)

        sync_cfg = config.get("sync", {})
        staging_dir    = sync_cfg.get("staging_dir", "/tmp/hzl_staging")
        quarantine_dir = sync_cfg.get("quarantine_dir", "/tmp/hzl_quarantine")
        max_size_mb    = float(sync_cfg.get("max_staging_size_mb", 500))

        self.scanner = ContentScanner(
            staging_dir=staging_dir,
            quarantine_dir=quarantine_dir,
            max_file_size_mb=max_size_mb,
        )
        self.staging_dir = staging_dir

    # ------------------------------------------------------------------
    # Queue
    # ------------------------------------------------------------------

    def queue_request(self, msg: HazelMessage) -> dict:
        """Ingest a single HazelMessage into the queue."""
        return self.queue.ingest([msg])

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    def get_state(self) -> dict:
        """Return combined relay, queue, and staging state."""
        return {
            "relay":   self.relay.state_dict(),
            "queue":   self.queue.status(),
            "staging": self.list_staging(),
        }

    def list_staging(self) -> List[dict]:
        """List files in the staging directory."""
        entries: List[dict] = []
        try:
            for filename in os.listdir(self.staging_dir):
                full_path = os.path.join(self.staging_dir, filename)
                if os.path.isfile(full_path):
                    stat = os.stat(full_path)
                    entries.append({
                        "name":     filename,
                        "size":     stat.st_size,
                        "modified": stat.st_mtime,
                    })
        except OSError:
            pass
        return entries

    # ------------------------------------------------------------------
    # Sync cycle
    # ------------------------------------------------------------------

    async def run_sync_cycle(self) -> dict:
        """
        Core sync cycle:
          1. Enter internet mode (relay opens, WiFi comes up).
          2. Drain the gateway-bound outbound queue (real fetchers come later).
          3. Scan staging directory; quarantine unsafe files.
          4. Return to core mode (WiFi down, relay closes).

        Returns counts of fetched, scanned, quarantined, and delivered items.
        """
        fetched    = 0
        scanned    = 0
        quarantined = 0
        delivered  = 0

        # Step 1: connect to internet
        await self.relay.enter_internet_mode(reason="sync_cycle")

        # Step 2: process outbound queue — dispatch to real fetchers
        messages = self.queue.get_outbound("gateway")
        for msg in messages:
            try:
                result = await self._dispatch_fetch(msg)
                if result.get("success"):
                    self.queue.ack(msg.id)
                    fetched += 1
                    delivered += 1
                else:
                    self.queue.fail(msg.id, result.get("summary", "fetch failed"))
                    logger.warning(f"Fetch failed for {msg.action}: {result}")
            except Exception as e:
                self.queue.fail(msg.id, str(e))
                logger.error(f"Fetch error for {msg.action}: {e}")

        # Step 3: scan staging directory
        results = self.scanner.scan_directory(self.staging_dir)
        for result in results:
            scanned += 1
            if not result.safe:
                # quarantine_and_log: move the file
                self.scanner.scan_and_quarantine(result.path)
                quarantined += 1

        # Step 4: return to core
        await self.relay.enter_core_mode(reason="sync_cycle")

        return {
            "fetched":     fetched,
            "scanned":     scanned,
            "quarantined": quarantined,
            "delivered":   delivered,
        }

    async def _dispatch_fetch(self, msg: HazelMessage) -> dict:
        """Dispatch a fetch message to the appropriate fetcher."""
        action = msg.action
        payload = msg.payload
        staging = self.staging_dir
        simulate = self.relay._simulate  # use relay's simulate flag

        if action == "fetch.weather":
            if fetch_weather:
                return fetch_weather(
                    staging_dir=staging,
                    latitude=payload.get("latitude", 40.7128),
                    longitude=payload.get("longitude", -74.0060),
                    days=payload.get("days", 3),
                    simulate=simulate,
                )
            return {"success": True, "summary": "weather fetcher not installed"}
        elif action == "fetch.news":
            if fetch_news:
                return fetch_news(
                    staging_dir=staging,
                    feeds=payload.get("feeds"),
                    max_articles_per_feed=payload.get("max_articles", 10),
                    simulate=simulate,
                )
            return {"success": True, "summary": "news fetcher not installed"}
        elif action == "fetch.email":
            if fetch_email:
                return fetch_email(
                    staging_dir=staging,
                    imap_host=payload.get("imap_host", "127.0.0.1"),
                    imap_port=payload.get("imap_port", 1143),
                    username=payload.get("username", ""),
                    password=payload.get("password", ""),
                    folder=payload.get("folder", "INBOX"),
                    since_days=payload.get("since_days", 3),
                    max_emails=payload.get("max_emails", 50),
                    use_ssl=payload.get("use_ssl", False),
                    simulate=simulate,
                )
            return {"success": True, "summary": "email fetcher not installed"}
        elif action == "fetch.podcast":
            _fetch_podcasts = _safe_import(
                "hzl_cluster.fetchers.podcast_fetcher", "fetch_podcasts"
            )
            if _fetch_podcasts:
                return _fetch_podcasts(
                    staging_dir=staging,
                    feeds=payload.get("feeds"),
                    max_episodes=payload.get("max_episodes", 3),
                    simulate=simulate,
                )
            return {"success": True, "summary": "podcast fetcher not installed"}
        elif action == "fetch.maps":
            _fetch_maps = _safe_import(
                "hzl_cluster.fetchers.maps_fetcher", "fetch_maps"
            )
            if _fetch_maps:
                return _fetch_maps(
                    staging_dir=staging,
                    locations=payload.get("locations"),
                    zoom=payload.get("zoom", 13),
                    simulate=simulate,
                )
            return {"success": True, "summary": "maps fetcher not installed"}
        elif action == "fetch.packages":
            _fetch_packages = _safe_import(
                "hzl_cluster.fetchers.package_fetcher", "fetch_packages"
            )
            if _fetch_packages:
                return _fetch_packages(
                    staging_dir=staging,
                    packages=payload.get("packages"),
                    simulate=simulate,
                )
            return {"success": True, "summary": "packages fetcher not installed"}
        elif action == "fetch.calendar":
            _fetch_calendar = _safe_import(
                "hzl_cluster.fetchers.calendar_fetcher", "fetch_calendar"
            )
            if _fetch_calendar:
                return _fetch_calendar(
                    staging_dir=staging,
                    calendar_urls=payload.get("calendar_urls"),
                    days_ahead=payload.get("days_ahead", 7),
                    simulate=simulate,
                )
            return {"success": True, "summary": "calendar fetcher not installed"}
        elif action == "fetch.url":
            logger.info("URL fetch requested but fetcher not yet implemented")
            return {"success": True, "summary": "url fetch queued (fetcher pending)"}
        elif action.startswith("send."):
            logger.info(f"Send action {action} requested but sender not yet implemented")
            return {"success": True, "summary": f"{action} queued (sender pending)"}
        else:
            logger.warning(f"Unknown action: {action}")
            return {"success": True, "summary": f"unknown action {action} — acked"}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Shut down the queue hub (closes DB connection)."""
        self.queue.close()

    # ------------------------------------------------------------------
    # REST API
    # ------------------------------------------------------------------

    def start_api(self) -> web.Application:
        """Build and return the aiohttp application."""
        app = web.Application()
        app["daemon"] = self

        app.router.add_post("/request",   _handle_request)
        app.router.add_get( "/queue",     _handle_queue)
        app.router.add_post("/sync",      _handle_sync)
        app.router.add_get( "/state",     _handle_state)
        app.router.add_get( "/staging",   _handle_staging)
        app.router.add_get( "/health",    _handle_health)
        app.router.add_post("/lock",      _handle_lock)
        app.router.add_post("/unlock",    _handle_unlock)
        app.router.add_post("/emergency", _handle_emergency)

        return app


# ──────────────────────────────────────────────────────────────
# Route handlers
# ──────────────────────────────────────────────────────────────

async def _handle_request(request: web.Request) -> web.Response:
    daemon: GatewayDaemon = request.app["daemon"]
    try:
        data = await request.json()
        msg = HazelMessage.from_dict(data)
    except Exception as exc:
        return web.json_response({"error": str(exc)}, status=400)
    result = daemon.queue_request(msg)
    return web.json_response(result)


async def _handle_queue(request: web.Request) -> web.Response:
    daemon: GatewayDaemon = request.app["daemon"]
    return web.json_response(daemon.queue.status())


async def _handle_sync(request: web.Request) -> web.Response:
    daemon: GatewayDaemon = request.app["daemon"]
    result = await daemon.run_sync_cycle()
    return web.json_response(result)


async def _handle_state(request: web.Request) -> web.Response:
    daemon: GatewayDaemon = request.app["daemon"]
    return web.json_response(daemon.get_state())


async def _handle_staging(request: web.Request) -> web.Response:
    daemon: GatewayDaemon = request.app["daemon"]
    return web.json_response(daemon.list_staging())


async def _handle_health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "role": "gateway"})


async def _handle_lock(request: web.Request) -> web.Response:
    daemon: GatewayDaemon = request.app["daemon"]
    daemon.relay.lock()
    return web.json_response({"state": daemon.relay.state.value})


async def _handle_unlock(request: web.Request) -> web.Response:
    daemon: GatewayDaemon = request.app["daemon"]
    daemon.relay.unlock()
    return web.json_response({"state": daemon.relay.state.value})


async def _handle_emergency(request: web.Request) -> web.Response:
    daemon: GatewayDaemon = request.app["daemon"]
    await daemon.relay.emergency_disconnect()
    return web.json_response({"state": daemon.relay.state.value})


# ──────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────

def main() -> None:
    """Load config, create daemon, start REST API."""
    import yaml  # type: ignore

    config_path = os.environ.get("HZL_CONFIG", "example_config.yaml")
    with open(config_path) as fh:
        config = yaml.safe_load(fh)

    daemon = GatewayDaemon(config)
    app = daemon.start_api()

    logger.info(f"[GatewayDaemon] Starting API on {GATEWAY_HOST}:{GATEWAY_PORT}")
    web.run_app(app, host=GATEWAY_HOST, port=GATEWAY_PORT)


if __name__ == "__main__":
    main()
