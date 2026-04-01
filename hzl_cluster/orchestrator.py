"""
hzl_orchestrator.py  v2
------------------------
HZL Orchestrator — REST API on port 9000.
hzl_ws.py hits /route before every Claude call.
/outcome closes the feedback loop for circuit breakers.
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
import uuid
from typing import Optional

from aiohttp import web

from hzl_cluster.network import HZLNetwork, load_config
from hzl_cluster.router import HZLRouter, classify_task, RoutingDecision

# ─────────────────────────────────────────────────────────────
# Structured logging
# ─────────────────────────────────────────────────────────────

class StructuredFormatter(logging.Formatter):
    _hostname: Optional[str] = None

    def format(self, record: logging.LogRecord) -> str:
        import socket as _sock
        if not self._hostname:
            StructuredFormatter._hostname = _sock.gethostname()

        data: dict = {
            "ts":       self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level":    record.levelname,
            "logger":   record.name,
            "msg":      record.getMessage(),
            "host":     self._hostname,
        }
        if hasattr(record, "request_id"):
            data["request_id"] = record.request_id
        if record.exc_info:
            data["exc"] = self.formatException(record.exc_info)
        return json.dumps(data)


def setup_logging(config: dict) -> None:
    log_cfg   = config.get("logging", {})
    level_str = log_cfg.get("level", "INFO").upper()
    fmt       = log_cfg.get("format", "json")
    log_dir   = config.get("paths", {}).get("log_dir", "./logs")

    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "hzl_orchestrator.log")

    formatter = (
        StructuredFormatter()
        if fmt == "json"
        else logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")
    )

    handlers = [
        logging.StreamHandler(),
        logging.FileHandler(log_file),
    ]
    for h in handlers:
        h.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(getattr(logging, level_str, logging.INFO))
    for h in handlers:
        root.addHandler(h)


logger = logging.getLogger("hzl.orchestrator")

ORCHESTRATOR_PORT = int(os.environ.get("HZL_ORCH_PORT", 9000))
ORCHESTRATOR_HOST = os.environ.get("HZL_ORCH_HOST", "127.0.0.1")


# ─────────────────────────────────────────────────────────────
# Middleware
# ─────────────────────────────────────────────────────────────

@web.middleware
async def request_id_middleware(request: web.Request, handler) -> web.Response:
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())[:8]
    request["request_id"] = request_id
    response = await handler(request)
    response.headers["X-Request-ID"] = request_id
    return response


@web.middleware
async def timing_middleware(request: web.Request, handler) -> web.Response:
    start = time.monotonic()
    try:
        response = await handler(request)
        status = response.status
    except web.HTTPException as exc:
        status = exc.status
        raise
    finally:
        duration_ms = round((time.monotonic() - start) * 1000, 1)
        logger.info(
            f"{request.method} {request.path} {status} {duration_ms}ms",
            extra={"request_id": request.get("request_id", "-")},
        )
    return response


# ─────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────

class HZLOrchestrator:
    def __init__(self, config: dict):
        self.config     = config
        self.network    = HZLNetwork(config)
        self.router     = HZLRouter(config, self.network)
        self.start_time = time.monotonic()
        self._runner: Optional[web.AppRunner] = None
        self._stopping  = False

        self.network.on_node_event(
            lambda ev, node: logger.info(f"[Topology] {ev.value}: {node.hostname} ({node.ip})")
        )

    # ── Handlers ──────────────────────────────────────────────

    async def handle_route(self, request: web.Request) -> web.Response:
        """POST /route — get routing decision for a message."""
        rid = request.get("request_id", "-")
        try:
            body = await request.json()
            text      = body.get("text", "")
            task_type = body.get("task_type")

            t0 = time.monotonic()
            if task_type:
                decision = await self.router.route(task_type, is_task_type=True)
            else:
                decision = await self.router.route(text)
            latency_ms = (time.monotonic() - t0) * 1000

            logger.debug(
                f"[Route] task={decision.task_type} "
                f"-> {decision.node.hostname if decision.node else 'CLOUD'} "
                f"model={decision.model} "
                f"local={decision.local} latency={latency_ms:.1f}ms",
                extra={"request_id": rid},
            )

            return web.json_response(decision.to_dict())

        except Exception as e:
            logger.error(f"[Route] Error: {e}", exc_info=True, extra={"request_id": rid})
            return web.json_response({"error": str(e)}, status=500)

    async def handle_classify(self, request: web.Request) -> web.Response:
        """POST /classify -- classify without routing (debugging)."""
        try:
            body = await request.json()
            text = body.get("text", "")
            return web.json_response({
                "task_type": classify_task(text),
                "preview": text[:100],
            })
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def handle_outcome(self, request: web.Request) -> web.Response:
        """
        POST /outcome
        { "hostname": "...", "success": true, "latency_ms": 340, "task_type": "reasoning" }

        Updates per-node circuit breaker and latency metrics.
        Called by hzl_ws_integration after each Claude API response.
        """
        rid = request.get("request_id", "-")
        try:
            body = await request.json()
            hostname   = body.get("hostname")
            success    = bool(body.get("success", True))
            latency_ms = float(body.get("latency_ms", 0))
            task_type  = body.get("task_type", "unknown")

            if not hostname:
                return web.json_response({"error": "hostname required"}, status=400)

            if success:
                self.router.record_success(hostname, latency_ms, task_type)
            else:
                self.router.record_failure(hostname)
                logger.warning(
                    f"[Outcome] Failure recorded: {hostname} task={task_type}",
                    extra={"request_id": rid},
                )

            return web.json_response({"ok": True})

        except Exception as e:
            logger.error(f"[Outcome] Error: {e}", extra={"request_id": rid})
            return web.json_response({"error": str(e)}, status=500)

    async def handle_status(self, request: web.Request) -> web.Response:
        """GET /status -- full cluster + orchestrator state."""
        status = await self.router.cluster_status()
        status["orchestrator"] = {
            "port": ORCHESTRATOR_PORT,
            "uptime_seconds": int(time.monotonic() - self.start_time),
            "hostname": self.network.hostname,
        }
        return web.json_response(status)

    async def handle_nodes(self, request: web.Request) -> web.Response:
        """GET /nodes -- live nodes only."""
        live = await self.network.get_live_nodes()
        return web.json_response({h: n.to_dict() for h, n in live.items()})

    async def handle_health(self, request: web.Request) -> web.Response:
        """GET /health -- liveness probe."""
        return web.json_response({
            "status": "ok",
            "hostname": self.network.hostname,
            "uptime": int(time.monotonic() - self.start_time),
        })

    async def handle_circuit_breakers(self, request: web.Request) -> web.Response:
        """GET /circuit-breakers -- current breaker states."""
        return web.json_response({
            hostname: cb.to_dict()
            for hostname, cb in self.router._circuit_breakers.items()
        })

    # ── API setup ─────────────────────────────────────────────

    async def _start_api(self) -> None:
        app = web.Application(middlewares=[request_id_middleware, timing_middleware])
        app.router.add_post("/route",            self.handle_route)
        app.router.add_post("/classify",         self.handle_classify)
        app.router.add_post("/outcome",          self.handle_outcome)
        app.router.add_get("/status",            self.handle_status)
        app.router.add_get("/nodes",             self.handle_nodes)
        app.router.add_get("/health",            self.handle_health)
        app.router.add_get("/circuit-breakers",  self.handle_circuit_breakers)

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, ORCHESTRATOR_HOST, ORCHESTRATOR_PORT)
        await site.start()

        logger.info(f"[Orchestrator] API on http://{ORCHESTRATOR_HOST}:{ORCHESTRATOR_PORT}")
        logger.info("[Orchestrator] Routes: /route /classify /outcome /status /nodes /health /circuit-breakers")

    # ── Graceful shutdown ─────────────────────────────────────

    async def shutdown(self, sig: Optional[signal.Signals] = None) -> None:
        if self._stopping:
            return
        self._stopping = True

        if sig:
            logger.info(f"[Orchestrator] Received {sig.name} -- shutting down")
        else:
            logger.info("[Orchestrator] Shutting down")

        self.network.stop()

        if self._runner:
            await self._runner.cleanup()

        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        logger.info("[Orchestrator] Clean shutdown complete")

    # ── Main run ──────────────────────────────────────────────

    async def run(self) -> None:
        loop = asyncio.get_event_loop()

        # Signal handlers — Linux (Pi) only, Windows doesn't support add_signal_handler
        if sys.platform != "win32":
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(
                    sig,
                    lambda s=sig: asyncio.create_task(self.shutdown(s)),
                )

        logger.info("=" * 60)
        logger.info("  HZL Orchestrator v2")
        logger.info(f"  Node:   {self.network.hostname} ({self.network.ip})")
        logger.info(f"  Role:   {self.network.role}")
        logger.info(f"  Caps:   {self.network.capabilities}")
        logger.info("=" * 60)

        try:
            await self._start_api()
        except Exception as e:
            logger.error(f"[Orchestrator] API failed to start: {e}")
            await self.shutdown()
            return

        await self.network.start()


# ─────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────

def main() -> None:
    config = load_config()
    setup_logging(config)
    orch = HZLOrchestrator(config)
    asyncio.run(orch.run())


if __name__ == "__main__":
    main()
