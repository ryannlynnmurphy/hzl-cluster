"""
hzl_ws_integration.py  v2
--------------------------
Orchestrator client for hzl_ws.py / brain.py.

Usage:
  from hzl_ws_integration import get_routing_context, record_routing_outcome, shutdown_integration

  ctx = await get_routing_context(user_text)
  # use ctx.model, ctx.max_tokens in your Claude call
  record_routing_outcome(ctx, success=True, latency_ms=elapsed)
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional

import aiohttp

logger = logging.getLogger("hzl.ws.integration")

ORCHESTRATOR_URL   = os.environ.get("HZL_ORCHESTRATOR_URL", "http://localhost:9000")
REQUEST_TIMEOUT    = 2.0
MAX_RETRIES        = 2
BACKOFF_BASE       = 0.2
CB_FAILURE_THRESH  = 4
CB_RECOVERY_SECS   = 30.0

# ─────────────────────────────────────────────────────────────
# Module-level session singleton
# ─────────────────────────────────────────────────────────────

_session: Optional[aiohttp.ClientSession] = None


def _get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        connector = aiohttp.TCPConnector(
            limit=5,
            keepalive_timeout=60,
            enable_cleanup_closed=True,
        )
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        _session = aiohttp.ClientSession(connector=connector, timeout=timeout)
    return _session


async def shutdown_integration() -> None:
    global _session
    if _session and not _session.closed:
        await _session.close()
        _session = None
    logger.info("[Integration] Session pool closed.")


# ─────────────────────────────────────────────────────────────
# Client-side circuit breaker for the orchestrator
# ─────────────────────────────────────────────────────────────

class _OrchestratorBreaker:
    def __init__(self):
        self._failures = 0
        self._opened_at = 0.0
        self._open = False

    def is_open(self) -> bool:
        if self._open:
            if time.monotonic() - self._opened_at >= CB_RECOVERY_SECS:
                self._open = False
                logger.info("[Integration] Orchestrator circuit half-open -- probing")
        return self._open

    def record_success(self) -> None:
        if self._failures > 0:
            logger.info("[Integration] Orchestrator circuit CLOSED")
        self._failures = 0
        self._open = False

    def record_failure(self) -> None:
        self._failures += 1
        if self._failures >= CB_FAILURE_THRESH and not self._open:
            self._open = True
            self._opened_at = time.monotonic()
            logger.warning(
                f"[Integration] Orchestrator circuit OPEN "
                f"after {self._failures} failures -- using defaults"
            )


_breaker = _OrchestratorBreaker()

# ─────────────────────────────────────────────────────────────
# RoutingContext
# ─────────────────────────────────────────────────────────────

@dataclass
class RoutingContext:
    task_type:  str
    model:      str
    max_tokens: int
    timeout:    int
    local:      bool
    node_hostname: Optional[str]
    _request_id: str = field(default="", repr=False)
    _routed_at:  float = field(default_factory=time.monotonic, repr=False)


_DEFAULTS = RoutingContext(
    task_type="voice_response",
    model="claude-haiku-4-5-20251001",
    max_tokens=500,
    timeout=10,
    local=True,
    node_hostname=None,
)

# ─────────────────────────────────────────────────────────────
# Main API
# ─────────────────────────────────────────────────────────────

async def get_routing_context(text: str) -> RoutingContext:
    """
    Ask the orchestrator for a routing decision.
    Never raises -- falls back to defaults so Hazel keeps working.
    """
    if _breaker.is_open():
        return _DEFAULTS

    session = _get_session()
    last_exc: Optional[Exception] = None

    for attempt in range(MAX_RETRIES):
        try:
            async with session.post(
                f"{ORCHESTRATOR_URL}/route",
                json={"text": text},
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    _breaker.record_success()

                    node = data.get("node") or {}
                    return RoutingContext(
                        task_type=data.get("task_type", "voice_response"),
                        model=data.get("model", _DEFAULTS.model),
                        max_tokens=data.get("max_tokens", _DEFAULTS.max_tokens),
                        timeout=data.get("timeout", _DEFAULTS.timeout),
                        local=data.get("local", True),
                        node_hostname=node.get("hostname"),
                        _request_id=resp.headers.get("X-Request-ID", ""),
                    )
                else:
                    logger.warning(f"[Integration] Orchestrator returned {resp.status}")
                    last_exc = ValueError(f"HTTP {resp.status}")

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_exc = e
            if attempt < MAX_RETRIES - 1:
                backoff = BACKOFF_BASE * (2 ** attempt)
                await asyncio.sleep(backoff)

    _breaker.record_failure()
    logger.warning(f"[Integration] Orchestrator unreachable ({last_exc}) -- using defaults")
    return _DEFAULTS


def record_routing_outcome(
    ctx: RoutingContext,
    success: bool,
    latency_ms: float = 0.0,
) -> None:
    """
    Fire-and-forget outcome report to orchestrator.
    Call after each Claude API response.
    """
    if not ctx.node_hostname:
        return

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(
                _post_outcome(ctx.node_hostname, success, latency_ms, ctx.task_type)
            )
    except RuntimeError:
        pass


async def _post_outcome(
    hostname: str,
    success: bool,
    latency_ms: float,
    task_type: str,
) -> None:
    session = _get_session()
    try:
        async with session.post(
            f"{ORCHESTRATOR_URL}/outcome",
            json={
                "hostname": hostname,
                "success": success,
                "latency_ms": latency_ms,
                "task_type": task_type,
            },
            timeout=aiohttp.ClientTimeout(total=0.5),
        ) as resp:
            pass
    except Exception:
        pass
