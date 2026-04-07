"""
Token bucket rate limiter for Hazel cluster REST APIs.

Prevents abuse and protects Raspberry Pi nodes from being overwhelmed.
Each key (typically a client IP) gets its own independent bucket.

Algorithm:
  - Each bucket holds up to `burst` tokens.
  - Tokens refill at `rate` tokens per second (continuous drip).
  - Each request consumes one token. If the bucket is empty: deny (429).

Usage:
    limiter = RateLimiter(rate=10.0, burst=20)

    # Direct use
    if limiter.allow("192.168.1.5"):
        ...  # proceed
    else:
        ...  # reject

    # aiohttp middleware
    app = web.Application(middlewares=[RateLimiter.middleware(rate=10.0, burst=20)])
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from aiohttp import web

logger = logging.getLogger("hzl.rate_limiter")


class _Bucket:
    """Single token bucket for one key."""

    __slots__ = ("tokens", "last_refill", "rate", "burst")

    def __init__(self, rate: float, burst: int) -> None:
        self.rate = rate
        self.burst = burst
        self.tokens: float = float(burst)   # start full
        self.last_refill: float = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(float(self.burst), self.tokens + elapsed * self.rate)
        self.last_refill = now

    def consume(self) -> bool:
        """Return True and deduct a token if one is available."""
        self._refill()
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False

    def remaining(self) -> int:
        self._refill()
        return int(self.tokens)


class RateLimiter:
    """
    Per-key token bucket rate limiter.

    Parameters
    ----------
    rate:
        Tokens added to each bucket per second.
    burst:
        Maximum bucket capacity (and the initial fill level).
    """

    def __init__(self, rate: float, burst: int) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        if burst < 1:
            raise ValueError("burst must be at least 1")
        self._rate = rate
        self._burst = burst
        self._buckets: dict[str, _Bucket] = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_or_create(self, key: str) -> _Bucket:
        if key not in self._buckets:
            self._buckets[key] = _Bucket(self._rate, self._burst)
        return self._buckets[key]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def allow(self, key: str = "default") -> bool:
        """
        Attempt to consume one token for *key*.

        Returns True if the request is allowed, False if the bucket is
        empty and the request should be rejected.
        """
        allowed = self._get_or_create(key).consume()
        if not allowed:
            logger.debug("Rate limit exceeded for key=%s", key)
        return allowed

    def remaining(self, key: str = "default") -> int:
        """Return the number of whole tokens remaining for *key*."""
        return self._get_or_create(key).remaining()

    def reset(self, key: Optional[str] = None) -> None:
        """
        Reset token bucket(s) to full capacity.

        Parameters
        ----------
        key:
            The specific key to reset.  Pass ``None`` (default) to reset
            every bucket in this limiter.
        """
        if key is None:
            self._buckets.clear()
        elif key in self._buckets:
            del self._buckets[key]

    # ------------------------------------------------------------------
    # aiohttp middleware factory
    # ------------------------------------------------------------------

    @staticmethod
    def middleware(rate: float, burst: int):
        """
        Return an aiohttp middleware that rate-limits requests by client IP.

        Each unique remote IP address gets its own token bucket configured
        with *rate* tokens/second and *burst* maximum tokens.  When a
        bucket is exhausted the middleware returns 429 Too Many Requests
        without calling the downstream handler.

        Parameters
        ----------
        rate:
            Tokens per second per IP address.
        burst:
            Maximum burst size per IP address.
        """
        limiter = RateLimiter(rate=rate, burst=burst)

        @web.middleware
        async def _rate_limit_middleware(
            request: web.Request, handler
        ) -> web.Response:
            ip = request.remote or "unknown"
            if limiter.allow(ip):
                return await handler(request)

            logger.warning(
                "429 Too Many Requests — rate limit exceeded for %s %s (ip=%s)",
                request.method,
                request.path,
                ip,
            )
            return web.json_response(
                {"error": "Too Many Requests"},
                status=429,
                headers={"Retry-After": "1"},
            )

        return _rate_limit_middleware
