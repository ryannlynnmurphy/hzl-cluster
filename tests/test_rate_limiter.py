"""
test_rate_limiter.py — Tests for the RateLimiter token bucket.
"""

import asyncio
import time

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from hzl_cluster.rate_limiter import RateLimiter


# ──────────────────────────────────────────────────────────────
# Unit tests — RateLimiter
# ──────────────────────────────────────────────────────────────

class TestRateLimiter:

    # 1. Requests within the burst limit are all allowed
    def test_allow_within_rate(self):
        """Every request within the burst capacity is permitted."""
        limiter = RateLimiter(rate=10.0, burst=5)
        results = [limiter.allow("client") for _ in range(5)]
        assert all(results), "Expected all 5 requests within burst to be allowed"

    # 2. Requests beyond the burst are denied
    def test_deny_over_burst(self):
        """Requests exceeding burst size are rejected."""
        limiter = RateLimiter(rate=10.0, burst=3)

        allowed = [limiter.allow("client") for _ in range(3)]
        assert all(allowed), "First 3 requests should be allowed"

        # Bucket is now empty — next request must be denied.
        assert limiter.allow("client") is False

    # 3. Tokens refill over real time
    def test_tokens_refill_over_time(self):
        """Tokens accumulate in the bucket as time passes."""
        limiter = RateLimiter(rate=100.0, burst=3)  # 100 tokens/s → ~10ms per token

        # Drain the bucket completely.
        for _ in range(3):
            limiter.allow("client")
        assert limiter.allow("client") is False, "Bucket should be empty"

        # Wait long enough to refill at least 1 token (100 t/s → 15ms ~ 1.5 tokens).
        time.sleep(0.015)

        assert limiter.allow("client") is True, "Should be allowed after refill"

    # 4. Different keys have independent buckets
    def test_per_key_isolation(self):
        """Exhausting one key's bucket does not affect other keys."""
        limiter = RateLimiter(rate=10.0, burst=2)

        # Drain key A completely.
        limiter.allow("key_a")
        limiter.allow("key_a")
        assert limiter.allow("key_a") is False, "key_a should be exhausted"

        # key_b is a fresh bucket — still full.
        assert limiter.allow("key_b") is True, "key_b should be unaffected"
        assert limiter.allow("key_b") is True

    # 5. reset removes a single key's bucket
    def test_reset_key(self):
        """reset(key) refills that key; reset() refills all keys."""
        limiter = RateLimiter(rate=10.0, burst=2)

        # Drain both keys.
        for _ in range(2):
            limiter.allow("alpha")
            limiter.allow("beta")

        assert limiter.allow("alpha") is False
        assert limiter.allow("beta") is False

        # Reset only alpha.
        limiter.reset("alpha")
        assert limiter.allow("alpha") is True,  "alpha should be full after reset"
        assert limiter.allow("beta") is False,  "beta should still be exhausted"

        # Reset all.
        limiter.reset()
        assert limiter.allow("beta") is True, "beta should be full after global reset"

    # 6. remaining returns correct token count
    def test_remaining_count(self):
        """remaining() reflects consumed tokens accurately."""
        limiter = RateLimiter(rate=10.0, burst=5)

        assert limiter.remaining("x") == 5

        limiter.allow("x")
        assert limiter.remaining("x") == 4

        limiter.allow("x")
        limiter.allow("x")
        assert limiter.remaining("x") == 2

        # Drain and verify zero.
        limiter.allow("x")
        limiter.allow("x")
        assert limiter.remaining("x") == 0


# ──────────────────────────────────────────────────────────────
# Middleware integration test
# ──────────────────────────────────────────────────────────────

class TestRateLimiterMiddleware:

    def test_middleware_returns_429_when_exceeded(self):
        """
        The aiohttp middleware allows requests within burst, then returns
        429 Too Many Requests once the bucket is empty.
        """

        async def _run():
            # Burst of 2 — first two requests pass, third gets 429.
            app = web.Application(
                middlewares=[RateLimiter.middleware(rate=1.0, burst=2)]
            )

            async def handler(_request):
                return web.json_response({"ok": True})

            app.router.add_get("/api/data", handler)

            async with TestClient(TestServer(app)) as client:
                resp1 = await client.get("/api/data")
                assert resp1.status == 200

                resp2 = await client.get("/api/data")
                assert resp2.status == 200

                resp3 = await client.get("/api/data")
                assert resp3.status == 429

                body = await resp3.json()
                assert "error" in body

        asyncio.run(_run())
