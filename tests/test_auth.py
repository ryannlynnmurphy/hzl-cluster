"""
test_auth.py — Tests for HazelAuth HMAC token authentication.
"""

import os
import tempfile
import time

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from hzl_cluster.auth import HazelAuth, load_secret


# ──────────────────────────────────────────────────────────────
# Unit tests — HazelAuth
# ──────────────────────────────────────────────────────────────

class TestHazelAuth:

    def setup_method(self):
        self.auth = HazelAuth("test-secret-key")

    # 1. generate and verify round-trip
    def test_generate_and_verify(self):
        """A freshly generated token verifies correctly."""
        token = self.auth.generate_token("/route")
        assert self.auth.verify_token(token, "/route") is True

    # 2. wrong payload
    def test_wrong_payload_fails(self):
        """A token generated for one payload does not verify against another."""
        token = self.auth.generate_token("/route")
        assert self.auth.verify_token(token, "/different-route") is False

    # 3. expired token
    def test_expired_token_fails(self):
        """A token whose timestamp is older than max_age is rejected."""
        past_ts = time.time() - 400          # 400 seconds ago
        token = self.auth.generate_token("/route", timestamp=past_ts)
        assert self.auth.verify_token(token, "/route", max_age=300) is False

    # 4. wrong secret
    def test_wrong_secret_fails(self):
        """A token signed with a different secret cannot be verified."""
        other_auth = HazelAuth("completely-different-secret")
        token = other_auth.generate_token("/route")
        assert self.auth.verify_token(token, "/route") is False

    # ──────────────────────────────────────────────────────────
    # Middleware integration
    # ──────────────────────────────────────────────────────────

    # 5. /health is exempt from auth
    def test_health_exempt(self):
        """/health responds 200 without any X-Hazel-Auth header."""
        import asyncio

        async def _run():
            app = web.Application(middlewares=[self.auth.middleware()])

            async def health(_request):
                return web.json_response({"status": "ok"})

            async def protected(_request):
                return web.json_response({"data": "secret"})

            app.router.add_get("/health", health)
            app.router.add_get("/data",   protected)

            async with TestClient(TestServer(app)) as client:
                # /health — no auth header
                resp = await client.get("/health")
                assert resp.status == 200

                # /data — no auth header → 401
                resp = await client.get("/data")
                assert resp.status == 401

                # /data — valid auth header → 200
                token = self.auth.generate_token("/data")
                resp = await client.get("/data", headers={"X-Hazel-Auth": token})
                assert resp.status == 200

        asyncio.run(_run())


# ──────────────────────────────────────────────────────────────
# Unit tests — load_secret
# ──────────────────────────────────────────────────────────────

class TestLoadSecret:

    # 6. generates and saves secret when file doesn't exist
    def test_load_secret_creates_file(self):
        """load_secret generates a secret and writes it when the file is absent."""
        with tempfile.TemporaryDirectory() as tmpdir:
            secret_path = os.path.join(tmpdir, "cluster.secret")
            config = {"auth": {"secret_file": secret_path}}

            assert not os.path.exists(secret_path)

            secret = load_secret(config)

            assert isinstance(secret, str)
            assert len(secret) > 0
            assert os.path.exists(secret_path)

            # The file contents must match what was returned.
            with open(secret_path) as fh:
                saved = fh.read().strip()
            assert saved == secret

    # 7. reads existing secret file without overwriting
    def test_load_secret_reads_existing(self):
        """load_secret returns the existing file contents unchanged."""
        with tempfile.TemporaryDirectory() as tmpdir:
            secret_path = os.path.join(tmpdir, "cluster.secret")
            known_secret = "known-test-secret-value-abc123"

            with open(secret_path, "w") as fh:
                fh.write(known_secret + "\n")   # trailing newline stripped

            config = {"auth": {"secret_file": secret_path}}
            result = load_secret(config)

            assert result == known_secret
