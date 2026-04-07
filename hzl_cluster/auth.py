"""
API authentication -- HMAC token auth for inter-node communication.
Nodes share a secret key. Each request includes an HMAC signature
in the X-Hazel-Auth header. Prevents unauthorized access to cluster APIs.

Auth flow:
  1. On startup, both orchestrator and gateway load the same shared secret.
  2. When node A sends a request to node B, it includes:
       X-Hazel-Auth: {timestamp}:{hmac}
  3. Node B verifies the HMAC and checks the timestamp is fresh
     (within max_age seconds, default 300 / 5 minutes).
  4. If invalid: 401 Unauthorized.

Exempt paths: /health (liveness probes don't need auth).
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import secrets
import time
from typing import Callable, Optional

from aiohttp import web

logger = logging.getLogger("hzl.auth")

# Paths that skip authentication entirely.
_EXEMPT_PATHS = {"/health"}


class HazelAuth:
    """HMAC-SHA256 token authentication for inter-node REST calls."""

    def __init__(self, secret: str) -> None:
        self._secret = secret.encode() if isinstance(secret, str) else secret

    # ------------------------------------------------------------------
    # Token generation
    # ------------------------------------------------------------------

    def generate_token(self, payload: str, timestamp: Optional[float] = None) -> str:
        """
        Return "{timestamp}:{hmac}" for the given payload.

        Parameters
        ----------
        payload:
            Arbitrary string that represents what is being signed
            (e.g. a route or request body digest). The same value must be
            supplied to verify_token.
        timestamp:
            Unix epoch float. Defaults to time.time() when omitted.
        """
        ts = timestamp if timestamp is not None else time.time()
        message = f"{payload}:{ts}".encode()
        sig = hmac.new(self._secret, message, hashlib.sha256).hexdigest()
        return f"{ts}:{sig}"

    # ------------------------------------------------------------------
    # Token verification
    # ------------------------------------------------------------------

    def verify_token(self, token: str, payload: str, max_age: int = 300) -> bool:
        """
        Return True if the token is valid and not older than max_age seconds.

        Parameters
        ----------
        token:
            The value of the X-Hazel-Auth header ("{timestamp}:{hmac}").
        payload:
            The same payload string used when the token was generated.
        max_age:
            Maximum allowed age in seconds (default 300 / 5 minutes).
        """
        try:
            ts_str, sig = token.split(":", 1)
            ts = float(ts_str)
        except (ValueError, AttributeError):
            logger.warning("Malformed auth token")
            return False

        # Reject stale tokens.
        age = time.time() - ts
        if age > max_age or age < 0:
            logger.warning("Auth token age %.1fs outside window (%ds)", age, max_age)
            return False

        # Constant-time HMAC comparison.
        message = f"{payload}:{ts}".encode()
        expected = hmac.new(self._secret, message, hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, sig)

    # ------------------------------------------------------------------
    # aiohttp middleware
    # ------------------------------------------------------------------

    def middleware(self) -> Callable:
        """
        Return an aiohttp middleware that enforces X-Hazel-Auth on every
        request, except those whose path is in _EXEMPT_PATHS.

        The middleware uses the request path as the token payload so that
        a token generated for one endpoint cannot be replayed against another.
        """

        @web.middleware
        async def _auth_middleware(request: web.Request, handler: Callable) -> web.Response:
            if request.path in _EXEMPT_PATHS:
                return await handler(request)

            auth_header = request.headers.get("X-Hazel-Auth", "")
            if not auth_header:
                logger.warning("Missing X-Hazel-Auth on %s %s", request.method, request.path)
                return web.json_response({"error": "Unauthorized"}, status=401)

            if not self.verify_token(auth_header, request.path):
                logger.warning("Invalid X-Hazel-Auth on %s %s", request.method, request.path)
                return web.json_response({"error": "Unauthorized"}, status=401)

            return await handler(request)

        return _auth_middleware


# ──────────────────────────────────────────────────────────────
# Helper
# ──────────────────────────────────────────────────────────────

def load_secret(config: dict) -> str:
    """
    Return the shared HMAC secret for this cluster.

    Looks up ``config["auth"]["secret_file"]``.  If the file exists its
    contents (stripped of whitespace) are returned.  If the file does not
    exist a 64-character hex secret is generated, written to that path,
    and returned so that all nodes can later read the same value.

    If the config dict has no ``auth.secret_file`` key a random secret is
    returned without writing anything to disk (useful in tests).
    """
    secret_file: Optional[str] = config.get("auth", {}).get("secret_file")

    if secret_file:
        if os.path.exists(secret_file):
            with open(secret_file) as fh:
                return fh.read().strip()

        # File missing — generate and persist.
        new_secret = secrets.token_hex(32)
        os.makedirs(os.path.dirname(os.path.abspath(secret_file)), exist_ok=True)
        with open(secret_file, "w") as fh:
            fh.write(new_secret)
        logger.info("Generated new cluster secret and saved to %s", secret_file)
        return new_secret

    # No path configured — ephemeral secret (dev / test only).
    logger.warning("No auth.secret_file in config; using ephemeral secret")
    return secrets.token_hex(32)
