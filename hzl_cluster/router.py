"""
hzl_router.py  v2
-----------------
HZL Router — task classification + node selection with circuit breakers.
"""

import asyncio
import logging
import random
import re
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from hzl_cluster.network import HZLNetwork, NodeInfo

logger = logging.getLogger("hzl.router")


# ─────────────────────────────────────────────────────────────
# Circuit Breaker
# ─────────────────────────────────────────────────────────────

@dataclass
class CircuitBreaker:
    """
    Three-state circuit breaker per node.
    closed -> normal | open -> skip | half-open -> probe
    """
    failure_threshold: int
    recovery_timeout: float

    _failures: int   = field(default=0, init=False, repr=False)
    _opened_at: float = field(default=0.0, init=False, repr=False)
    _state: str      = field(default="closed", init=False, repr=False)

    @property
    def state(self) -> str:
        if self._state == "open":
            if time.monotonic() - self._opened_at >= self.recovery_timeout:
                self._state = "half-open"
        return self._state

    def is_open(self) -> bool:
        return self.state == "open"

    def record_success(self) -> None:
        self._failures = 0
        self._state = "closed"

    def record_failure(self) -> None:
        self._failures += 1
        if self._failures >= self.failure_threshold:
            if self._state != "open":
                logger.warning(f"[CircuitBreaker] OPENED after {self._failures} failures")
                self._state = "open"
                self._opened_at = time.monotonic()

    def to_dict(self) -> dict:
        return {
            "state": self.state,
            "failures": self._failures,
            "open_since": self._opened_at if self._state != "closed" else None,
        }


# ─────────────────────────────────────────────────────────────
# Metrics
# ─────────────────────────────────────────────────────────────

class LatencyWindow:
    """Rolling window of latency samples. Computes p50/p95/p99."""

    def __init__(self, maxlen: int = 100):
        self._samples: deque = deque(maxlen=maxlen)

    def record(self, ms: float) -> None:
        self._samples.append(ms)

    def percentiles(self) -> dict:
        if not self._samples:
            return {"p50": None, "p95": None, "p99": None, "count": 0}
        s = sorted(self._samples)
        n = len(s)
        def p(pct): return s[min(int(n * pct / 100), n - 1)]
        return {"p50": p(50), "p95": p(95), "p99": p(99), "count": n}


@dataclass
class TaskMetrics:
    requests: int = 0
    local_hits: int = 0
    cloud_fallbacks: int = 0
    latency: LatencyWindow = field(default_factory=LatencyWindow)

    def to_dict(self) -> dict:
        return {
            "requests": self.requests,
            "local_hits": self.local_hits,
            "cloud_fallbacks": self.cloud_fallbacks,
            "latency_ms": self.latency.percentiles(),
        }


# ─────────────────────────────────────────────────────────────
# Task classifier
# ─────────────────────────────────────────────────────────────

_TASK_PATTERNS: List[Tuple[str, List[Tuple[str, bool]]]] = [
    ("gateway_sync", [
        ("go online", True), ("sync now", True),
        ("connect to the internet", True),
        ("run a sync", True),
    ]),
    ("gateway_fetch", [
        ("check my email", True), ("check email", True),
        ("fetch my email", True), ("fetch email", True),
        ("download", False),
        ("get me the news", True), ("get the news", True),
        ("update my weather", True), ("update weather", True),
        ("update my forecast", True), ("update forecast", True),
        ("update my maps", True), ("update maps", True),
        ("sync with the internet", True),
    ]),
    ("home_control", [
        ("turn on", True), ("turn off", True), ("dim the", True),
        ("switch on", True), ("switch off", True),
        ("lights", False), ("thermostat", False), ("lock", False),
        ("unlock", False), ("fan", False), ("heat", False),
        ("home assistant", True), ("set the temperature", True),
    ]),
    ("search", [
        ("search for", True), ("look up", True), ("find out", True),
        ("latest news", True), ("breaking news", True),
        ("what happened", True), ("who won", True),
        ("weather", False), ("forecast", False),
        ("current events", True),
    ]),
    ("memory_write", [
        ("remember that", True), ("note that", True),
        ("don't forget", True), ("save this", True),
        ("add to my", True), ("remind me", True),
    ]),
    ("heavy_inference", [
        ("write a", True), ("write me", True), ("draft", False),
        ("generate a", True), ("create a script", True),
        ("code", False), ("function", False),
        ("analyze", False), ("summarize", False),
    ]),
    ("reasoning", [
        ("why does", True), ("how does", True), ("explain", False),
        ("what do you think", True), ("help me understand", True),
        ("what should i", True), ("your opinion", True),
        ("recommend", False), ("advice", False),
    ]),
]

_COMPILED: List[Tuple[str, List[Tuple[re.Pattern, bool]]]] = [
    (
        task,
        [
            (
                re.compile(r"\b" + re.escape(pat) + r"\b", re.IGNORECASE) if not is_phrase
                else re.compile(re.escape(pat), re.IGNORECASE),
                is_phrase,
            )
            for pat, is_phrase in patterns
        ],
    )
    for task, patterns in _TASK_PATTERNS
]

_DEFAULT_TASK = "voice_response"
_HEAVY_CHAR_THRESHOLD = 250


def classify_task(text: str) -> str:
    """
    Classify raw text into a task type.
    Fast, zero API cost. Uses compiled regex with word boundaries.
    Falls back to voice_response by default.
    """
    for task_type, compiled_patterns in _COMPILED:
        for pattern, _ in compiled_patterns:
            if pattern.search(text):
                if task_type == "reasoning" and len(text) > _HEAVY_CHAR_THRESHOLD:
                    return "heavy_inference"
                return task_type
    return _DEFAULT_TASK


# ─────────────────────────────────────────────────────────────
# Router
# ─────────────────────────────────────────────────────────────

@dataclass
class RoutingDecision:
    task_type: str
    model: str
    max_tokens: int
    timeout: int
    local: bool
    node: Optional[NodeInfo]
    circuit_breaker: Optional[CircuitBreaker] = field(default=None, repr=False)

    def to_dict(self) -> dict:
        return {
            "task_type": self.task_type,
            "model": self.model,
            "max_tokens": self.max_tokens,
            "timeout": self.timeout,
            "local": self.local,
            "node": self.node.to_dict() if self.node else None,
        }


class HZLRouter:
    """
    Routes tasks to the best available node.
    Fallback chain: preferred -> any_capable -> core -> cloud_direct
    """

    def __init__(self, config: dict, network: HZLNetwork):
        self.config = config
        self.network = network
        self.task_map: dict = config["routing"]["task_map"]
        self.thresholds: dict = config["thresholds"]
        self.core_hostname: str = config["cluster"]["core_node"]

        cb_cfg = config.get("circuit_breaker", {})
        self._cb_failure_threshold: int   = cb_cfg.get("failure_threshold", 4)
        self._cb_recovery_timeout: float  = float(cb_cfg.get("recovery_timeout", 45))

        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._metrics: Dict[str, TaskMetrics] = {
            t: TaskMetrics() for t in self.task_map
        }
        self._metrics.setdefault(_DEFAULT_TASK, TaskMetrics())

        win = config.get("metrics", {}).get("latency_window", 100)
        for m in self._metrics.values():
            m.latency = LatencyWindow(maxlen=win)

    def _cb(self, hostname: str) -> CircuitBreaker:
        if hostname not in self._circuit_breakers:
            self._circuit_breakers[hostname] = CircuitBreaker(
                failure_threshold=self._cb_failure_threshold,
                recovery_timeout=self._cb_recovery_timeout,
            )
        return self._circuit_breakers[hostname]

    def record_success(self, hostname: str, latency_ms: float, task_type: str) -> None:
        self._cb(hostname).record_success()
        m = self._metrics.get(task_type)
        if m:
            m.local_hits += 1
            m.latency.record(latency_ms)

    def record_failure(self, hostname: str) -> None:
        self._cb(hostname).record_failure()

    def _is_healthy(self, node: NodeInfo) -> bool:
        return (
            node.alive
            and node.cpu_percent    < self.thresholds["cpu_overload"]
            and node.memory_percent < self.thresholds["memory_overload"]
            and not self._cb(node.hostname).is_open()
        )

    def _score(self, node: NodeInfo) -> float:
        w_cpu = self.thresholds.get("cpu_weight", 0.70)
        w_mem = self.thresholds.get("memory_weight", 0.30)
        return (node.cpu_percent / 100.0 * w_cpu) + (node.memory_percent / 100.0 * w_mem)

    def _best_capable(
        self,
        nodes: Dict[str, NodeInfo],
        capability: Optional[str],
        role: Optional[str] = None,
    ) -> Optional[NodeInfo]:
        candidates = [
            n for n in nodes.values()
            if self._is_healthy(n)
            and n.has_capability(capability)
            and (role is None or n.role == role)
        ]
        if not candidates:
            return None
        return min(candidates, key=self._score)

    async def route(
        self,
        text_or_task: str,
        is_task_type: bool = False,
    ) -> RoutingDecision:
        task_type = text_or_task if is_task_type else classify_task(text_or_task)
        task_cfg  = self.task_map.get(task_type, self.task_map.get(_DEFAULT_TASK, {}))

        model      = task_cfg.get("model")  # Can be None for gateway tasks
        max_tokens = task_cfg.get("max_tokens", 500)
        timeout    = task_cfg.get("timeout", 10)
        preferred  = task_cfg.get("preferred_node", self.core_hostname)
        capability = task_cfg.get("capability")

        m = self._metrics.setdefault(task_type, TaskMetrics())
        m.requests += 1

        live = await self.network.get_live_nodes()
        target: Optional[NodeInfo] = None

        # 1. Preferred node
        if preferred == "any_worker":
            workers = {h: n for h, n in live.items() if n.role == "worker"}
            target = self._best_capable(workers, capability)
        elif preferred == "any_node":
            target = self._best_capable(live, capability)
        elif preferred in live:
            candidate = live[preferred]
            if self._is_healthy(candidate) and candidate.has_capability(capability):
                target = candidate

        # 2. Any capable node
        if target is None:
            target = self._best_capable(live, capability)
            if target:
                logger.info(f"[Router] {task_type}: preferred unavailable -> {target.hostname}")

        # 3. Core (capability-relaxed)
        if target is None:
            core = live.get(self.core_hostname)
            if core and core.alive and not self._cb(core.hostname).is_open():
                target = core
                logger.warning(f"[Router] {task_type}: no capable node -> core (capability relaxed)")

        # 4. Cloud direct
        if target is None:
            logger.error(f"[Router] {task_type}: ALL NODES DOWN -- cloud direct")
            m.cloud_fallbacks += 1
            return RoutingDecision(
                task_type=task_type, model=model, max_tokens=max_tokens,
                timeout=timeout, local=False, node=None,
            )

        logger.debug(
            f"[Router] {task_type} -> {target.hostname} "
            f"score={self._score(target):.2f} "
            f"cpu={target.cpu_percent:.0f}% mem={target.memory_percent:.0f}%"
        )

        return RoutingDecision(
            task_type=task_type, model=model, max_tokens=max_tokens,
            timeout=timeout, local=True, node=target,
            circuit_breaker=self._cb(target.hostname),
        )

    async def cluster_status(self) -> dict:
        all_nodes  = await self.network.get_all_nodes()
        return {
            "cluster": self.config["cluster"]["name"],
            "total_nodes": len(all_nodes),
            "nodes": [
                {
                    "hostname": n.hostname,
                    "ip": n.ip,
                    "role": n.role,
                    "alive": n.alive,
                    "healthy": self._is_healthy(n),
                    "score": round(self._score(n), 3),
                    "cpu": round(n.cpu_percent, 1),
                    "memory": round(n.memory_percent, 1),
                    "load1": round(n.load1_percent, 1),
                    "capabilities": n.capabilities,
                    "circuit_breaker": self._cb(n.hostname).to_dict(),
                }
                for n in all_nodes.values()
            ],
            "metrics": {
                task: m.to_dict() for task, m in self._metrics.items()
            },
        }


if __name__ == "__main__":
    import json as _json

    TEST_INPUTS = [
        "turn off the kitchen lights",
        "turn on your analytical thinking",
        "what is the meaning of life",
        "what is the weather today",
        "search for the latest AI news",
        "write a short story about a robot",
        "hey what time is it",
        "remember that my dentist is Thursday",
        "explain how transformers work in detail and walk me through the architecture step by step with examples",
    ]

    print("\n-- HZL Router Classifier v2 --")
    for text in TEST_INPUTS:
        task = classify_task(text)
        print(f"  [{task:20s}] {text[:60]}")
