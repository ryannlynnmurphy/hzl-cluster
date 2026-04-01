"""
hzl_network.py  v2
------------------
HZL Network Layer — UDP beacon discovery + health tracking.

Each node broadcasts its presence every heartbeat_interval seconds.
Registry tracks live/dead state. Callbacks fire on topology changes.
"""

import asyncio
import json
import logging
import os
import random
import socket
import threading
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Callable, Dict, List, Optional

import psutil
import yaml

logger = logging.getLogger("hzl.network")

# Resolve config relative to this file's directory
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.environ.get("HZL_CONFIG", os.path.join(_THIS_DIR, "hzl_config.yaml"))
BEACON_VERSION = 2


def load_config(path: str = CONFIG_PATH) -> dict:
    with open(path) as f:
        cfg = yaml.safe_load(f)
    # Resolve relative paths against the config file's directory
    base = os.path.dirname(os.path.abspath(path))
    paths = cfg.get("paths", {})
    for key in ("db", "log_dir", "config"):
        val = paths.get(key)
        if val and not os.path.isabs(val):
            paths[key] = os.path.join(base, val)
    return cfg


def get_local_ip() -> str:
    """Best-effort local IP -- prefers the interface that can reach the internet."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


# ─────────────────────────────────────────────────────────────
# Non-blocking system stats
# ─────────────────────────────────────────────────────────────

class SystemMonitor:
    """
    Samples CPU/memory in a background daemon thread.
    Never called from the event loop -- zero blocking.
    """

    def __init__(self, interval: float = 4.0):
        self._interval = interval
        self._cpu: float = 0.0
        self._memory: float = 0.0
        self._load1: float = 0.0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        psutil.cpu_percent()  # prime — first call always returns 0
        self._thread = threading.Thread(target=self._run, daemon=True, name="hzl-sysmon")
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                cpu = psutil.cpu_percent()
                mem = psutil.virtual_memory().percent
                try:
                    load1 = os.getloadavg()[0] * 100 / max(psutil.cpu_count(), 1)
                except (AttributeError, OSError):
                    # Windows doesn't have getloadavg — use cpu as proxy
                    load1 = cpu
                with self._lock:
                    self._cpu = cpu
                    self._memory = mem
                    self._load1 = load1
            except Exception as e:
                logger.debug(f"[SysMonitor] Sample error: {e}")

    def stop(self) -> None:
        self._stop.set()

    @property
    def cpu(self) -> float:
        with self._lock:
            return self._cpu

    @property
    def memory(self) -> float:
        with self._lock:
            return self._memory

    @property
    def load1(self) -> float:
        with self._lock:
            return self._load1

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "cpu_percent": self._cpu,
                "memory_percent": self._memory,
                "load1_percent": self._load1,
            }


# ─────────────────────────────────────────────────────────────
# Data model
# ─────────────────────────────────────────────────────────────

class NodeEvent(Enum):
    JOINED    = "joined"
    RECOVERED = "recovered"
    LOST      = "lost"
    UPDATED   = "updated"


@dataclass
class NodeInfo:
    hostname: str
    ip: str
    role: str                    # "core" | "worker"
    capabilities: List[str]
    port: int
    orchestrator_port: int
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    load1_percent: float = 0.0
    beacon_version: int = BEACON_VERSION
    last_seen: float = field(default_factory=time.monotonic)
    alive: bool = True

    def to_dict(self) -> dict:
        d = asdict(self)
        d["last_seen_ago"] = round(time.monotonic() - self.last_seen, 1)
        return d

    def has_capability(self, cap: Optional[str]) -> bool:
        if cap is None:
            return True
        return cap in self.capabilities

    @classmethod
    def from_beacon(cls, payload: dict) -> "NodeInfo":
        return cls(
            hostname=payload["hostname"],
            ip=payload["ip"],
            role=payload.get("role", "worker"),
            capabilities=payload.get("capabilities", []),
            port=payload.get("port", 8765),
            orchestrator_port=payload.get("orchestrator_port", 9000),
            cpu_percent=payload.get("cpu_percent", 0.0),
            memory_percent=payload.get("memory_percent", 0.0),
            load1_percent=payload.get("load1_percent", 0.0),
            beacon_version=payload.get("beacon_version", 1),
            last_seen=time.monotonic(),
            alive=True,
        )


# ─────────────────────────────────────────────────────────────
# Async UDP protocol (proper asyncio pattern)
# ─────────────────────────────────────────────────────────────

class _BeaconProtocol(asyncio.DatagramProtocol):
    """asyncio-native UDP datagram handler."""

    def __init__(self, on_receive: Callable[[bytes], None]):
        self._on_receive = on_receive
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport
        sock = transport.get_extra_info("socket")
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def datagram_received(self, data: bytes, addr) -> None:
        try:
            self._on_receive(data)
        except Exception as e:
            logger.debug(f"[Network] Beacon parse error from {addr}: {e}")

    def error_received(self, exc: Exception) -> None:
        logger.warning(f"[Network] UDP protocol error: {exc}")

    def connection_lost(self, exc: Optional[Exception]) -> None:
        if exc:
            logger.error(f"[Network] UDP connection lost: {exc}")


# ─────────────────────────────────────────────────────────────
# Network layer
# ─────────────────────────────────────────────────────────────

EventCallback = Callable[[NodeEvent, NodeInfo], None]


class HZLNetwork:
    """
    Cluster discovery and health tracking via UDP broadcast.
    """

    def __init__(self, config: dict):
        self.config = config
        net = config["network"]

        self.discovery_port: int       = net["discovery_port"]
        self.broadcast_addr: str       = net["broadcast_addr"]
        self.heartbeat_interval: float = float(net["heartbeat_interval"])
        self.heartbeat_jitter: float   = float(net.get("heartbeat_jitter", 1.0))
        self.node_timeout: float       = float(net["node_timeout"])
        self.max_beacon_bytes: int     = int(net.get("max_beacon_bytes", 4096))

        self.hostname: str = socket.gethostname()
        self.ip: str       = get_local_ip()

        self_cfg = config["nodes"].get(self.hostname, {})
        self.role: str             = self_cfg.get("role", "worker")
        self.capabilities: List[str] = self_cfg.get("capabilities", ["inference"])
        self.port: int             = self_cfg.get("port", 8765)
        self.orchestrator_port: int = self_cfg.get("orchestrator_port", 9000)

        self._sysmon = SystemMonitor(interval=max(1.0, self.heartbeat_interval - 0.5))
        self._nodes: Dict[str, NodeInfo] = {}
        self._lock = asyncio.Lock()
        self._callbacks: List[EventCallback] = []
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._running: bool = False

    def on_node_event(self, cb: EventCallback) -> None:
        self._callbacks.append(cb)

    def _fire(self, event: NodeEvent, node: NodeInfo) -> None:
        for cb in self._callbacks:
            try:
                cb(event, node)
            except Exception as e:
                logger.warning(f"[Network] Callback error: {e}")

    def _build_beacon(self) -> bytes:
        stats = self._sysmon.snapshot()
        payload = {
            "type": "hzl_beacon",
            "beacon_version": BEACON_VERSION,
            "hostname": self.hostname,
            "ip": self.ip,
            "role": self.role,
            "capabilities": self.capabilities,
            "port": self.port,
            "orchestrator_port": self.orchestrator_port,
            **stats,
            "timestamp": time.time(),
        }
        data = json.dumps(payload).encode()
        if len(data) > self.max_beacon_bytes:
            logger.error(f"[Network] Beacon too large: {len(data)} bytes")
        return data

    def _handle_beacon(self, data: bytes) -> None:
        try:
            if len(data) > self.max_beacon_bytes:
                return
            payload = json.loads(data.decode())
            if payload.get("type") != "hzl_beacon":
                return
            if payload.get("beacon_version", 1) != BEACON_VERSION:
                return
            peer = payload.get("hostname")
            if not peer or peer == self.hostname:
                return
            self._loop.create_task(self._update_node(payload))
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

    async def _update_node(self, payload: dict) -> None:
        peer = payload["hostname"]
        async with self._lock:
            existing = self._nodes.get(peer)
            was_dead = existing is not None and not existing.alive
            is_new   = existing is None
            self._nodes[peer] = NodeInfo.from_beacon(payload)
            node = self._nodes[peer]

        # Fire callbacks outside the lock to prevent deadlocks
        if is_new:
            logger.info(f"[Network] Node JOINED: {peer} ({payload['ip']}) caps={payload.get('capabilities', [])}")
            self._fire(NodeEvent.JOINED, node)
        elif was_dead:
            logger.info(f"[Network] Node RECOVERED: {peer}")
            self._fire(NodeEvent.RECOVERED, node)
        else:
            self._fire(NodeEvent.UPDATED, node)

    async def _register_self(self) -> None:
        stats = self._sysmon.snapshot()
        async with self._lock:
            self._nodes[self.hostname] = NodeInfo(
                hostname=self.hostname,
                ip=self.ip,
                role=self.role,
                capabilities=self.capabilities,
                port=self.port,
                orchestrator_port=self.orchestrator_port,
                **stats,
                alive=True,
            )
        logger.info(
            f"[Network] Self: {self.hostname} ({self.ip}) "
            f"role={self.role} caps={self.capabilities}"
        )

    async def _broadcast_loop(self) -> None:
        while self._running:
            if self._transport:
                try:
                    self._transport.sendto(
                        self._build_beacon(),
                        (self.broadcast_addr, self.discovery_port),
                    )
                    async with self._lock:
                        if self.hostname in self._nodes:
                            s = self._sysmon.snapshot()
                            n = self._nodes[self.hostname]
                            n.cpu_percent    = s["cpu_percent"]
                            n.memory_percent = s["memory_percent"]
                            n.load1_percent  = s["load1_percent"]
                            n.last_seen      = time.monotonic()
                except Exception as e:
                    logger.warning(f"[Network] Broadcast error: {e}")

            jitter = random.uniform(-self.heartbeat_jitter, self.heartbeat_jitter)
            await asyncio.sleep(max(1.0, self.heartbeat_interval + jitter))

    async def _watchdog_loop(self) -> None:
        while self._running:
            now = time.monotonic()
            lost_nodes = []
            async with self._lock:
                for hostname, node in list(self._nodes.items()):
                    if hostname == self.hostname:
                        continue
                    age = now - node.last_seen
                    if node.alive and age > self.node_timeout:
                        node.alive = False
                        logger.warning(
                            f"[Network] Node LOST: {hostname} "
                            f"(silent for {int(age)}s)"
                        )
                        lost_nodes.append(node)
            # Fire callbacks outside the lock to prevent deadlocks
            for node in lost_nodes:
                self._fire(NodeEvent.LOST, node)
            await asyncio.sleep(self.heartbeat_interval)

    async def start(self) -> None:
        self._running = True
        self._sysmon.start()
        await self._register_self()

        loop = asyncio.get_running_loop()
        self._loop = loop
        self._transport, _ = await loop.create_datagram_endpoint(
            lambda: _BeaconProtocol(on_receive=self._handle_beacon),
            local_addr=("", self.discovery_port),
            allow_broadcast=True,
        )
        logger.info(f"[Network] UDP endpoint on :{self.discovery_port}")

        await asyncio.gather(
            self._broadcast_loop(),
            self._watchdog_loop(),
        )

    def stop(self) -> None:
        self._running = False
        self._sysmon.stop()
        if self._transport:
            self._transport.close()

    async def get_all_nodes(self) -> Dict[str, NodeInfo]:
        async with self._lock:
            return dict(self._nodes)

    async def get_live_nodes(self) -> Dict[str, NodeInfo]:
        async with self._lock:
            return {h: n for h, n in self._nodes.items() if n.alive}

    async def get_workers(self) -> Dict[str, NodeInfo]:
        async with self._lock:
            return {h: n for h, n in self._nodes.items() if n.alive and n.role == "worker"}

    async def get_node(self, hostname: str) -> Optional[NodeInfo]:
        async with self._lock:
            return self._nodes.get(hostname)

    async def summary(self) -> dict:
        async with self._lock:
            live = [n for n in self._nodes.values() if n.alive]
            workers = [n for n in live if n.role == "worker"]
            return {
                "total_known": len(self._nodes),
                "live": len(live),
                "workers_live": len(workers),
                "nodes": {h: n.to_dict() for h, n in self._nodes.items()},
            }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    cfg = load_config()
    net = HZLNetwork(cfg)
    net.on_node_event(
        lambda ev, node: print(f"  EVENT: {ev.value} -> {node.hostname}")
    )
    try:
        asyncio.run(net.start())
    except KeyboardInterrupt:
        net.stop()
        print("\n[Network] Stopped.")
