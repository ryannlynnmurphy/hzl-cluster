"""
hzl-cluster -- Distributed AI routing and orchestration for Pi clusters.

Usage:
    from hzl_cluster import HZLNetwork, HZLRouter, HZLOrchestrator
    from hzl_cluster.integration import get_routing_context, record_routing_outcome
"""

from hzl_cluster.network import (
    HZLNetwork,
    NodeInfo,
    NodeEvent,
    SystemMonitor,
    load_config,
    get_local_ip,
)
from hzl_cluster.router import (
    HZLRouter,
    RoutingDecision,
    CircuitBreaker,
    classify_task,
)
from hzl_cluster.orchestrator import HZLOrchestrator
from hzl_cluster.queue_hub import (
    HazelMessage,
    QueueDB,
    QueueHub,
)

__version__ = "1.0.0"
__all__ = [
    "HZLNetwork",
    "HZLRouter",
    "HZLOrchestrator",
    "NodeInfo",
    "NodeEvent",
    "RoutingDecision",
    "CircuitBreaker",
    "SystemMonitor",
    "load_config",
    "get_local_ip",
    "classify_task",
    "HazelMessage",
    "QueueDB",
    "QueueHub",
]
