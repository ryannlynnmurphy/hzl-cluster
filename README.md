# hzl-cluster

Distributed AI routing and orchestration for Raspberry Pi clusters.

Routes Claude API calls (or any AI API) across a cluster of Pis based on node health, capability, task type, and circuit breaker state. Zero-config node discovery via UDP broadcast.

## What it does

- **Node discovery** -- UDP beacon broadcast, nodes auto-join the cluster
- **Task classification** -- regex-based classifier maps user text to task types (zero API cost)
- **Smart routing** -- picks the best healthy node with a 4-level fallback chain
- **Circuit breakers** -- per-node failure tracking, auto-bypass flapping nodes
- **Health monitoring** -- CPU/memory sampling in a background thread (never blocks async)
- **REST API** -- `/route`, `/outcome`, `/status`, `/health`, `/circuit-breakers`
- **Client library** -- singleton session, client-side circuit breaker, retry with backoff

## Install

```bash
pip install git+https://github.com/ryannlynnmurphy/hzl-cluster.git
```

Or clone and install in dev mode:

```bash
git clone https://github.com/ryannlynnmurphy/hzl-cluster.git
cd hzl-cluster
pip install -e .
```

## Quick start

1. Copy `example_config.yaml` to your project and edit node definitions
2. Set `HZL_CONFIG` env var to your config path

```python
from hzl_cluster import load_config, HZLOrchestrator

config = load_config("/path/to/hzl_config.yaml")
orch = HZLOrchestrator(config)
asyncio.run(orch.run())
```

Client side:

```python
from hzl_cluster.integration import get_routing_context, record_routing_outcome

ctx = await get_routing_context("write me a haiku")
# ctx.model, ctx.max_tokens, ctx.task_type, ctx.node_hostname

# After your API call:
record_routing_outcome(ctx, success=True, latency_ms=340)
```

## Architecture

```
User text --> classify_task() --> task_type
                                    |
                              HZLRouter.route()
                                    |
                    +---------------+---------------+
                    |               |               |
              preferred_node   any_capable     core_fallback
                    |               |               |
                    +-------+-------+               |
                            |                       |
                      best healthy node      cloud_direct
                      (lowest score)         (all down)
```

## Node roles

| Role | Description |
|------|-------------|
| `core` | Orchestrator + UI + voice + routing |
| `worker` | Inference, reasoning, heavy tasks |

Workers auto-register via UDP broadcast. No config changes needed on the core when adding workers.

## License

MIT
