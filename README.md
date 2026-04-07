```
╔══════════════════════════════════════════════════════════════════╗
║                                                                  ║
║    ██╗  ██╗ █████╗ ███████╗███████╗██╗      ██████╗ ███████╗   ║
║    ██║  ██║██╔══██╗╚══███╔╝██╔════╝██║     ██╔═══██╗██╔════╝   ║
║    ███████║███████║  ███╔╝ █████╗  ██║     ██║   ██║███████╗   ║
║    ██╔══██║██╔══██║ ███╔╝  ██╔══╝  ██║     ██║   ██║╚════██║   ║
║    ██║  ██║██║  ██║███████╗███████╗███████╗╚██████╔╝███████║   ║
║    ╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝╚══════╝╚══════╝ ╚═════╝ ╚══════╝  ║
║                                                                  ║
║               A I R - G A P P E D   C L U S T E R              ║
╚══════════════════════════════════════════════════════════════════╝
```

**Air-gapped AI cluster infrastructure for the world's first security-first, AI-native operating system.**

> *Ryann Murphy built me in March 2026 having never written a line of code before February. My name is Hazel. I live on a Raspberry Pi 5 in a small apartment. I run on-device speech recognition via Whisper. Her memory, her patterns, her 2am thoughts -- those live in a SQLite database that belongs to her. She is building a company around closing the gap between what AI promises and what it costs you when you're not looking.*
>
> *-- Hazel, from ["Mostly"](https://github.com/ryannlynnmurphy)*

---

## What is this?

`hzl-cluster` is the infrastructure that makes Hazel sovereign. It manages a Raspberry Pi cluster with physical air-gap security -- not a firewall rule, but a GPIO-controlled relay that electrically disconnects the network between internet access windows. The cluster is voice-first: saying "Hazel, check my email" triggers a full sync cycle where the relay opens, content is fetched and scanned for threats, and the relay closes before anything reaches the core. Every query, every memory, every file stays on hardware you own.

Built by a playwright who realized that theater and software are the same discipline in different materials. Every system has a structure. Every structure is an argument. The question is always whose argument, built for whom, and what it costs you when you're not looking.

No cloud dependency. No always-on internet. No persistent attack surface. The intelligence belongs to the person inside.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  ZONE A: AIR-GAPPED CLUSTER                                     │
│                                                                 │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐       │
│  │  hazel-core  │   │  worker-01   │   │  worker-02   │       │
│  │  :9000       │   │  inference   │   │  inference   │       │
│  │  voice, hub  │   │  reasoning   │   │  reasoning   │       │
│  │  queue, ui   │   │              │   │              │       │
│  └──────────────┘   └──────────────┘   └──────────────┘       │
│          ▲                  ▲                   ▲               │
│          └──────────────────┴───────────────────┘               │
│                     UDP beacon discovery                        │
│                                                                 │
│         ┌─────────────────┐                                     │
│         │   hazel-phone   │ (when docked — voice + camera)      │
│         └─────────────────┘                                     │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                     USB RELAY (GPIO pin 17)
                     physical air-gap — Ethernet
                     and WiFi are never active
                     simultaneously
                              │
┌─────────────────────────────┴───────────────────────────────────┐
│  ZONE B: GATEWAY                                                │
│                                                                 │
│  ┌──────────────────────────────────┐                          │
│  │  hazel-gateway          :9010    │                          │
│  │                                  │                          │
│  │  RelayController  ─── GPIO       │  WiFi → Internet         │
│  │  GatewayDaemon    ─── REST API   │  (only when relay open)  │
│  │  ContentScanner   ─── quarantine │                          │
│  │  QueueHub         ─── SQLite     │                          │
│  │  Fetchers         ─── 7 types    │                          │
│  └──────────────────────────────────┘                          │
└─────────────────────────────────────────────────────────────────┘
```

Routing flow:

```
Voice / text input
      │
      ▼
 classify_task()          regex-based, zero API cost
      │
      ▼
 HZLRouter.route()
      │
      ├─── preferred_node  (task config specifies a target)
      │
      ├─── any_capable_node  (lowest CPU/memory score wins)
      │
      ├─── core_fallback  (hazel-core always accepts)
      │
      └─── cloud_direct  (all nodes down — go to API)
```

---

## Features

- Physical air-gap via GPIO-controlled USB relay — Ethernet and WiFi are mutually exclusive at the hardware level
- UDP beacon node discovery — zero configuration, workers auto-join on boot
- Intelligent task routing with 4-level fallback chain
- Three-state circuit breakers per node (closed / open / half-open) with auto-recovery
- `HazelMessage` queue protocol backed by SQLite WAL — durable, prioritized, TTL-aware
- Content scanning and quarantine before any fetched file reaches the cluster (extension blocklist, PE magic byte check, size cap)
- Seven real fetchers: weather (Open-Meteo), news (RSS), email (IMAP), podcasts (RSS enclosures), map tiles (OSM), pip packages, and CalDAV calendar
- Real-time ANSI terminal dashboard with per-node CPU/memory bars, circuit breaker state, queue depth, and relay status
- One-command deployment: `python -m hzl_cluster.deploy --role core` — sets hostname, writes config, installs systemd service
- Voice-first: "Hazel, check my email" triggers the full sync cycle end to end
- Watchdog enforces maximum internet session duration (default 10 minutes), then closes the relay automatically
- Emergency disconnect API endpoint for immediate relay closure regardless of state
- Structured JSON logging throughout
- 100 tests

---

## Quick Start

Install:

```bash
pip install git+https://github.com/ryannlynnmurphy/hzl-cluster.git

# With GPIO support (Gateway Pi only):
pip install "git+https://github.com/ryannlynnmurphy/hzl-cluster.git#egg=hzl-cluster[gateway]"
```

Or clone for development:

```bash
git clone https://github.com/ryannlynnmurphy/hzl-cluster.git
cd hzl-cluster
pip install -e ".[dev]"
```

**Deploy each node in one command:**

```bash
# Core node (orchestrator + voice + queue hub)
sudo python -m hzl_cluster.deploy --role core

# Worker nodes (inference + reasoning)
sudo python -m hzl_cluster.deploy --role worker --name hazel-worker-01
sudo python -m hzl_cluster.deploy --role worker --name hazel-worker-02

# Gateway node (relay + fetchers + scanner)
sudo python -m hzl_cluster.deploy --role gateway
```

Deploy sets the hostname, creates `/var/hazel/` and `/etc/hazel/`, writes a role-specific config to `/etc/hazel/hzl_config.yaml`, and installs a systemd service.

**Start services:**

```bash
sudo systemctl start hazel-core
sudo systemctl start hazel-worker
sudo systemctl start hazel-gateway
```

**Check cluster status:**

```bash
python -m hzl_cluster.deploy status
```

**Launch the live dashboard:**

```bash
python -m hzl_cluster.dashboard
python -m hzl_cluster.dashboard --host 192.168.10.1 --gateway-host 192.168.10.4
```

**Use from Python:**

```python
from hzl_cluster import load_config, HZLOrchestrator

config = load_config("/etc/hazel/hzl_config.yaml")
orch = HZLOrchestrator(config)
asyncio.run(orch.run())
```

Client-side routing:

```python
from hzl_cluster.integration import get_routing_context, record_routing_outcome

ctx = await get_routing_context("summarize this document")
# ctx.model, ctx.max_tokens, ctx.task_type, ctx.node_hostname

# After your API call:
record_routing_outcome(ctx, success=True, latency_ms=820)
```

---

## Module Map

| Module | Role | Port |
|---|---|---|
| `orchestrator.py` | REST API hub — `/route`, `/outcome`, `/status`, `/nodes`, `/queue`, `/health` | 9000 |
| `network.py` | UDP beacon broadcast and discovery, node registry, `SystemMonitor` background thread | 9099 (UDP) |
| `router.py` | Task classification, node scoring, 4-level fallback chain, `CircuitBreaker`, latency metrics | — |
| `gateway.py` | `GatewayDaemon` — REST API, sync cycle orchestration, fetcher dispatch | 9010 |
| `relay.py` | `RelayController` — GPIO relay state machine, watchdog, audit log | — |
| `queue_hub.py` | `HazelMessage`, `QueueDB` (SQLite WAL), `QueueHub` — durable inter-node message broker | — |
| `scanner.py` | `ContentScanner` — extension blocklist, PE magic bytes, size cap, quarantine | — |
| `dashboard.py` | ANSI terminal dashboard — nodes, CPU/memory bars, queue depth, relay state, metrics | — |
| `deploy.py` | One-command node provisioning — hostname, directories, config, systemd service | — |
| `integration.py` | Thin client library — `get_routing_context`, `record_routing_outcome`, retry with backoff | — |
| `fetchers/` | Seven data fetchers (see table below) | — |

---

## Fetchers

All fetchers run exclusively on the Gateway node during a sync window. All output is staged to disk and scanned before delivery to the cluster.

| Fetcher | API / Protocol | Output |
|---|---|---|
| `weather_fetcher.py` | Open-Meteo REST (free, no key) | JSON forecast — temperature, precipitation, wind, hourly breakdown |
| `news_fetcher.py` | RSS/Atom (configurable feeds) | JSON articles per feed — title, link, summary, published date |
| `email_fetcher.py` | IMAP (SSL optional) | JSON messages — from, subject, date, body; supports ProtonMail Bridge, Gmail, Fastmail |
| `podcast_fetcher.py` | RSS enclosures | Downloaded audio files + JSON episode manifest |
| `map_fetcher.py` | OpenStreetMap tile server | PNG tiles organized by zoom/x/y for offline map rendering |
| `package_fetcher.py` | pip download subprocess | Wheel files + `manifest.json` for offline installation on cluster nodes |
| `calendar_fetcher.py` | CalDAV (Nextcloud, Radicale, iCloud, Google via bridge) | JSON events — title, start, end, location, description |

---

## Voice Commands

Voice input reaches `hazel-core`, gets classified by `classify_task()`, and is routed to the appropriate node or triggers a gateway sync. No API call is made for classification — it is pure regex pattern matching.

| What you say | Task type | What happens |
|---|---|---|
| "Hazel, check my email" | `gateway_sync` | Full sync cycle — relay opens, IMAP fetch, scan, relay closes |
| "What's the weather this week?" | `gateway_fetch` | Queue a `fetch.weather` message to the gateway |
| "Summarize this document" | `heavy_inference` | Routed to a worker node with the highest inference capacity |
| "Write a haiku about the ocean" | `voice_response` | Routed to hazel-core, fast model, short timeout |
| "Reason through this problem step by step" | `reasoning` | Routed to any worker with reasoning capability |
| "Download the numpy wheel" | `gateway_fetch` | Queue a `fetch.packages` message to the gateway |
| "What's on my calendar tomorrow?" | `gateway_fetch` | Queue a `fetch.calendar` message to the gateway |

---

## Security Model

The air-gap is physical, not software. The relay is a hardware switch controlled by a GPIO pin on the Gateway Pi. When it is closed, the Gateway's Ethernet port is electrically connected to the cluster switch. When it is open, it is not — regardless of any software state.

**Sync cycle:**

1. Core queues a `HazelMessage` with `destination: gateway` and the desired `action` (e.g. `fetch.email`).
2. Gateway detects the message, calls `relay.enter_internet_mode()` — GPIO goes HIGH, WiFi comes up via `nmcli`.
3. Fetchers run. All output lands in `/var/hazel/staging/`.
4. `ContentScanner` walks the staging directory. Any file matching a blocked extension (`.exe`, `.bat`, `.ps1`, `.dll`, `.sh`, and 20+ others), containing PE magic bytes (`MZ`), or exceeding the size cap is moved to `/var/hazel/quarantine/` and never delivered.
5. `relay.enter_core_mode()` — WiFi goes down, GPIO goes LOW, Ethernet reconnects.
6. Clean files are delivered to the cluster.

A watchdog timer enforces a maximum internet session length (default 600 seconds). If the sync overruns, the watchdog fires `enter_core_mode()` automatically. An emergency disconnect endpoint (`POST /emergency`) drops the connection immediately regardless of state.

The relay can also be `lock()`ed programmatically — while locked, no software call can open it.

---

## REST API Reference

**Orchestrator (hazel-core :9000)**

| Method | Path | Description |
|---|---|---|
| `POST` | `/route` | Route a task — returns model, node, max_tokens, task_type |
| `POST` | `/outcome` | Record task outcome — updates circuit breakers and metrics |
| `GET` | `/status` | All nodes with CPU, memory, capabilities, circuit breaker state |
| `GET` | `/nodes` | Node registry (alive/dead, last seen) |
| `GET` | `/queue` | Queue depth by destination |
| `GET` | `/health` | `{"status": "ok", "role": "core"}` |
| `GET` | `/circuit-breakers` | Per-node circuit breaker states |

**Gateway (hazel-gateway :9010)**

| Method | Path | Description |
|---|---|---|
| `POST` | `/request` | Queue a `HazelMessage` fetch request |
| `GET` | `/queue` | Queue status |
| `POST` | `/sync` | Trigger a sync cycle immediately |
| `GET` | `/state` | Relay state + queue depth + staging directory listing |
| `GET` | `/staging` | Files waiting in staging |
| `GET` | `/health` | `{"status": "ok", "role": "gateway"}` |
| `POST` | `/lock` | Lock the relay (no software can open it while locked) |
| `POST` | `/unlock` | Unlock the relay |
| `POST` | `/emergency` | Emergency disconnect — closes relay immediately |

---

## Configuration

`deploy` generates a full config automatically. The relevant sections:

```yaml
cluster:
  name: HZL
  core_node: hazel-core

routing:
  task_map:
    voice_response:
      model: claude-haiku-4-5-20251001
      preferred_node: hazel-core
      capability: voice
      max_tokens: 500
      timeout: 8
    heavy_inference:
      model: claude-sonnet-4-6
      preferred_node: any_worker
      capability: inference
      max_tokens: 4000
      timeout: 60
  fallback_chain: [preferred_node, any_capable_node, core, cloud_direct]

relay:
  gpio_pin: 17
  max_internet_duration: 600   # watchdog fires after 10 minutes
  wifi_interface: wlan0
  ethernet_interface: eth0

sync:
  schedule: "0 6 * * *"
  staging_dir: /var/hazel/staging
  quarantine_dir: /var/hazel/quarantine
  max_staging_size_mb: 500
```

Set `HZL_CONFIG` to point to your config file:

```bash
export HZL_CONFIG=/etc/hazel/hzl_config.yaml
```

---

## Requirements

- Python 3.10+
- Raspberry Pi 5 (recommended) or any ARM/x86 Linux host
- `aiohttp >= 3.9`, `psutil >= 5.9`, `pyyaml >= 6.0`
- `gpiozero >= 2.0` — Gateway Pi only, optional (relay runs in simulation mode without it)
- `pytest`, `pytest-asyncio` — development only

---

## Running Tests

```bash
pytest tests/ -v
```

100 tests across all modules: network, router, orchestrator, gateway, relay, queue hub, scanner, dashboard, deploy, and all seven fetchers. All fetcher tests have a `simulate=True` mode and run without network access.

---

## Project Structure

```
hzl-cluster/
├── hzl_cluster/
│   ├── __init__.py          public API surface
│   ├── orchestrator.py      core REST API
│   ├── network.py           UDP discovery + health
│   ├── router.py            task routing + circuit breakers
│   ├── gateway.py           gateway daemon + REST API
│   ├── relay.py             GPIO relay state machine
│   ├── queue_hub.py         inter-node message queue
│   ├── scanner.py           content scanning + quarantine
│   ├── dashboard.py         terminal dashboard
│   ├── deploy.py            node provisioning CLI
│   ├── integration.py       client library
│   └── fetchers/
│       ├── weather_fetcher.py
│       ├── news_fetcher.py
│       ├── email_fetcher.py
│       ├── podcast_fetcher.py
│       ├── map_fetcher.py
│       ├── package_fetcher.py
│       └── calendar_fetcher.py
├── tests/                   100 tests
├── example_config.yaml
└── pyproject.toml
```

---

## License

MIT

---

## Built by

**Ryann Murphy** -- playwright, technologist, founder of HZL Studio.

She taught herself to code in February 2026 and shipped a patent-pending learning platform, a distributed inference cluster, a hardware-integrated creative studio, and a voice assistant named Hazel in three months. She is not embarrassed about how she learned. She is building the infrastructure for a multi-domain practice spanning AI-integrated homes, entertainment, and the built environment. The compute heats the water. The heat exchanger is load-bearing. The intelligence belongs to the person inside.

[hzlstudio.com](https://hzlstudio.com) -- [github.com/ryannlynnmurphy](https://github.com/ryannlynnmurphy)
