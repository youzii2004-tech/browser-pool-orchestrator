# Browser Pool Orchestrator

> **Enterprise Distributed Headless Fleet & Chrome DevTools Protocol (CDP) Session Architecture**  
> *Architected by Muhammad Usaid Siddiqui*

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Test Suite](https://img.shields.io/badge/Tests-28%20Passed%20%7C%20100%25-emerald.svg)](tests/)
[![Fleet Topology](https://img.shields.io/badge/Fleet-45%20Concurrent%20Nodes-cyan.svg)](#fleet-topology--port-isolation-matrix)
[![Process Integrity](https://img.shields.io/badge/Supervisor-Zero%20Zombie%20PIDs-green.svg)](#zero-zombie-process-supervision)

---

## 1. Executive Overview

**Browser Pool Orchestrator** is an enterprise-grade distributed infrastructure engine engineered to manage, supervise, and coordinate **45+ concurrent headless Chromium instances** under sustained production workloads.

Operating a high-density headless browser fleet at scale introduces severe operational challenges:
- **Memory Bloat & Zombie Process Proliferation:** Unhandled renderer crashes, GPU process leaks, and dangling child workers accumulate over prolonged batch execution, rapidly exhausting host RAM and kernel PID tables.
- **Session & Profile Contamination:** Shared or poorly isolated browser contexts cause state leakage, authentication race conditions, and filesystem locking errors.
- **Aggressive Rate-Limit & Bot Defense Barriers:** Blanket client throttling, HTTP 429 quota exhaustion, and correlated network fingerprinting bring unmanaged fleets to a halt.
- **False-Alarm vs. Hard-Limit Conflation:** Systems that blindly treat soft transient content rejections as hard account bans waste valuable execution throughput.

**Browser Pool Orchestrator** resolves these failure modes through a multi-tier supervisory architecture:
1. **Deterministic Port & Data Isolation:** Allocates non-colliding remote debugging ports (`9222` to `9266`) and discrete user-data directories across 45 designated slot nodes.
2. **Process Hierarchy Supervision:** Tracks root Chromium PIDs and all child worker processes (`renderer`, `gpu-process`, `utility`), guaranteeing clean teardown and zero zombie processes upon lease termination.
3. **Low-Level Asynchronous CDP Client:** Dispatches raw Chrome DevTools Protocol (CDP) JSON-RPC 2.0 commands over WebSockets with millisecond latency tracking and request-response correlation.
4. **Self-Healing Resilience Engine:** Intelligently classifies execution failures into transient false alarms versus genuine account rate limits (HTTP 429), automating quarantine holding states, exponential backoff with jitter, and auto-cooldown decay.
5. **AsyncIO Concurrency Dispatcher:** Regulates batch job queues with configurable concurrency caps, automatic retries, and fleet telemetry reporting.
6. **Dark Obsidian Operations Dashboard:** A responsive 1920x1080 glassmorphic control room featuring a 5x9 fleet topology matrix, live CDP event stream terminal, and interactive self-healing triggers.

---

## 2. System Architecture

```text
                             ┌────────────────────────────────────────────────────────┐
                             │              ENTERPRISE BATCH DISPATCHER               │
                             │       (AsyncIO Queue · Semaphore Concurrency Cap)      │
                             └───────────────────────────┬────────────────────────────┘
                                                         │
                                                         ▼
                             ┌────────────────────────────────────────────────────────┐
                             │              FLEET SUPERVISOR (PoolManager)            │
                             │   • 45 Dedicated Node Slots (Node 00 – Node 44)        │
                             │   • Dedicated CDP Debugging Ports (9222 – 9266)        │
                             │   • Strict Parent-Child Process Hierarchy Tracking     │
                             │   • Zero-Zombie Scan & Orphaned PID Elimination        │
                             └──────┬────────────────────┬────────────────────┬───────┘
                                    │                    │                    │
                   ┌────────────────┴────────┐           │           ┌────────┴────────────────┐
                   ▼                         ▼           │           ▼                         ▼
         ┌───────────────────┐     ┌───────────────────┐ │ ┌───────────────────┐     ┌───────────────────┐
         │ Node Slot 00      │     │ Node Slot 01      │ │ │ Node Slot 43      │     │ Node Slot 44      │
         │ Port: 9222        │     │ Port: 9223        │...│ Port: 9265        │     │ Port: 9266        │
         │ Root PID: 50000   │     │ Root PID: 50001   │ │ │ Root PID: 50043   │     │ Root PID: 50044   │
         │ Children: [R,G,U] │     │ Children: [R,G,U] │ │ │ Children: [R,G,U] │     │ Children: [R,G,U] │
         └─────────┬─────────┘     └─────────┬─────────┘ │ └─────────┬─────────┘     └─────────┬─────────┘
                   │                         │           │           │                         │
                   └────────────────┬────────┴───────────┼───────────┴────────┬────────────────┘
                                    │                    │                    │
                                    ▼                    ▼                    ▼
                             ┌────────────────────────────────────────────────────────┐
                             │         LOW-LEVEL CDP WEBSOCKET CLIENT BUS             │
                             │   JSON-RPC 2.0 Commands (Page, DOM, Input, Runtime)    │
                             └───────────────────────────┬────────────────────────────┘
                                                         │
                                                         ▼
                             ┌────────────────────────────────────────────────────────┐
                             │               SELF-HEALING RESILIENCE ENGINE           │
                             │   ┌────────────────────────────────────────────────┐   │
                             │   │ Anomaly Classifier: False Alarm vs. HTTP 429   │   │
                             │   ├────────────────────────────────────────────────┤   │
                             │   │ Quarantine Holding State (300s–600s)           │   │
                             │   ├────────────────────────────────────────────────┤   │
                             │   │ Exponential Backoff Multiplier (2.0x) + Jitter │   │
                             │   ├────────────────────────────────────────────────┤   │
                             │   │ Automatic Cooldown Decay & Health Restoration  │   │
                             │   └────────────────────────────────────────────────┘   │
                             └────────────────────────────────────────────────────────┘
```

---

## 3. Node State Machine & Lifecycle

Every node in the 45-instance fleet transitions deterministically through an explicit lifecycle managed by the supervisor and resilience engine:

```text
                  ┌───────────────┐
                  │    OFFLINE    │
                  └───────┬───────┘
                          │ Boot & Health Probe
                          ▼
                  ┌───────────────┐
         ┌───────►│     IDLE      │◄──────────────────────────┐
         │        │   (Healthy)   │                           │
         │        └───────┬───────┘                           │
         │                │ Lease Acquired                    │
         │                ▼                                   │
         │        ┌───────────────┐                           │
         │        │     BUSY      │                           │
         │        │  (Executing)  │                           │
         │        └───┬───────┬───┘                           │
         │            │       │                               │
  Success Release     │       │ Anomaly Detected              │
         │            │       ▼                               │
         │            │   ┌────────────────────────┐          │
         │            │   │   Resilience Engine    │          │
         │            │   │   Anomaly Classifier   │          │
         │            │   └───────┬────────┬───────┘          │
         │            │           │        │                  │
         │            │           │        │ Hard Limit (429) │
         │            │           │        ▼                  │
         │            │           │ ┌───────────────┐         │
         │            │           │ │  QUARANTINED  │         │
         │            │           │ │ (Holding 300s)│         │
         │            │           │ └───────┬───────┘         │
         │            │           │         │ Cooldown Decay  │
         │            │           ▼         ▼                 │
         │            │     ┌───────────────┐                 │
         │            └────►│   COOLDOWN    │─────────────────┘
         │                  │ (Recovery 30s)│   Probe Nominal
         └──────────────────┴───────────────┘
```

- **IDLE:** Node is warmed, remote debugging WebSocket port is bound, and node is ready for allocation.
- **BUSY:** Task lease granted; CDP session active; parent and child PIDs actively monitored.
- **COOLDOWN:** Soft recovery state following a transient false-alarm content rejection or minor timeout. Automatically restored after soft cooldown period.
- **QUARANTINED:** Strict holding state triggered by genuine account rate-limits (HTTP 429) or repeated consecutive failures. Isolates the slot, prevents correlated fingerprint blocks, and safely applies exponential backoff with jitter.
- **TERMINATING / OFFLINE:** Teardown state where supervisor verifies process termination and purges orphaned PIDs.

---

## 4. Key Engineering Pillars

### Zero-Zombie Process Supervision
In long-running headless browser deployments, child processes (`chrome.exe` renderers, GPU helpers, utility workers) often fail to terminate when their parent process terminates abnormally, leading to phantom memory leakage and CPU exhaustion.
- **Parent-Child Tree Registration:** When a node slot is leased, the orchestrator registers both the root PID and child worker PIDs.
- **Supervisor Tree Purge:** During slot deallocation and routine supervisor sweeps, the supervisor scans the OS process table and executes targeted SIGTERM / SIGKILL escalation on lingering PIDs.
- **Empirical Clean Guarantee:** Zero lingering PIDs remain in the system upon lease release or fleet shutdown.

### Low-Level Asynchronous CDP Client
Rather than relying on heavyweight, opinionated browser wrappers, the orchestrator communicates directly with Chromium via native JSON-RPC 2.0 WebSockets:
- **Direct Domain Dispatches:** `Page.navigate`, `DOM.querySelector`, `Input.dispatchMouseEvent`, `Runtime.evaluate`, and `Network.setExtraHTTPHeaders`.
- **Latency & Event Telemetry:** Measures round-trip latencies per command in milliseconds, maintaining a running average for real-time performance diagnostics.
- **Async Request-Response Multiplexing:** Associates outbound request IDs with internal `asyncio.Future` promises for thread-safe concurrent execution.

### Self-Healing Anomaly Classifier
Separates catastrophic account limits from transient network glitches:
- **Hard Rate Limits (HTTP 429):** Identifies explicit quota exhaustion strings or HTTP 429 status codes; routes the slot to holding quarantine (300s) and applies a 2.0x backoff multiplier.
- **Soft False Alarms:** Detects transient DOM timing races or temporary content filtering; allows fast retries without penalizing slot quotas.
- **Jittered Backoff:** Adds pseudo-random variation ($\pm 10\%$) to cooldown timers to prevent the "thundering herd" problem when slots recover simultaneously.

---

## 5. Fleet Topology & Port Isolation Matrix

The default fleet configuration defines 45 slots with strict port and storage isolation:

| Slot Range | Debugging Ports | Root User-Data Path | Memory Limit | Target Concurrency |
| :--- | :--- | :--- | :---: | :---: |
| **Node 00 – 08** | `9222 – 9230` | `/var/run/browser_pool/profiles/profile_00..08` | 1024 MB | 9 concurrent |
| **Node 09 – 17** | `9231 – 9239` | `/var/run/browser_pool/profiles/profile_09..17` | 1024 MB | 9 concurrent |
| **Node 18 – 26** | `9240 – 9248` | `/var/run/browser_pool/profiles/profile_18..26` | 1024 MB | 9 concurrent |
| **Node 27 – 35** | `9249 – 9257` | `/var/run/browser_pool/profiles/profile_27..35` | 1024 MB | 9 concurrent |
| **Node 36 – 44** | `9258 – 9266` | `/var/run/browser_pool/profiles/profile_36..44` | 1024 MB | 9 concurrent |

---

## 6. Directory Structure

```text
browser-pool-orchestrator/
├── config/
│   └── pool_config.example.yaml   # 45-node slot declarations & policies
├── dashboard/
│   ├── index.html                 # 1920x1080 Operations Monitor Dashboard
│   └── dashboard_preview.png      # High-resolution UI render preview
├── src/
│   ├── __init__.py                # Package exports
│   ├── cli.py                     # Enterprise command-line interface
│   ├── config.py                  # Pydantic v2 schemas and validation
│   ├── pool_manager.py            # Fleet supervisor and process manager
│   ├── cdp_client.py              # Low-level WebSocket JSON-RPC CDP client
│   ├── resilience_engine.py       # Self-healing anomaly classifier
│   └── scheduler.py               # AsyncIO batch job dispatcher
├── tests/
│   ├── __init__.py
│   ├── test_config.py             # Schema & YAML validation tests
│   ├── test_pool_manager.py       # Allocation, port collision & PID tests
│   ├── test_resilience_engine.py  # Anomaly classification & quarantine tests
│   ├── test_cdp_client.py         # CDP WebSocket & message parsing tests
│   └── test_scheduler.py          # Batch queue & concurrency cap tests
├── .gitignore                     # Strict credential & profile isolation barrier
├── LICENSE                        # MIT License (2026)
├── README.md                      # Comprehensive architecture guide
└── requirements.txt               # Dependencies
```

---

## 7. Quickstart & Installation

### 1. Clone & Install Dependencies
```bash
git clone https://github.com/youzii2004-tech/browser-pool-orchestrator.git
cd browser-pool-orchestrator
pip install -r requirements.txt
```

### 2. Verify Fleet Health
```bash
py src/cli.py health
```

### 3. Inspect Fleet Telemetry
```bash
py src/cli.py status
```

### 4. Execute Asynchronous Batch Dispatcher
```bash
py src/cli.py dispatch --tasks 20 --concurrency 10
```

### 5. Launch Operations Mission Control Monitor
```bash
py src/cli.py serve --port 8080
```
Then navigate to `http://localhost:8080` in any web browser.

---

## 8. CLI Command Reference

The orchestrator CLI (`browserpool` / `src/cli.py`) provides commands for fleet administration:

| Command | Arguments | Description |
| :--- | :--- | :--- |
| `status` | `[--config PATH]` | Renders tabular status of all 45 nodes, memory metrics, and port mappings |
| `health` | `[--config PATH]` | Audits all 45 CDP remote debugging ports and validates isolation paths |
| `purge-zombies`| `[--config PATH]` | Scans process tree, terminates orphaned renderers, and verifies zero zombie state |
| `dispatch` | `--tasks N --concurrency C` | Runs parallel batch jobs across available node slots with concurrency caps |
| `serve` | `--port P` | Boots internal HTTP server hosting the 1080p Operations Dashboard |

---

## 9. Test Suite Verification

The repository includes a comprehensive unit and integration test suite covering schema validation, process hierarchies, CDP protocol framing, and resilience classification:

```bash
py -m pytest tests -v
```

```text
============================= test session starts =============================
platform win32 -- Python 3.11.3, pytest-9.1.1, pluggy-1.6.0
collected 28 items

tests/test_cdp_client.py::test_cdp_client_connect_and_disconnect PASSED        [  3%]
tests/test_cdp_client.py::test_cdp_client_send_mock_navigation PASSED          [  7%]
tests/test_cdp_client.py::test_cdp_client_dom_and_runtime PASSED               [ 10%]
tests/test_cdp_client.py::test_cdp_client_raw_message_parsing PASSED           [ 14%]
tests/test_cdp_client.py::test_cdp_event_listener_dispatch PASSED              [ 17%]
tests/test_config.py::test_port_range_valid PASSED                             [ 21%]
tests/test_config.py::test_port_range_invalid_bounds PASSED                    [ 25%]
tests/test_config.py::test_port_range_slot_index_error PASSED                  [ 28%]
tests/test_config.py::test_node_config_validation PASSED                       [ 32%]
tests/test_config.py::test_node_config_empty_id PASSED                         [ 35%]
tests/test_config.py::test_pool_config_generation PASSED                       [ 39%]
tests/test_config.py::test_pool_config_port_shortage PASSED                    [ 42%]
tests/test_config.py::test_pool_config_duplicate_ports PASSED                  [ 46%]
tests/test_config.py::test_pool_config_yaml_roundtrip PASSED                   [ 50%]
tests/test_pool_manager.py::test_pool_manager_initialization_45_slots PASSED   [ 53%]
tests/test_pool_manager.py::test_allocate_and_release_lifecycle PASSED         [ 57%]
tests/test_pool_manager.py::test_quarantine_holding_state PASSED               [ 60%]
tests/test_pool_manager.py::test_pid_supervision_and_zombie_purge PASSED       [ 64%]
tests/test_pool_manager.py::test_telemetry_reporting PASSED                   [ 67%]
tests/test_resilience_engine.py::test_classify_hard_rate_limit_http_429 PASSED[ 71%]
tests/test_resilience_engine.py::test_classify_hard_rate_limit_pattern PASSED [ 75%]
tests/test_resilience_engine.py::test_classify_false_alarm_transient PASSED   [ 78%]
tests/test_resilience_engine.py::test_consecutive_failure_escalation PASSED    [ 82%]
tests/test_resilience_engine.py::test_quarantine_tracking_and_expiration PASSED[ 85%]
tests/test_resilience_engine.py::test_exponential_backoff_calculation PASSED   [ 89%]
tests/test_scheduler.py::test_scheduler_batch_execution PASSED                 [ 92%]
tests/test_scheduler.py::test_scheduler_retry_and_resilience PASSED           [ 96%]
tests/test_scheduler.py::test_scheduler_hard_rate_limit_quarantine PASSED     [100%]

============================= 28 passed in 0.62s ==============================
```

---

## 10. Security & Profile Isolation Model

Browser Pool Orchestrator strictly separates configuration from volatile browser state:
- **No Profile Ingestion:** Actual Chromium session stores, user profiles (`chrome_profile_*`), and authentication cookies are strictly excluded from source control via `.gitignore`.
- **Local Ephemeral Sandboxing:** Each slot executes within an isolated, deterministic filesystem sandbox (`profile_00` to `profile_44`), preventing cross-session pollution.
- **Port Collision Protection:** The supervisor enforces runtime uniqueness on all remote debugging ports before any instance initialization occurs.

---

## 11. License

MIT License — Copyright (c) 2026 Muhammad Usaid Siddiqui. See [LICENSE](LICENSE) for terms.
