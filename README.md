# Browser Pool Orchestrator

Supervises a pool of headless Chromium processes: one DevTools port and one profile
directory per slot, leases with cooldown/quarantine, an asyncio CDP client, and a
scheduler that classifies failures (rate limit vs. transient) from real page responses.

[![CI](https://github.com/youzii2004-tech/browser-pool-orchestrator/actions/workflows/ci.yml/badge.svg)](.github/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> **Status: early-stage (v0.2).** It launches and supervises real Chromium and is tested
> against it, but it has not been run at scale. See [Limitations](#limitations) before
> relying on it. If you need a production browser fleet, look at Playwright/Puppeteer
> pools, Selenium Grid, or Browserless.

## What it does

| Component | Behavior |
| :-- | :-- |
| `PoolManager` | Leases node slots. `launch_node` / `acquire` spawn a **real** Chromium (own session, unique `--remote-debugging-port` and `--user-data-dir`), wait for DevTools, track the root PID and its live helper processes via `psutil`, and stop the whole tree (SIGTERM → SIGKILL, root reaped so no defunct entry is left). |
| Orphan cleanup | `purge_zombies` scans the **OS process table** for processes whose `--user-data-dir` is one of the pool's profiles and that no slot owns. By default only true orphans (parent gone) are killed, so a second orchestrator's browsers are not touched; `--force` widens it. |
| Health | `check_health` detects dead roots, unresponsive DevTools ports and RSS above `memory_limit_mb`; idle offenders are recycled and relaunched on the next `acquire`. `max_sessions` recycles a browser after N leases. |
| `CDPClient` | asyncio WebSocket JSON-RPC client: id→future correlation, timeouts, event listeners/waiters, in-flight commands fail immediately if the socket drops. `navigate()` waits for load and returns the main document's HTTP status. |
| `ResilienceEngine` | Classifies errors (token-boundary pattern matching + HTTP status) into rate limit / transient / crash / session problems and computes exponential backoff with jitter. |
| `BatchScheduler` | Queue + semaphore concurrency cap, retries, and acts on the classification: quarantine on 429, recycle the browser after a crash, wipe the profile on a contaminated session. With `launch_browsers=True` each lease gets a real browser. |
| Telemetry | Memory is measured RSS of live browser trees (`None` when nothing is running). The recovery rate is quarantines-expired ÷ quarantines-entered (`None` until one happens). Nothing is estimated or defaulted. |

The classifier only reacts to what it is given: the built-in `cdp_navigate_handler` feeds
it real HTTP statuses and CDP errors, but it does not crawl or detect bans on its own.

## Install and run

```bash
pip install -r requirements.txt     # needs Python 3.10+ and a Chromium/Chrome binary
export BROWSER_POOL_CHROMIUM=/path/to/chromium   # or chromium_path in the config, or have it on PATH

python -m src.cli --no-sandbox dispatch -n 20 --concurrency 5 --url https://example.com
python -m src.cli --no-sandbox run -n 3          # launch 3 browsers and supervise until Ctrl+C
python -m src.cli health                         # config check + probe each DevTools port
python -m src.cli status --all
python -m src.cli purge-zombies --dry-run
```

`--no-sandbox` is required when running as root or in many containers; otherwise
Chromium refuses to start. Use `python` (or `python3`) on every platform. `base_user_data_dir`
defaults to a directory under the system temp dir; point it somewhere persistent if you
want profiles to survive reboots.

Library use:

```python
from src import PoolConfig, PoolManager, BatchScheduler, JobTask, cdp_navigate_handler

pool = PoolManager(PoolConfig(pool_size=4, no_sandbox=True))
sched = BatchScheduler(pool, max_concurrency=4, launch_browsers=True)
sched.enqueue_batch([JobTask(payload={"target": "https://example.com"}) for _ in range(10)])
await sched.run(worker_count=4, handler=cdp_navigate_handler)
await pool.ashutdown()   # stops every browser and sweeps orphans
```

## Tests

```bash
python -m pytest -v -rs
```

* Unit tests: config validation, node state machine, resilience classification.
* CDP protocol tests against a real local WebSocket server.
* **Integration tests against real Chromium** (skipped, and reported by `-rs`, if no
  browser is found): launch, drive via CDP, kill -9 crash detection and relaunch,
  orphan purge, a scheduler run with a real HTTP 429 → quarantine, and a check that no
  pool process survives shutdown.
* `CDPClient(mock_mode=True)` and `allocate_node(mock_process=True)` exist for offline
  state-machine tests only. They return canned data / synthetic PIDs.

CI (`.github/workflows/ci.yml`) installs Chromium via Playwright and runs the same suite;
it has not yet had a run on GitHub at the time of writing.

## Limitations

* One page target per browser; no per-job incognito contexts yet, so cookies persist in a
  node's profile between jobs unless the session is purged.
* Single host, single process: slot state lives in memory and is not shared between
  orchestrator processes (the orphan scan is the only cross-process safety net).
* Orphan detection relies on the parent being gone (`ppid ≤ 1` or dead). Under a
  subreaper, orphans are reparented to a live process and need `--force`.
* Tested on Linux only; Windows/macOS paths are best-effort.
* No evidence of operation at 45-node scale has been collected; the default config
  merely *defines* 45 slots (ports 9222–9266).
* `dashboard/index.html` is a static simulation (it says so on the page) and is not
  connected to a live pool.

## Layout

```text
src/process_supervisor.py  Chromium discovery/launch args, psutil tree + host scans
src/pool_manager.py        Slots, leases, launch/stop, health, orphan purge, telemetry
src/cdp_client.py          Async CDP client (+ navigate, event waiters)
src/resilience_engine.py   Error classification, backoff, quarantine registry
src/scheduler.py           Queue, retries, real CDP handler
src/cli.py                 browserpool CLI
tests/                     Unit, protocol, and real-Chromium integration tests
```

## License

MIT - see [LICENSE](LICENSE).
