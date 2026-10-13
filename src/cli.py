"""
Command-line interface for Browser Pool Orchestrator.

Commands: status, health (probes real DevTools ports), purge-zombies (scans the real
process table), run (launch and supervise a fleet), dispatch (batch jobs through real
browsers, or --dry-run without), serve (demo dashboard).
"""

from __future__ import annotations

import argparse
import asyncio
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
import json
import os
from pathlib import Path
import sys
import time
from typing import List, Optional

if __package__ in (None, ""):
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from src import process_supervisor as ps
    from src.config import PoolConfig
    from src.pool_manager import NodeStatus, PoolManager
    from src.resilience_engine import ResilienceEngine
    from src.scheduler import BatchScheduler, JobTask, cdp_navigate_handler
else:
    from . import process_supervisor as ps
    from .config import PoolConfig
    from .pool_manager import NodeStatus, PoolManager
    from .resilience_engine import ResilienceEngine
    from .scheduler import BatchScheduler, JobTask, cdp_navigate_handler


def get_default_config_path() -> Path:
    base = Path(__file__).resolve().parent.parent
    cfg_file = base / "config" / "pool_config.example.yaml"
    return cfg_file


def load_pool_manager(args: argparse.Namespace) -> PoolManager:
    config_path = getattr(args, "config", None)
    path = Path(config_path) if config_path else get_default_config_path()
    if path.exists():
        cfg = PoolConfig.from_yaml(path)
    else:
        cfg = PoolConfig()
        cfg.generate_nodes()
    if getattr(args, "no_sandbox", False):
        cfg.no_sandbox = True
    if getattr(args, "chromium", None):
        cfg.chromium_path = args.chromium
    return PoolManager(cfg)


def _fmt(value: Optional[float], unit: str = "", digits: int = 1) -> str:
    return "n/a" if value is None else f"{value:.{digits}f}{unit}"


def cmd_status(args: argparse.Namespace) -> int:
    """Fleet state as seen by this process, plus a scan of the host for pool browsers."""
    manager = load_pool_manager(args)
    t = manager.get_telemetry()
    hosted = ps.scan_pool_processes([s.config.user_data_dir for s in manager.slots.values()])

    print(f"\nFleet: {t.fleet_name}   slots: {t.total_nodes}   "
          f"ports: {manager.config.port_range.start}-{manager.config.port_range.end}")
    print(f"States (this process): IDLE {t.idle_count} | BUSY {t.busy_count} | COOLDOWN {t.cooldown_count} "
          f"| QUARANTINED {t.quarantined_count} | OFFLINE {t.offline_count}")
    print(f"Browsers launched by this process: {t.running_browsers}   measured RSS: {_fmt(t.total_memory_mb, ' MB')}")
    print(f"Pool-profile processes found on host (any owner): {len(hosted)}")
    print(f"Auto-recovery rate: {_fmt(t.auto_recovery_rate, '%')} (quarantines expired / entered)\n")

    print(f"  {'NODE ID':<10} {'PORT':<7} {'STATUS':<12} {'PID':<8} {'SESSIONS':<9} {'FAILURES':<9}")
    shown = list(manager.slots.values()) if args.all else list(manager.slots.values())[:15]
    for s in shown:
        print(f"  {s.node_id:<10} {s.cdp_port:<7} {s.status.value:<12} {(s.pid or '-')!s:<8} "
              f"{s.session_count:<9} {s.consecutive_failures:<9}")
    if len(shown) < len(manager.slots):
        print(f"  ... {len(manager.slots) - len(shown)} more (use --all)")
    print()
    return 0


def cmd_health(args: argparse.Namespace) -> int:
    """Validate config and probe each node's DevTools port for a live browser."""
    manager = load_pool_manager(args)

    async def _probe():
        return await asyncio.gather(*(ps.probe_cdp(s.cdp_port, timeout=1.0) for s in manager.slots.values()))

    binary = ps.find_chromium_binary(manager.config.chromium_path)
    print(f"\nChromium binary : {binary or 'NOT FOUND (set chromium_path or $BROWSER_POOL_CHROMIUM)'}")
    dirs = [str(s.config.user_data_dir) for s in manager.slots.values()]
    print(f"Config          : {len(manager.slots)} slots, {len(set(dirs))} distinct profile dirs, "
          f"{len(manager.port_to_node)} distinct ports")

    start = time.time()
    results = asyncio.run(_probe())
    up = [(s, r) for s, r in zip(manager.slots.values(), results) if r]
    print(f"DevTools probe  : {len(up)}/{len(manager.slots)} ports answering ({(time.time() - start) * 1000:.0f} ms)")
    for s, info in up:
        print(f"  {s.node_id} :{s.cdp_port}  {info.get('Browser', '?')}")
    config_ok = len(set(dirs)) == len(dirs) and binary is not None
    if args.require_running and len(up) != len(manager.slots):
        return 1
    return 0 if config_ok else 1


def cmd_purge_zombies(args: argparse.Namespace) -> int:
    """Find (and kill) real orphaned Chromium processes that belong to the pool's profiles."""
    manager = load_pool_manager(args)
    orphans = manager.scan_orphans(include_live_parents=args.force)
    print(f"\nOrphaned pool-profile processes found: {len(orphans)}")
    for o in orphans:
        print(f"  pid {o.pid:<7} ppid {o.ppid:<7} {o.status:<9} {Path(o.cmdline[0]).name if o.cmdline else '?'}")
    if args.dry_run or not orphans:
        print("(dry run, nothing killed)\n" if args.dry_run else "Nothing to purge.\n")
        return 0
    purged = manager.purge_zombies(include_live_parents=args.force)
    remaining = manager.scan_orphans(include_live_parents=args.force)
    print(f"Eliminated: {purged}   Remaining orphans: {len(remaining)}\n")
    return 0 if not remaining else 1


def cmd_run(args: argparse.Namespace) -> int:
    """Launch real browsers for the first N slots and supervise them until Ctrl+C."""
    manager = load_pool_manager(args)
    names = list(manager.slots)[: args.nodes]

    async def _main() -> None:
        stop = asyncio.Event()
        try:
            await asyncio.gather(*(manager.launch_node(n) for n in names))
            for n in names:
                s = manager.slots[n]
                print(f"  {n} :{s.cdp_port} pid {s.pid} ({len(manager.refresh_process_tree(n))} helper procs)")
            monitor = asyncio.create_task(manager.run_health_monitor(stop))
            print("Fleet running. Ctrl+C to stop.")
            try:
                await asyncio.Event().wait()
            finally:
                stop.set()
                await monitor
        finally:
            await manager.ashutdown()
            left = manager.scan_orphans(include_live_parents=True)
            print(f"Shutdown complete. Pool processes left on host: {len(left)}")

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
    return 0


def cmd_dispatch(args: argparse.Namespace) -> int:
    """Run a batch of URL-fetch jobs through real browsers (or --dry-run for scheduler logic only)."""
    manager = load_pool_manager(args)
    resilience = ResilienceEngine(manager.config.resilience)
    live = not args.dry_run
    scheduler = BatchScheduler(
        manager, resilience_engine=resilience, max_concurrency=args.concurrency, launch_browsers=live
    )
    tasks = [JobTask(payload={"task_index": i, "target": args.url}) for i in range(args.tasks)]
    scheduler.enqueue_batch(tasks)
    mode = "dry-run (no browsers)" if args.dry_run else "real Chromium"
    print(f"\nDispatching {args.tasks} tasks, concurrency {scheduler.max_concurrency}, mode: {mode}")

    async def _run() -> None:
        start = time.time()
        try:
            await scheduler.run(
                worker_count=scheduler.max_concurrency,
                handler=cdp_navigate_handler if live else None,
                until_empty=True,
            )
        finally:
            await manager.ashutdown()
        dur = max(time.time() - start, 1e-9)
        done, failed = scheduler.completed_tasks, scheduler.failed_tasks
        print(f"Completed {len(done)}  Failed {len(failed)}  in {dur:.2f}s  ({len(done) / dur:.2f} tasks/s)")
        for t in failed[:5]:
            print(f"  FAILED {t.job_id}: {t.error_message}")
        if live and done:
            print(f"  sample result: {done[0].result}")
        left = manager.scan_orphans(include_live_parents=True)
        print(f"Pool processes left on host after shutdown: {len(left)}\n")

    asyncio.run(_run())
    return 0 if not scheduler.failed_tasks else 1


def cmd_serve(args: argparse.Namespace) -> int:
    """Serve the dashboard. NOTE: the dashboard is a static simulation, not wired to a live fleet."""
    dashboard_dir = Path(__file__).resolve().parent.parent / "dashboard"
    if not dashboard_dir.exists():
        print(f"[!] Dashboard directory not found at: {dashboard_dir}")
        return 1
    handler = partial(SimpleHTTPRequestHandler, directory=str(dashboard_dir))
    httpd = HTTPServer(("127.0.0.1", args.port), handler)
    print(f"\nDemo dashboard (simulated data) at http://127.0.0.1:{args.port}/  - Ctrl+C to stop.\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="browserpool",
        description="Browser Pool Orchestrator - supervise a pool of headless Chromium processes over CDP",
    )
    parser.add_argument("--config", "-c", type=str, help="Path to pool configuration YAML")
    parser.add_argument("--chromium", type=str, help="Chromium/Chrome executable (overrides config and $BROWSER_POOL_CHROMIUM)")
    parser.add_argument("--no-sandbox", action="store_true", help="Pass --no-sandbox to Chromium (needed as root / in containers)")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("status", help="Show slot states and pool processes found on the host")
    p.add_argument("--all", action="store_true", help="List every slot")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("health", help="Validate config and probe DevTools ports")
    p.add_argument("--require-running", action="store_true", help="Exit non-zero unless every port answers")
    p.set_defaults(func=cmd_health)

    p = sub.add_parser("purge-zombies", help="Kill orphaned Chromium processes using the pool's profile dirs")
    p.add_argument("--dry-run", action="store_true", help="List orphans without killing")
    p.add_argument("--force", action="store_true", help="Also kill pool-profile processes whose parent is still alive")
    p.set_defaults(func=cmd_purge_zombies)

    p = sub.add_parser("run", help="Launch real browsers and supervise them until Ctrl+C")
    p.add_argument("--nodes", "-n", type=int, default=3, help="How many slots to launch")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("dispatch", help="Run a batch of navigation jobs through the pool")
    p.add_argument("--tasks", "-n", type=int, default=10, help="Number of tasks")
    p.add_argument("--concurrency", type=int, default=5, help="Concurrency limit")
    p.add_argument("--url", default="data:text/html,<title>pool-ok</title>", help="URL each task loads")
    p.add_argument("--dry-run", action="store_true", help="Exercise scheduling only; launch no browsers")
    p.set_defaults(func=cmd_dispatch)

    p = sub.add_parser("serve", help="Serve the (simulated) demo dashboard")
    p.add_argument("--port", "-p", type=int, default=8080)
    p.set_defaults(func=cmd_serve)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
