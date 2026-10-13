"""
Fleet supervisor: node leasing, real Chromium process lifecycle, and orphan cleanup.

Each node slot owns one DevTools port and one user-data directory. Slots can be backed
by a real Chromium process (``launch_node`` / ``acquire``) or leased without any
process at all (``allocate_node``). ``mock_process=True`` attaches *synthetic* PIDs and
exists only so state-machine tests can run without a browser; it is never the default.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
import logging
import os
from pathlib import Path
import shutil
import sys
import time
from typing import Any, Dict, List, Optional, Set

from . import process_supervisor as ps
from .config import NodeConfig, PoolConfig

logger = logging.getLogger("browser_pool.manager")


class NodeStatus(str, Enum):
    """Lifecycle states for browser fleet nodes."""

    IDLE = "IDLE"
    BUSY = "BUSY"
    COOLDOWN = "COOLDOWN"
    QUARANTINED = "QUARANTINED"
    OFFLINE = "OFFLINE"
    TERMINATING = "TERMINATING"


@dataclass
class NodeSlot:
    """Runtime tracking slot for an individual browser instance."""

    config: NodeConfig
    status: NodeStatus = NodeStatus.IDLE
    pid: Optional[int] = None
    child_pids: List[int] = field(default_factory=list)
    session_count: int = 0
    launch_session_count: int = 0  # leases served by the current browser process
    consecutive_failures: int = 0
    last_lease_time: Optional[float] = None
    last_release_time: Optional[float] = None
    cooldown_expires_at: Optional[float] = None
    quarantine_expires_at: Optional[float] = None
    quarantine_reason: Optional[str] = None
    active_job_id: Optional[str] = None
    mock_process: bool = False
    process: Optional[asyncio.subprocess.Process] = field(default=None, repr=False)
    launched_at: Optional[float] = None
    quarantine_count: int = 0   # times this slot entered quarantine
    recoveries: int = 0         # times quarantine/cooldown expired and the slot returned to IDLE
    crash_count: int = 0

    @property
    def node_id(self) -> str:
        return self.config.node_id

    @property
    def cdp_port(self) -> int:
        return self.config.cdp_port

    @property
    def has_real_process(self) -> bool:
        return self.pid is not None and not self.mock_process

    def is_available(self, current_time: Optional[float] = None) -> bool:
        """Determines if the slot can be leased for a new task."""
        now = time.time() if current_time is None else current_time
        if not self.config.active:
            return False

        if self.status == NodeStatus.COOLDOWN:
            if self.cooldown_expires_at is not None and now >= self.cooldown_expires_at:
                self.status = NodeStatus.IDLE
                self.cooldown_expires_at = None
                self.recoveries += 1
            else:
                return False

        if self.status == NodeStatus.QUARANTINED:
            if self.quarantine_expires_at is not None and now >= self.quarantine_expires_at:
                self.status = NodeStatus.IDLE
                self.quarantine_expires_at = None
                self.quarantine_reason = None
                self.consecutive_failures = 0
                self.recoveries += 1
            else:
                return False

        return self.status == NodeStatus.IDLE


@dataclass
class FleetTelemetry:
    """Operational metrics. Values that cannot be measured are None, never invented."""

    fleet_name: str
    total_nodes: int
    idle_count: int
    busy_count: int
    cooldown_count: int
    quarantined_count: int
    offline_count: int
    active_concurrency_ratio: float
    total_memory_mb: Optional[float]   # measured RSS of live browser trees; None if none running
    running_browsers: int
    zombie_pids_eliminated: int
    total_leases_granted: int
    auto_recovery_rate: Optional[float]  # % of quarantines that have expired; None if no quarantines yet
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "fleet_name": self.fleet_name,
            "total_nodes": self.total_nodes,
            "idle_count": self.idle_count,
            "busy_count": self.busy_count,
            "cooldown_count": self.cooldown_count,
            "quarantined_count": self.quarantined_count,
            "offline_count": self.offline_count,
            "active_concurrency_ratio": round(self.active_concurrency_ratio, 3),
            "total_memory_mb": None if self.total_memory_mb is None else round(self.total_memory_mb, 1),
            "running_browsers": self.running_browsers,
            "zombie_pids_eliminated": self.zombie_pids_eliminated,
            "total_leases_granted": self.total_leases_granted,
            "auto_recovery_rate": None if self.auto_recovery_rate is None else round(self.auto_recovery_rate, 1),
            "timestamp": self.timestamp,
        }


class PoolManager:
    """Supervisor for a fixed set of node slots with real process tracking."""

    def __init__(self, config: Optional[PoolConfig] = None) -> None:
        self.config = config or PoolConfig()
        if not self.config.nodes:
            self.config.generate_nodes()

        self.slots: Dict[str, NodeSlot] = {}
        self.port_to_node: Dict[int, str] = {}
        self.tracked_pids: Set[int] = set()
        self.zombies_purged_counter: int = 0
        self.total_leases_granted: int = 0
        self._launch_locks: Dict[str, asyncio.Lock] = {}

        self._initialize_slots()

    # ------------------------------------------------------------------ setup

    def _initialize_slots(self) -> None:
        """Sets up internal slot registry and verifies no port collisions."""
        for node_cfg in self.config.nodes:
            if node_cfg.cdp_port in self.port_to_node:
                conflict_node = self.port_to_node[node_cfg.cdp_port]
                raise RuntimeError(
                    f"Port collision detected: Port {node_cfg.cdp_port} assigned to both "
                    f"'{conflict_node}' and '{node_cfg.node_id}'"
                )
            self.slots[node_cfg.node_id] = NodeSlot(config=node_cfg)
            self.port_to_node[node_cfg.cdp_port] = node_cfg.node_id

    def get_node(self, node_id: str) -> Optional[NodeSlot]:
        """Fetch slot by node_id."""
        return self.slots.get(node_id)

    # Backwards-compatible views over per-slot counters.
    @property
    def recovery_attempts_count(self) -> int:
        return sum(s.quarantine_count for s in self.slots.values())

    @property
    def recovery_events_count(self) -> int:
        return sum(s.recoveries for s in self.slots.values())

    # ---------------------------------------------------------------- leasing

    def allocate_node(
        self,
        job_id: Optional[str] = None,
        tags: Optional[List[str]] = None,
        mock_process: bool = False,
    ) -> Optional[NodeConfig]:
        """
        Leases the next available idle node. This only reserves the slot; use
        ``acquire`` to also guarantee a running browser behind it.

        ``mock_process=True`` attaches synthetic PIDs (50000 + slot_index, children at
        +100/+200). They are not real processes and exist for state-machine tests only.
        """
        now = time.time()
        for slot in self.slots.values():
            if not slot.is_available(now):
                continue

            if tags and not all(tag in slot.config.tags for tag in tags):
                continue

            slot.status = NodeStatus.BUSY
            slot.active_job_id = job_id
            slot.last_lease_time = now
            slot.session_count += 1
            slot.launch_session_count += 1
            self.total_leases_granted += 1

            if mock_process and slot.pid is None:
                synthetic_pid = 50000 + slot.config.slot_index
                self.register_process(
                    slot.node_id, synthetic_pid, [synthetic_pid + 100, synthetic_pid + 200], is_mock=True
                )

            logger.info("Allocated %s on port %d for job %s", slot.node_id, slot.config.cdp_port, job_id)
            return slot.config

        return None

    async def acquire(
        self,
        job_id: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> Optional[NodeConfig]:
        """Lease a node and make sure a real Chromium is running (and fresh) behind it."""
        cfg = self.allocate_node(job_id=job_id, tags=tags)
        if cfg is None:
            return None
        try:
            await self.ensure_running(cfg.node_id)
        except BaseException:
            self.release_node(cfg.node_id, success=False, cooldown_sec=5.0)
            raise
        return cfg

    def release_node(
        self,
        node_id: str,
        success: bool = True,
        cooldown_sec: float = 0.0,
        quarantine_sec: float = 0.0,
        quarantine_reason: Optional[str] = None,
    ) -> None:
        """Releases a node back to the pool, applying cooldown or quarantine if requested."""
        slot = self.slots.get(node_id)
        if not slot:
            raise KeyError(f"Node {node_id} does not exist in pool")

        now = time.time()
        slot.last_release_time = now
        slot.active_job_id = None

        if not success:
            slot.consecutive_failures += 1
        else:
            slot.consecutive_failures = 0

        if slot.status == NodeStatus.OFFLINE:
            return  # fleet was shut down while the lease was outstanding; stay offline

        if quarantine_sec > 0.0:
            slot.status = NodeStatus.QUARANTINED
            slot.quarantine_expires_at = now + quarantine_sec
            slot.quarantine_reason = quarantine_reason or "Rate limit / resilience quarantine"
            slot.quarantine_count += 1
            logger.warning(
                "Node %s QUARANTINED for %.1fs (Reason: %s)", node_id, quarantine_sec, slot.quarantine_reason
            )
        elif cooldown_sec > 0.0:
            slot.status = NodeStatus.COOLDOWN
            slot.cooldown_expires_at = now + cooldown_sec
            logger.info("Node %s placed in COOLDOWN for %.1fs", node_id, cooldown_sec)
        else:
            slot.status = NodeStatus.IDLE
            logger.info("Node %s released to IDLE", node_id)

    # ------------------------------------------------------ process lifecycle

    def register_process(
        self,
        node_id: str,
        pid: int,
        child_pids: Optional[List[int]] = None,
        is_mock: bool = False,
    ) -> None:
        """Registers a process hierarchy (root PID + children) against a slot."""
        slot = self.slots.get(node_id)
        if not slot:
            raise KeyError(f"Node {node_id} not found")

        slot.pid = pid
        slot.child_pids = list(child_pids or [])
        slot.mock_process = is_mock
        self.tracked_pids.add(pid)
        self.tracked_pids.update(slot.child_pids)

    def _binary(self) -> str:
        binary = ps.find_chromium_binary(self.config.chromium_path)
        if not binary:
            raise FileNotFoundError(
                "No Chromium/Chrome executable found. Set chromium_path in the config, "
                f"export {ps.CHROMIUM_ENV_VAR}, or install chromium."
            )
        return binary

    async def launch_node(self, node_id: str) -> int:
        """Spawn a real headless Chromium for the slot and wait until DevTools answers."""
        slot = self.slots.get(node_id)
        if not slot:
            raise KeyError(f"Node {node_id} not found")
        lock = self._launch_locks.setdefault(node_id, asyncio.Lock())
        async with lock:
            if slot.has_real_process and ps.pid_alive(slot.pid):  # type: ignore[arg-type]
                return slot.pid  # type: ignore[return-value]
            if slot.pid is not None:
                self._forget_process(slot)

            binary = self._binary()
            if await ps.probe_cdp(slot.cdp_port, timeout=0.5):
                raise RuntimeError(
                    f"Port {slot.cdp_port} for {node_id} is already served by another DevTools endpoint"
                )

            Path(slot.config.user_data_dir).mkdir(parents=True, exist_ok=True)
            args = ps.build_launch_args(
                slot.config,
                binary,
                no_sandbox=self.config.no_sandbox,
                extra_args=self.config.extra_chromium_args,
            )
            kwargs: Dict[str, Any] = {}
            if sys.platform != "win32":
                kwargs["start_new_session"] = True  # own process group; nothing leaks to our terminal
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                **kwargs,
            )
            slot.process = proc
            slot.launched_at = time.time()
            slot.launch_session_count = 1 if slot.status == NodeStatus.BUSY else 0
            self.register_process(node_id, proc.pid, [], is_mock=False)
            try:
                await ps.wait_for_cdp(slot.cdp_port, timeout=self.config.launch_timeout_sec, pid=proc.pid)
            except BaseException:
                await self._stop(slot)
                raise
            self.refresh_process_tree(node_id)
            logger.info("Launched %s (pid %d) on port %d", node_id, proc.pid, slot.cdp_port)
            return proc.pid

    async def ensure_running(self, node_id: str) -> int:
        """Launch the slot's browser if it is missing, dead, unresponsive, or past max_sessions."""
        slot = self.slots[node_id]
        if slot.has_real_process:
            alive = ps.pid_alive(slot.pid)  # type: ignore[arg-type]
            worn_out = slot.launch_session_count > slot.config.max_sessions
            if alive and not worn_out and await ps.probe_cdp(slot.cdp_port, timeout=1.0):
                return slot.pid  # type: ignore[return-value]
            if not alive:
                slot.crash_count += 1
                logger.warning("Browser for %s died; relaunching", node_id)
            elif worn_out:
                logger.info("Recycling %s after %d sessions", node_id, slot.launch_session_count - 1)
            await self.stop_node(node_id)
        return await self.launch_node(node_id)

    def refresh_process_tree(self, node_id: str) -> List[int]:
        """Re-read the live child processes (renderers, GPU, utility) of a real slot."""
        slot = self.slots[node_id]
        if not slot.has_real_process:
            return slot.child_pids
        self.tracked_pids.difference_update(slot.child_pids)
        slot.child_pids = ps.descendant_pids(slot.pid)  # type: ignore[arg-type]
        self.tracked_pids.update(slot.child_pids)
        return slot.child_pids

    def _forget_process(self, slot: NodeSlot) -> None:
        if slot.pid is not None:
            self.tracked_pids.discard(slot.pid)
        self.tracked_pids.difference_update(slot.child_pids)
        slot.pid = None
        slot.child_pids = []
        slot.process = None
        slot.mock_process = False
        slot.launched_at = None

    async def _stop(self, slot: NodeSlot) -> int:
        """Terminate a real browser tree and reap the root so no defunct entry remains."""
        if slot.pid is None:
            return 0
        pid, proc = slot.pid, slot.process
        helpers = ps.descendant_pids(pid)
        killed = 0
        if proc is not None and proc.returncode is None:
            try:
                proc.terminate()
                killed += 1
                await asyncio.wait_for(proc.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
            except ProcessLookupError:
                pass
        # Helpers are not our children; escalate on any that outlived the root.
        killed += await asyncio.to_thread(self._kill_leftovers, helpers)
        if proc is None:
            killed += await asyncio.to_thread(ps.kill_process_tree, pid)
        self._forget_process(slot)
        return killed

    @staticmethod
    def _kill_leftovers(pids: List[int]) -> int:
        n = 0
        for p in pids:
            if ps.pid_alive(p):
                n += ps.kill_process_tree(p, grace_sec=2.0)
        return n

    async def stop_node(self, node_id: str) -> int:
        """Stop a node's browser (real or mock) and return how many processes were killed."""
        slot = self.slots[node_id]
        if slot.mock_process:
            return self.terminate_node_process(node_id)
        return await self._stop(slot)

    def terminate_node_process(self, node_id: str, force: bool = True) -> int:
        """
        Synchronous teardown of a node's process tree. Real trees get SIGTERM then
        SIGKILL via psutil (descendants included); mock PIDs are just forgotten.
        """
        slot = self.slots.get(node_id)
        if not slot or slot.pid is None:
            return 0

        if slot.mock_process:
            killed = 1 + len(slot.child_pids)
        else:
            killed = ps.kill_process_tree(slot.pid, grace_sec=0.5 if force else 3.0)
        self._forget_process(slot)
        if slot.status == NodeStatus.TERMINATING:
            slot.status = NodeStatus.IDLE
        return killed

    # ------------------------------------------------------- zombie handling

    def scan_orphans(self, include_live_parents: bool = False) -> List[ps.HostProcess]:
        """
        Scan the real OS process table for pool-profile Chromium processes that no slot
        owns. By default only true orphans are reported: processes whose parent is gone
        (re-parented to init/a subreaper), so another running orchestrator is not hit.
        ``include_live_parents`` reports every untracked pool-profile process.
        """
        profile_dirs = [s.config.user_data_dir for s in self.slots.values()]
        found = ps.scan_pool_processes(profile_dirs)
        owned: Set[int] = set()
        for slot in self.slots.values():
            if slot.has_real_process and ps.pid_alive(slot.pid):  # type: ignore[arg-type]
                owned.add(slot.pid)  # type: ignore[arg-type]
                owned.update(ps.descendant_pids(slot.pid))  # type: ignore[arg-type]
        orphans = []
        for pid, info in found.items():
            if pid in owned:
                continue
            parent_gone = info.ppid <= 1 or not ps.psutil.pid_exists(info.ppid)
            if parent_gone or include_live_parents:
                orphans.append(info)
        return orphans

    def purge_zombies(self, include_live_parents: bool = False) -> int:
        """
        Supervisor sweep. Eliminates:
          1. synthetic (mock) PIDs held by non-leased slots,
          2. tracked real PIDs whose process has died (bookkeeping only),
          3. real orphaned Chromium processes found in the OS process table.
        Warm idle browsers that are alive are deliberately left running.
        Returns the number of processes eliminated.
        """
        purged = 0

        for slot in self.slots.values():
            if slot.pid is None or slot.active_job_id is not None:
                continue
            if slot.status not in (NodeStatus.IDLE, NodeStatus.COOLDOWN, NodeStatus.QUARANTINED, NodeStatus.OFFLINE):
                continue
            if slot.mock_process:
                purged += self.terminate_node_process(slot.node_id, force=True)
            elif not ps.pid_alive(slot.pid):
                self._reap_defunct(slot.pid)
                self._forget_process(slot)
                slot.crash_count += 1

        orphans = self.scan_orphans(include_live_parents=include_live_parents)
        for info in orphans:
            if info.status == "zombie":
                if self._reap_defunct(info.pid):
                    purged += 1
                continue
            try:
                p = ps.psutil.Process(info.pid)
                p.kill()
                p.wait(timeout=3)
                purged += 1
            except (ps.psutil.NoSuchProcess, ps.psutil.AccessDenied, ps.psutil.TimeoutExpired):
                continue

        self.zombies_purged_counter += purged
        logger.info("Purged %d orphaned/zombie processes", purged)
        return purged

    @staticmethod
    def _reap_defunct(pid: int) -> bool:
        """Reap a defunct child of this process. Defunct non-children can only be reaped by their parent."""
        try:
            done, _ = os.waitpid(pid, os.WNOHANG)
            return done == pid
        except (ChildProcessError, OSError):
            return False

    # ---------------------------------------------------------------- health

    async def check_health(self) -> Dict[str, List[str]]:
        """
        One health pass over real browsers: detect crashed roots, unresponsive DevTools
        ports, and RSS above the node's memory_limit_mb. Idle offenders are stopped (they
        are relaunched lazily by ``acquire``); busy ones are only reported.
        """
        report: Dict[str, List[str]] = {"crashed": [], "unresponsive": [], "over_memory": [], "recycled": []}
        for slot in list(self.slots.values()):
            if not slot.has_real_process:
                continue
            idle = slot.status != NodeStatus.BUSY
            if not ps.pid_alive(slot.pid):  # type: ignore[arg-type]
                report["crashed"].append(slot.node_id)
                slot.crash_count += 1
                self._reap_defunct(slot.pid)  # type: ignore[arg-type]
                if idle:
                    await self._stop(slot)
                    report["recycled"].append(slot.node_id)
                continue
            self.refresh_process_tree(slot.node_id)
            if not await ps.probe_cdp(slot.cdp_port, timeout=2.0):
                report["unresponsive"].append(slot.node_id)
                if idle:
                    await self._stop(slot)
                    report["recycled"].append(slot.node_id)
                continue
            rss = ps.tree_rss_mb([slot.pid] + slot.child_pids)  # type: ignore[list-item]
            if rss > slot.config.memory_limit_mb:
                report["over_memory"].append(slot.node_id)
                if idle:
                    await self._stop(slot)
                    report["recycled"].append(slot.node_id)
        return report

    async def run_health_monitor(self, stop: asyncio.Event) -> None:
        """Background loop driving ``check_health`` and state-expiry refresh."""
        interval = self.config.telemetry.health_check_interval_sec
        while not stop.is_set():
            try:
                self.refresh_node_states()
                report = await self.check_health()
                if any(report[k] for k in ("crashed", "unresponsive", "over_memory")):
                    logger.warning("Health pass: %s", report)
            except Exception as exc:  # keep supervising
                logger.error("Health monitor error: %s", exc)
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    def refresh_node_states(self) -> int:
        """Applies cooldown/quarantine expirations, returning how many nodes recovered."""
        now = time.time()
        before = self.recovery_events_count
        for slot in self.slots.values():
            slot.is_available(now)
        return self.recovery_events_count - before

    # ------------------------------------------------------------- telemetry

    def get_telemetry(self) -> FleetTelemetry:
        """Aggregate fleet metrics. Memory is measured RSS, not an estimate."""
        self.refresh_node_states()
        slots = list(self.slots.values())
        total = len(slots)
        count = lambda st: sum(1 for s in slots if s.status == st)  # noqa: E731
        busy = count(NodeStatus.BUSY)

        real = [s for s in slots if s.has_real_process and ps.pid_alive(s.pid)]  # type: ignore[arg-type]
        memory = None
        if real:
            memory = sum(ps.tree_rss_mb([s.pid] + ps.descendant_pids(s.pid)) for s in real)  # type: ignore[operator]

        attempts = self.recovery_attempts_count
        rate = (self.recovery_events_count / attempts * 100.0) if attempts else None

        return FleetTelemetry(
            fleet_name=self.config.fleet_name,
            total_nodes=total,
            idle_count=count(NodeStatus.IDLE),
            busy_count=busy,
            cooldown_count=count(NodeStatus.COOLDOWN),
            quarantined_count=count(NodeStatus.QUARANTINED),
            offline_count=count(NodeStatus.OFFLINE),
            active_concurrency_ratio=busy / total if total else 0.0,
            total_memory_mb=memory,
            running_browsers=len(real),
            zombie_pids_eliminated=self.zombies_purged_counter,
            total_leases_granted=self.total_leases_granted,
            auto_recovery_rate=rate,
        )

    # -------------------------------------------------------------- profiles

    def purge_profile(self, node_id: str) -> None:
        """Delete a stopped node's user-data directory (cookies, cache, locks)."""
        slot = self.slots[node_id]
        if slot.pid is not None:
            raise RuntimeError(f"Stop {node_id} before purging its profile")
        shutil.rmtree(slot.config.user_data_dir, ignore_errors=True)

    # -------------------------------------------------------------- shutdown

    async def ashutdown(self) -> None:
        """Stop every browser (reaping roots), sweep orphans, and mark all slots OFFLINE."""
        for slot in self.slots.values():
            slot.status = NodeStatus.TERMINATING
        await asyncio.gather(*(self.stop_node(n) for n in self.slots))
        for slot in self.slots.values():
            slot.status = NodeStatus.OFFLINE
            slot.active_job_id = None
        self.purge_zombies()

    def shutdown(self) -> None:
        """Synchronous shutdown (works without a running event loop)."""
        for slot in self.slots.values():
            if slot.pid is not None:
                self.terminate_node_process(slot.node_id, force=True)
            slot.status = NodeStatus.OFFLINE
            slot.active_job_id = None
        self.purge_zombies()
