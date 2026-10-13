"""
Real OS-level Chromium process helpers.

Locates a Chromium binary, builds launch arguments for a pool node, waits for the
DevTools endpoint to come up, walks process trees with psutil, and scans the host
process table for Chromium processes that belong to the pool's profile directories.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import glob
import json
import logging
import os
from pathlib import Path
import shutil
import time
from typing import Dict, Iterable, List, Optional, Sequence
import urllib.error
import urllib.request

import psutil

from .config import NodeConfig

logger = logging.getLogger("browser_pool.process")

CHROMIUM_ENV_VAR = "BROWSER_POOL_CHROMIUM"

_PATH_CANDIDATES = (
    "chromium",
    "chromium-browser",
    "google-chrome",
    "google-chrome-stable",
    "chrome",
)

_GLOB_CANDIDATES = (
    "/opt/pw-browsers/chromium-*/chrome-linux/chrome",
    "~/.cache/ms-playwright/chromium-*/chrome-linux/chrome",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "C:/Program Files/Google/Chrome/Application/chrome.exe",
    "C:/Program Files (x86)/Google/Chrome/Application/chrome.exe",
)


def find_chromium_binary(explicit: Optional[str] = None) -> Optional[str]:
    """Resolve a Chromium/Chrome executable, or None if none can be found."""
    for candidate in (explicit, os.environ.get(CHROMIUM_ENV_VAR)):
        if candidate:
            resolved = shutil.which(candidate) or (candidate if os.path.isfile(candidate) else None)
            if resolved:
                return resolved
            return None  # an explicit choice that does not exist must not silently fall back

    for name in _PATH_CANDIDATES:
        found = shutil.which(name)
        if found:
            return found

    for pattern in _GLOB_CANDIDATES:
        matches = sorted(glob.glob(os.path.expanduser(pattern)))
        for match in reversed(matches):
            if os.path.isfile(match) and os.access(match, os.X_OK):
                return match
    return None


def build_launch_args(
    node: NodeConfig,
    binary: str,
    no_sandbox: bool = False,
    extra_args: Sequence[str] = (),
) -> List[str]:
    """Command line for one pool node. The node's port and profile are always unique."""
    args = [
        binary,
        f"--remote-debugging-port={node.cdp_port}",
        "--remote-debugging-address=127.0.0.1",
        f"--user-data-dir={node.user_data_dir}",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-background-networking",
        "--disable-dev-shm-usage",
        "--disable-breakpad",
        f"--js-flags=--max-old-space-size={max(64, node.memory_limit_mb // 2)}",
    ]
    if node.headless:
        args.append("--headless=new")
    if no_sandbox:
        args.append("--no-sandbox")
    args.extend(extra_args)
    args.append("about:blank")
    return args


def _fetch_json(url: str, timeout: float) -> Optional[dict]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310 (localhost only)
            return json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError):
        return None


async def probe_cdp(port: int, timeout: float = 1.0) -> Optional[dict]:
    """Return /json/version for a debugging port, or None if nothing answers."""
    return await asyncio.to_thread(_fetch_json, f"http://127.0.0.1:{port}/json/version", timeout)


async def wait_for_cdp(port: int, timeout: float = 20.0, pid: Optional[int] = None) -> dict:
    """Poll until the DevTools HTTP endpoint answers. Fails fast if the process dies."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = await probe_cdp(port, timeout=1.0)
        if info:
            return info
        if pid is not None and not pid_alive(pid):
            raise RuntimeError(f"Chromium (pid {pid}) exited before DevTools came up on port {port}")
        await asyncio.sleep(0.1)
    raise TimeoutError(f"DevTools endpoint on port {port} did not respond within {timeout:.1f}s")


def pid_alive(pid: int) -> bool:
    """True if the pid exists and is not a defunct zombie."""
    try:
        return psutil.Process(pid).status() != psutil.STATUS_ZOMBIE
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return False


def descendant_pids(pid: int) -> List[int]:
    """All live descendants (renderers, GPU, utility, crashpad) of a root process."""
    try:
        return [c.pid for c in psutil.Process(pid).children(recursive=True)]
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return []


def kill_process_tree(pid: int, grace_sec: float = 3.0) -> int:
    """
    Terminate a process and every descendant: SIGTERM, wait, then SIGKILL stragglers.
    Returns how many processes were signalled. The caller must still reap the root if
    it is its own child (see PoolManager.stop_node), otherwise a zombie remains.
    """
    try:
        root = psutil.Process(pid)
        procs = root.children(recursive=True) + [root]
    except psutil.NoSuchProcess:
        return 0
    except psutil.AccessDenied:
        return 0

    signalled = 0
    for p in procs:
        try:
            p.terminate()
            signalled += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    _, alive = psutil.wait_procs(procs, timeout=grace_sec)
    for p in alive:
        try:
            p.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    if alive:
        psutil.wait_procs(alive, timeout=grace_sec)
    return signalled


def tree_rss_mb(pids: Iterable[int]) -> float:
    """Resident memory (MB) summed over the given live pids."""
    total = 0
    for pid in pids:
        try:
            total += psutil.Process(pid).memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return total / (1024 * 1024)


@dataclass
class HostProcess:
    pid: int
    ppid: int
    status: str
    user_data_dir: Optional[str]
    cmdline: List[str]


def _arg_value(cmdline: Sequence[str], flag: str) -> Optional[str]:
    prefix = flag + "="
    for arg in cmdline:
        if arg.startswith(prefix):
            return arg[len(prefix):]
    return None


def scan_pool_processes(profile_dirs: Iterable[Path]) -> Dict[int, HostProcess]:
    """
    Scan the real OS process table for processes whose --user-data-dir is one of the
    given pool profile directories (the root browser and all its helpers carry it).
    """
    wanted = {os.path.realpath(str(p)) for p in profile_dirs}
    found: Dict[int, HostProcess] = {}
    for proc in psutil.process_iter(["pid", "ppid", "status", "cmdline"]):
        try:
            cmdline = proc.info.get("cmdline") or []
            udd = _arg_value(cmdline, "--user-data-dir")
            if udd is None or os.path.realpath(udd) not in wanted:
                continue
            found[proc.info["pid"]] = HostProcess(
                pid=proc.info["pid"],
                ppid=proc.info.get("ppid") or 0,
                status=proc.info.get("status") or "",
                user_data_dir=udd,
                cmdline=list(cmdline),
            )
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return found
