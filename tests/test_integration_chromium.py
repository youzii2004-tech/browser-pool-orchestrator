"""
Integration tests against a REAL Chromium process. Skipped when no browser is installed
(set BROWSER_POOL_CHROMIUM to point at one).
"""

import asyncio
import http.server
import os
import subprocess
import sys
import threading
import time

import psutil
import pytest

from src import process_supervisor as ps
from src.cdp_client import CDPClient
from src.config import PoolConfig, PortRange
from src.pool_manager import NodeStatus, PoolManager
from src.scheduler import BatchScheduler, JobStatus, JobTask, cdp_navigate_handler

BINARY = ps.find_chromium_binary()
pytestmark = pytest.mark.skipif(BINARY is None, reason="no Chromium/Chrome binary available")

BASE_PORT = 19400


def make_pool(tmp_path, size=2, base=BASE_PORT) -> PoolManager:
    cfg = PoolConfig(
        pool_size=size,
        port_range=PortRange(start=base, end=base + size - 1),
        base_user_data_dir=tmp_path / "profiles",
        chromium_path=BINARY,
        no_sandbox=True,  # CI containers/root; harmless for the test
        launch_timeout_sec=30,
    )
    return PoolManager(cfg)


def pool_procs(manager):
    return ps.scan_pool_processes([s.config.user_data_dir for s in manager.slots.values()])


@pytest.fixture
def http_server():
    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            code = 429 if self.path.startswith("/limited") else 200
            body = b"<html><title>served-ok</title><body>hi</body></html>"
            self.send_response(code)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


@pytest.mark.asyncio
async def test_launch_drive_and_clean_shutdown(tmp_path, http_server) -> None:
    manager = make_pool(tmp_path, 2)
    try:
        pid = await manager.launch_node("node_00")
        assert psutil.pid_exists(pid) and manager.get_node("node_00").has_real_process
        assert manager.refresh_process_tree("node_00"), "expected real helper processes"

        client = await CDPClient.for_port(manager.get_node("node_00").cdp_port)
        try:
            nav = await client.navigate(f"{http_server}/ok")
            assert nav["status"] == 200
            assert await client.runtime_evaluate("document.title") == "served-ok"
            assert await client.runtime_evaluate("1+2") == 3
            assert await client.dom_query_selector("body") > 0
            assert await client.dom_query_selector("#missing") == 0
        finally:
            await client.disconnect()

        telemetry = manager.get_telemetry()
        assert telemetry.running_browsers == 1 and telemetry.total_memory_mb > 20
    finally:
        await manager.ashutdown()
    assert not ps.pid_alive(pid)
    assert pool_procs(manager) == {}, "processes survived shutdown"


@pytest.mark.asyncio
async def test_crash_is_detected_and_node_relaunched(tmp_path) -> None:
    manager = make_pool(tmp_path, 1, base=BASE_PORT + 10)
    try:
        old = await manager.launch_node("node_00")
        psutil.Process(old).kill()
        await asyncio.sleep(0.5)
        report = await manager.check_health()
        assert report["crashed"] == ["node_00"] and report["recycled"] == ["node_00"]
        assert manager.get_node("node_00").pid is None
        new = await manager.ensure_running("node_00")
        assert new != old and ps.pid_alive(new)
    finally:
        await manager.ashutdown()
    assert pool_procs(manager) == {}


@pytest.mark.asyncio
async def test_purge_finds_real_orphans_but_spares_live_parents(tmp_path) -> None:
    manager = make_pool(tmp_path, 1, base=BASE_PORT + 20)
    profile = manager.get_node("node_00").config.user_data_dir
    sleeper = "import time; time.sleep(120)"
    # Orphan: intermediate shell exits, leaving the sleeper re-parented.
    subprocess.run(
        ["sh", "-c", f"{sys.executable} -c '{sleeper}' --user-data-dir={profile} >/dev/null 2>&1 &"], check=True
    )
    live_parent = subprocess.Popen([sys.executable, "-c", sleeper, f"--user-data-dir={profile}"])
    try:
        await asyncio.sleep(0.5)
        orphan_pids = {o.pid for o in manager.scan_orphans()}
        assert live_parent.pid not in orphan_pids, "must not kill a process whose parent is alive"
        found = set(pool_procs(manager)) - {live_parent.pid}
        if not found:
            pytest.skip("could not create an orphan in this environment")
        if not (found & orphan_pids):
            pytest.skip("environment re-parents orphans to a live subreaper")
        assert manager.purge_zombies() >= 1
        assert not (found & set(pool_procs(manager)))
        assert psutil.pid_exists(live_parent.pid)
        assert manager.purge_zombies(include_live_parents=True) >= 1  # --force path
    finally:
        live_parent.kill()
        live_parent.wait()


@pytest.mark.asyncio
async def test_scheduler_runs_real_browsers_and_quarantines_on_real_429(tmp_path, http_server) -> None:
    manager = make_pool(tmp_path, 3, base=BASE_PORT + 30)
    scheduler = BatchScheduler(manager, max_concurrency=3, launch_browsers=True)
    good = [JobTask(payload={"target": f"{http_server}/ok?{i}"}) for i in range(4)]
    limited = JobTask(payload={"target": f"{http_server}/limited"}, max_retries=0)
    scheduler.enqueue_batch(good + [limited])
    try:
        await scheduler.run(worker_count=3, handler=cdp_navigate_handler, until_empty=True)
        assert all(t.status == JobStatus.COMPLETED and t.result["title"] == "served-ok" for t in good)
        assert limited.status == JobStatus.FAILED and "429" in limited.error_message
        assert len(scheduler.resilience.quarantine_registry) == 1
        quarantined = [s for s in manager.slots.values() if s.status == NodeStatus.QUARANTINED]
        assert len(quarantined) == 1
    finally:
        await manager.ashutdown()
    assert pool_procs(manager) == {}
