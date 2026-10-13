"""Unit tests for process helpers (no browser required)."""

import os
import subprocess
import sys
import time

import psutil

from src import process_supervisor as ps
from src.config import NodeConfig


def _node(tmp_path, port=19999) -> NodeConfig:
    return NodeConfig(node_id="n0", slot_index=0, cdp_port=port, user_data_dir=tmp_path / "p0")


def test_explicit_missing_binary_does_not_fall_back() -> None:
    assert ps.find_chromium_binary("/definitely/not/here") is None


def test_launch_args_isolate_port_and_profile(tmp_path) -> None:
    args = ps.build_launch_args(_node(tmp_path, 19123), "/bin/chrome", no_sandbox=True)
    assert "--remote-debugging-port=19123" in args
    assert f"--user-data-dir={tmp_path / 'p0'}" in args
    assert "--headless=new" in args and "--no-sandbox" in args


def test_kill_process_tree_kills_descendants() -> None:
    parent = subprocess.Popen(
        [sys.executable, "-c",
         "import subprocess,sys,time; subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']); time.sleep(60)"]
    )
    deadline = time.time() + 5
    while not ps.descendant_pids(parent.pid) and time.time() < deadline:
        time.sleep(0.05)
    kids = ps.descendant_pids(parent.pid)
    assert kids
    ps.kill_process_tree(parent.pid, grace_sec=2)
    parent.wait(timeout=5)  # reap
    assert not any(psutil.pid_exists(k) for k in kids)


def test_scan_finds_only_matching_profile_dirs(tmp_path) -> None:
    mine, other = tmp_path / "mine", tmp_path / "other"
    sleeper = "import time; time.sleep(60)"
    a = subprocess.Popen([sys.executable, "-c", sleeper, f"--user-data-dir={mine}"])
    b = subprocess.Popen([sys.executable, "-c", sleeper, f"--user-data-dir={other}"])
    try:
        time.sleep(0.3)
        found = ps.scan_pool_processes([mine])
        assert a.pid in found and b.pid not in found
    finally:
        for p in (a, b):
            p.kill()
            p.wait()
