"""
Tests for PoolManager fleet supervision, node allocation, and zero-zombie teardown.
"""

import time
import pytest

from src.config import NodeConfig, PoolConfig, PortRange
from src.pool_manager import NodeStatus, PoolManager


def test_pool_manager_initialization_45_slots() -> None:
    cfg = PoolConfig(pool_size=45, port_range=PortRange(start=9222, end=9266))
    manager = PoolManager(cfg)

    assert len(manager.slots) == 45
    assert len(manager.port_to_node) == 45
    assert "node_00" in manager.slots
    assert "node_44" in manager.slots
    assert manager.slots["node_00"].cdp_port == 9222
    assert manager.slots["node_44"].cdp_port == 9266


def test_allocate_and_release_lifecycle() -> None:
    cfg = PoolConfig(pool_size=5, port_range=PortRange(start=9222, end=9226))
    manager = PoolManager(cfg)

    # 1. Allocate node
    allocated = manager.allocate_node(job_id="job_alpha", mock_process=True)
    assert allocated is not None
    assert allocated.node_id == "node_00"

    slot = manager.get_node("node_00")
    assert slot is not None
    assert slot.status == NodeStatus.BUSY
    assert slot.active_job_id == "job_alpha"
    assert slot.session_count == 1
    assert slot.pid is not None

    # 2. Allocate second node
    allocated_2 = manager.allocate_node(job_id="job_beta", mock_process=True)
    assert allocated_2 is not None
    assert allocated_2.node_id == "node_01"

    # 3. Release first node to IDLE
    manager.release_node("node_00", success=True)
    assert slot.status == NodeStatus.IDLE
    assert slot.active_job_id is None

    # 4. Release second node with cooldown
    manager.release_node("node_01", success=False, cooldown_sec=10.0)
    slot_2 = manager.get_node("node_01")
    assert slot_2.status == NodeStatus.COOLDOWN
    assert slot_2.consecutive_failures == 1
    assert slot_2.cooldown_expires_at is not None


def test_quarantine_holding_state() -> None:
    cfg = PoolConfig(pool_size=3, port_range=PortRange(start=9222, end=9224))
    manager = PoolManager(cfg)

    allocated = manager.allocate_node(job_id="job_quar")
    assert allocated is not None
    node_id = allocated.node_id

    # Release with quarantine
    manager.release_node(
        node_id=node_id,
        success=False,
        quarantine_sec=60.0,
        quarantine_reason="Rate limit threshold",
    )

    slot = manager.get_node(node_id)
    assert slot.status == NodeStatus.QUARANTINED
    assert slot.quarantine_reason == "Rate limit threshold"
    assert slot.is_available() is False

    # Simulate time jump to expire quarantine
    future_time = time.time() + 70.0
    assert slot.is_available(current_time=future_time) is True
    assert slot.status == NodeStatus.IDLE


def test_pid_supervision_and_zombie_purge() -> None:
    cfg = PoolConfig(pool_size=2, port_range=PortRange(start=9222, end=9223))
    manager = PoolManager(cfg)

    # Allocate node and verify tracked PIDs
    allocated = manager.allocate_node(job_id="job_pid", mock_process=True)
    assert allocated is not None
    slot = manager.get_node(allocated.node_id)
    assert slot.pid is not None
    assert len(slot.child_pids) == 2
    assert slot.pid in manager.tracked_pids

    # Simulate task finishing and releasing to IDLE without process clean
    manager.release_node(allocated.node_id, success=True)
    assert slot.status == NodeStatus.IDLE
    assert slot.pid is not None  # Process lingering while idle!

    # Execute supervisor zombie purge
    purged = manager.purge_zombies()
    assert purged == 3  # Root PID + 2 child PIDs purged
    assert slot.pid is None
    assert len(slot.child_pids) == 0
    assert manager.zombies_purged_counter == 3


def test_telemetry_reporting() -> None:
    cfg = PoolConfig(pool_size=10, port_range=PortRange(start=9222, end=9231))
    manager = PoolManager(cfg)

    # Allocate 3 nodes
    manager.allocate_node(job_id="j1")
    manager.allocate_node(job_id="j2")
    manager.allocate_node(job_id="j3")

    telemetry = manager.get_telemetry()
    assert telemetry.total_nodes == 10
    assert telemetry.busy_count == 3
    assert telemetry.idle_count == 7
    assert telemetry.active_concurrency_ratio == 0.3
    # Leases without a browser: nothing to measure, and nothing may be invented.
    assert telemetry.total_memory_mb is None
    assert telemetry.running_browsers == 0
    assert telemetry.auto_recovery_rate is None


def test_allocate_defaults_to_no_synthetic_process() -> None:
    manager = PoolManager(PoolConfig(pool_size=2, port_range=PortRange(start=9222, end=9223)))
    cfg = manager.allocate_node(job_id="j")
    assert cfg is not None
    assert manager.get_node(cfg.node_id).pid is None


def test_recovery_rate_counts_expiry_even_when_allocate_triggers_it() -> None:
    manager = PoolManager(PoolConfig(pool_size=1, port_range=PortRange(start=9222, end=9222)))
    cfg = manager.allocate_node(job_id="j")
    manager.release_node(cfg.node_id, success=False, quarantine_sec=0.05)
    time.sleep(0.1)
    # allocate (not refresh_node_states) performs the expiry transition
    assert manager.allocate_node(job_id="j2") is not None
    assert manager.get_telemetry().auto_recovery_rate == 100.0


def test_release_after_shutdown_stays_offline() -> None:
    manager = PoolManager(PoolConfig(pool_size=1, port_range=PortRange(start=9222, end=9222)))
    cfg = manager.allocate_node(job_id="j")
    manager.shutdown()
    manager.release_node(cfg.node_id)
    assert manager.get_node(cfg.node_id).status == NodeStatus.OFFLINE


def test_terminate_does_not_clobber_quarantine() -> None:
    manager = PoolManager(PoolConfig(pool_size=1, port_range=PortRange(start=9222, end=9222)))
    cfg = manager.allocate_node(job_id="j", mock_process=True)
    manager.release_node(cfg.node_id, success=False, quarantine_sec=60)
    manager.terminate_node_process(cfg.node_id)
    assert manager.get_node(cfg.node_id).status == NodeStatus.QUARANTINED
