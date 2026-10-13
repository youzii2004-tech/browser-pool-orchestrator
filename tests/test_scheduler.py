"""
Tests for AsyncIO batch scheduler, concurrency limits, and retry queues.
"""

import asyncio
import pytest

from src.config import PoolConfig, PortRange
from src.pool_manager import PoolManager
from src.resilience_engine import ResilienceEngine
from src.scheduler import BatchScheduler, JobStatus, JobTask


@pytest.mark.asyncio
async def test_scheduler_batch_execution() -> None:
    cfg = PoolConfig(pool_size=5, port_range=PortRange(start=9222, end=9226))
    pool = PoolManager(cfg)
    scheduler = BatchScheduler(pool, max_concurrency=3)

    tasks = [JobTask(payload={"id": i}) for i in range(6)]
    scheduler.enqueue_batch(tasks)

    assert scheduler.queue.qsize() == 6

    await scheduler.run(worker_count=3, until_empty=True)

    assert len(scheduler.completed_tasks) == 6
    assert len(scheduler.failed_tasks) == 0
    assert all(t.status == JobStatus.COMPLETED for t in scheduler.completed_tasks)


@pytest.mark.asyncio
async def test_scheduler_retry_and_resilience() -> None:
    cfg = PoolConfig(pool_size=3, port_range=PortRange(start=9222, end=9224))
    pool = PoolManager(cfg)
    scheduler = BatchScheduler(pool, max_concurrency=2)

    call_count = 0

    async def flaky_handler(node_cfg, task):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("timeout: DOM race condition")
        return {"ok": True}

    task = JobTask(payload={"target": "flaky_item"}, max_retries=2)
    scheduler.enqueue_task(task)

    await scheduler.run(worker_count=2, handler=flaky_handler, until_empty=True)

    assert len(scheduler.completed_tasks) == 1
    assert task.status == JobStatus.COMPLETED
    assert task.retries == 1  # 1 retry succeeded


@pytest.mark.asyncio
async def test_scheduler_hard_rate_limit_quarantine() -> None:
    cfg = PoolConfig(pool_size=3, port_range=PortRange(start=9222, end=9224))
    pool = PoolManager(cfg)
    scheduler = BatchScheduler(pool, max_concurrency=2)

    async def rate_limited_handler(node_cfg, task):
        raise RuntimeError("HTTP 429: account rate limit exceeded")

    task = JobTask(payload={"target": "rate_limited_item"}, max_retries=0)
    scheduler.enqueue_task(task)

    await scheduler.run(worker_count=1, handler=rate_limited_handler, until_empty=True)

    assert len(scheduler.failed_tasks) == 1
    assert task.status == JobStatus.FAILED
    # Verify resilience engine recorded quarantine
    assert len(scheduler.resilience.quarantine_registry) == 1
