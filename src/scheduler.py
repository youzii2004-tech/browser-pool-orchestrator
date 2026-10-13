"""
AsyncIO Batch Job Dispatcher & Concurrency Orchestrator.

Manages high-throughput asynchronous task queues, enforces fleet concurrency caps,
coordinates node leases with PoolManager, and routes failures through the
ResilienceEngine for self-healing retries.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
import logging
import time
from typing import Any, Callable, Coroutine, Dict, List, Optional
import uuid

from .cdp_client import CDPClient
from .config import NodeConfig
from .pool_manager import NodeStatus, PoolManager
from .resilience_engine import AnomalyType, ResilienceEngine

logger = logging.getLogger("browser_pool.scheduler")


class JobStatus(str, Enum):
    """Lifecycle states for batch jobs."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"


@dataclass
class JobTask:
    """Executable task metadata and runtime lifecycle tracking."""

    payload: Dict[str, Any]
    job_id: str = field(default_factory=lambda: f"job_{uuid.uuid4().hex[:8]}")
    status: JobStatus = JobStatus.PENDING
    assigned_node_id: Optional[str] = None
    retries: int = 0
    max_retries: int = 3
    error_message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None

    @property
    def duration_sec(self) -> Optional[float]:
        if self.started_at and self.completed_at:
            return round(self.completed_at - self.started_at, 3)
        return None


TaskHandler = Callable[[NodeConfig, JobTask], Coroutine[Any, Any, Dict[str, Any]]]


class BatchScheduler:
    """AsyncIO concurrency manager and dispatcher across the 45-node browser pool."""

    def __init__(
        self,
        pool_manager: PoolManager,
        resilience_engine: Optional[ResilienceEngine] = None,
        max_concurrency: int = 15,
    ) -> None:
        self.pool = pool_manager
        self.resilience = resilience_engine or ResilienceEngine(pool_manager.config.resilience)
        self.max_concurrency = max(1, min(max_concurrency, self.pool.config.pool_size))
        self.semaphore = asyncio.Semaphore(self.max_concurrency)

        self.queue: asyncio.Queue[JobTask] = asyncio.Queue()
        self.tasks_registry: Dict[str, JobTask] = {}
        self.completed_tasks: List[JobTask] = []
        self.failed_tasks: List[JobTask] = []

        self._active_workers: List[asyncio.Task] = []
        self._is_running = False

    def enqueue_task(self, task: JobTask) -> str:
        """Adds an individual task to the dispatch queue."""
        self.tasks_registry[task.job_id] = task
        self.queue.put_nowait(task)
        logger.debug("Enqueued task %s", task.job_id)
        return task.job_id

    def enqueue_batch(self, tasks: List[JobTask]) -> List[str]:
        """Submits a batch of tasks to the queue."""
        task_ids = []
        for t in tasks:
            task_ids.append(self.enqueue_task(t))
        logger.info("Enqueued batch of %d tasks", len(tasks))
        return task_ids

    async def execute_task(
        self,
        task: JobTask,
        handler: Optional[TaskHandler] = None,
    ) -> None:
        """
        Executes a single task within concurrency bounds, leasing a node from the pool.
        """
        async with self.semaphore:
            # Lease node from fleet
            node_cfg = self.pool.allocate_node(job_id=task.job_id)
            if not node_cfg:
                # Fleet currently saturated; re-enqueue with short backoff
                await asyncio.sleep(0.5)
                self.queue.put_nowait(task)
                return

            task.assigned_node_id = node_cfg.node_id
            task.status = JobStatus.RUNNING
            task.started_at = time.time()
            logger.info("Task %s executing on %s", task.job_id, node_cfg.node_id)

            success = False
            cooldown_sec = 0.0
            quarantine_sec = 0.0
            quarantine_reason = None

            try:
                if handler:
                    res = await handler(node_cfg, task)
                else:
                    # Default mock synthetic execution handler
                    await asyncio.sleep(0.05)  # 50ms mock work
                    res = {
                        "status": "success",
                        "node_id": node_cfg.node_id,
                        "cdp_port": node_cfg.cdp_port,
                        "processed_at": time.time(),
                    }

                task.result = res
                task.status = JobStatus.COMPLETED
                task.completed_at = time.time()
                self.completed_tasks.append(task)
                success = True
                logger.info("Task %s successfully completed on %s", task.job_id, node_cfg.node_id)

            except Exception as exc:
                task.error_message = str(exc)
                logger.warning("Task %s failed on %s: %s", task.job_id, node_cfg.node_id, exc)

                # Evaluate through ResilienceEngine
                classification = self.resilience.classify_error(
                    error_message=str(exc),
                    consecutive_failures=task.retries,
                )

                if classification.should_quarantine:
                    quarantine_sec = classification.cooldown_seconds
                    quarantine_reason = f"Classification: {classification.anomaly_type.value}"
                    self.resilience.record_quarantine(
                        node_id=node_cfg.node_id,
                        anomaly_type=classification.anomaly_type,
                        reason=quarantine_reason,
                        duration_sec=quarantine_sec,
                    )
                else:
                    cooldown_sec = classification.cooldown_seconds

                # Retry logic
                if task.retries < task.max_retries and classification.recommended_action != "PURGE_SESSION":
                    task.retries += 1
                    task.status = JobStatus.RETRYING
                    task.assigned_node_id = None
                    self.queue.put_nowait(task)
                    logger.info("Task %s scheduled for retry #%d", task.job_id, task.retries)
                else:
                    task.status = JobStatus.FAILED
                    task.completed_at = time.time()
                    self.failed_tasks.append(task)

            finally:
                self.pool.release_node(
                    node_id=node_cfg.node_id,
                    success=success,
                    cooldown_sec=cooldown_sec,
                    quarantine_sec=quarantine_sec,
                    quarantine_reason=quarantine_reason,
                )

    async def _worker_loop(self, handler: Optional[TaskHandler] = None) -> None:
        """Continuous background worker draining tasks from the queue."""
        while self._is_running:
            try:
                task = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                try:
                    await self.execute_task(task, handler=handler)
                finally:
                    self.queue.task_done()
            except asyncio.TimeoutError:
                # Check for recovered quarantined nodes periodically
                self.pool.refresh_node_states()
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in scheduler worker loop: %s", e)

    async def run(
        self,
        worker_count: int = 5,
        handler: Optional[TaskHandler] = None,
        until_empty: bool = True,
    ) -> None:
        """
        Runs the batch scheduler until the queue is drained or externally cancelled.
        """
        self._is_running = True
        workers = [
            asyncio.create_task(self._worker_loop(handler=handler))
            for _ in range(worker_count)
        ]
        self._active_workers = workers

        try:
            if until_empty:
                await self.queue.join()
                self._is_running = False
            else:
                while self._is_running:
                    await asyncio.sleep(0.5)
        finally:
            self._is_running = False
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

    def get_queue_stats(self) -> Dict[str, Any]:
        """Provides status distribution of enqueued and completed jobs."""
        return {
            "pending": self.queue.qsize(),
            "completed": len(self.completed_tasks),
            "failed": len(self.failed_tasks),
            "total_registered": len(self.tasks_registry),
            "concurrency_limit": self.max_concurrency,
        }
