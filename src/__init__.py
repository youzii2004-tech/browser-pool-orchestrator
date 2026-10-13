"""
Browser Pool Orchestrator
Supervise a pool of headless Chromium processes over the DevTools protocol.
"""

from .config import (
    PoolConfig,
    NodeConfig,
    PortRange,
    ResiliencePolicy,
    TelemetryConfig,
)
from .pool_manager import (
    PoolManager,
    NodeStatus,
    NodeSlot,
    FleetTelemetry,
)
from .cdp_client import (
    CDPClient,
    CDPResponseError,
    CDPTimeoutError,
)
from .resilience_engine import (
    ResilienceEngine,
    AnomalyType,
    QuarantineRecord,
    ClassificationResult,
)
from .scheduler import (
    BatchScheduler,
    JobTask,
    JobStatus,
    HTTPStatusError,
    cdp_navigate_handler,
)

__version__ = "0.2.0"

__all__ = [
    "PoolConfig",
    "NodeConfig",
    "PortRange",
    "ResiliencePolicy",
    "TelemetryConfig",
    "PoolManager",
    "NodeStatus",
    "NodeSlot",
    "FleetTelemetry",
    "CDPClient",
    "CDPResponseError",
    "CDPTimeoutError",
    "ResilienceEngine",
    "AnomalyType",
    "QuarantineRecord",
    "ClassificationResult",
    "BatchScheduler",
    "JobTask",
    "JobStatus",
    "HTTPStatusError",
    "cdp_navigate_handler",
]
