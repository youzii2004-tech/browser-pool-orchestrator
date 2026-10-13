"""
Browser Pool Orchestrator
Enterprise Distributed Headless Fleet & CDP Session Architecture.
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
)

__version__ = "1.0.0"

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
]
