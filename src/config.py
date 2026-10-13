"""
Configuration Models & Schema Validation.

Defines Pydantic v2 schemas for the Browser Pool Orchestrator, including port ranges,
individual node configurations, resilience policies, telemetry settings, and the
top-level fleet configuration with YAML serialization.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any, List, Optional
import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class PortRange(BaseModel):
    """CDP remote debugging port allocation bounds."""

    start: int = Field(default=9222, description="Initial port in debugging range")
    end: int = Field(default=9266, description="Final port in debugging range (inclusive)")

    @model_validator(mode="after")
    def validate_port_bounds(self) -> PortRange:
        if not (1024 <= self.start <= 65535):
            raise ValueError(f"start port {self.start} must be between 1024 and 65535")
        if not (1024 <= self.end <= 65535):
            raise ValueError(f"end port {self.end} must be between 1024 and 65535")
        if self.start > self.end:
            raise ValueError(f"start port ({self.start}) cannot exceed end port ({self.end})")
        return self

    @property
    def total_slots(self) -> int:
        """Total number of available port slots in this range."""
        return (self.end - self.start) + 1

    def port_for_slot(self, slot_index: int) -> int:
        """Calculate exact port for a given 0-indexed slot."""
        if slot_index < 0 or slot_index >= self.total_slots:
            raise IndexError(
                f"Slot index {slot_index} out of bounds for range {self.start}-{self.end} "
                f"(max {self.total_slots - 1})"
            )
        return self.start + slot_index

    def contains_port(self, port: int) -> bool:
        """Check whether a given port falls inside this range."""
        return self.start <= port <= self.end


class NodeConfig(BaseModel):
    """Configuration for an individual browser instance node."""

    node_id: str = Field(description="Unique node identifier, e.g. node_00")
    slot_index: int = Field(ge=0, description="0-indexed position within fleet")
    cdp_port: int = Field(ge=1024, le=65535, description="Dedicated CDP WebSocket port")
    user_data_dir: Path = Field(description="Isolated file path for session profiles")
    headless: bool = Field(default=True, description="Run Chromium in headless mode")
    memory_limit_mb: int = Field(default=1024, ge=256, description="Per-node memory ceiling in MB")
    max_sessions: int = Field(default=100, ge=1, description="Max session cycles before node recycling")
    tags: List[str] = Field(default_factory=list, description="Categorization or capability tags")
    active: bool = Field(default=True, description="Whether node is enabled in the active pool")

    @field_validator("node_id")
    @classmethod
    def validate_node_id(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("node_id cannot be empty")
        return v


class ResiliencePolicy(BaseModel):
    """Self-healing, rate-limit classification, and cooldown policies."""

    soft_limit_cooldown_sec: float = Field(
        default=30.0, ge=1.0, description="Cooldown for transient rate spikes or content rejections"
    )
    hard_limit_cooldown_sec: float = Field(
        default=300.0, ge=5.0, description="Cooldown for genuine account quota exhaustion (429)"
    )
    quarantine_duration_sec: float = Field(
        default=600.0, ge=10.0, description="Duration in holding quarantine when repeated errors occur"
    )
    max_consecutive_failures: int = Field(
        default=3, ge=1, description="Threshold of consecutive errors before quarantine escalation"
    )
    backoff_multiplier: float = Field(
        default=2.0, ge=1.0, description="Exponential backoff multiplier for repeated cooldowns"
    )
    jitter: bool = Field(default=True, description="Inject pseudo-random jitter into cooldown calculations")
    rate_limit_patterns: List[str] = Field(
        default_factory=lambda: [
            "429",
            "rate limit",
            "too many requests",
            "quota exceeded",
            "quota exhausted",
            "throttled",
            "resource exhausted",
        ],
        description="String substrings matching genuine rate limit rejections",
    )
    transient_patterns: List[str] = Field(
        default_factory=lambda: [
            "timeout",
            "content filtered",
            "policy retry",
            "navigation timeout",
            "temporary hiccup",
            "target closed",
            "dom race",
            "element not interactable",
        ],
        description="String substrings matching soft or transient false-alarms",
    )


class TelemetryConfig(BaseModel):
    """Fleet monitoring and telemetry export options."""

    enabled: bool = Field(default=True, description="Enable live telemetry collection")
    metrics_export_interval_sec: float = Field(
        default=5.0, ge=0.5, description="Heartbeat interval for metric aggregates"
    )
    log_level: str = Field(default="INFO", description="Standard logging level")
    health_check_interval_sec: float = Field(
        default=10.0, ge=1.0, description="Background health probe interval"
    )


class PoolConfig(BaseModel):
    """Top-level fleet configuration for a pool of headless Chromium instances."""

    fleet_name: str = Field(default="browser-pool-primary", description="Human-readable fleet name")
    pool_size: int = Field(default=45, ge=1, le=256, description="Total number of browser nodes")
    port_range: PortRange = Field(default_factory=PortRange, description="CDP port bounds")
    base_user_data_dir: Path = Field(
        default_factory=lambda: Path(tempfile.gettempdir()) / "browser_pool" / "profiles",
        description="Root directory for isolated user profiles (must be writable)",
    )
    chromium_path: Optional[str] = Field(
        default=None,
        description="Chromium/Chrome executable; falls back to $BROWSER_POOL_CHROMIUM then PATH lookup",
    )
    no_sandbox: bool = Field(
        default=False,
        description="Pass --no-sandbox (needed when running as root or in many containers)",
    )
    extra_chromium_args: List[str] = Field(default_factory=list, description="Extra Chromium flags")
    launch_timeout_sec: float = Field(
        default=20.0, gt=0, description="Max seconds to wait for a launched browser's DevTools port"
    )
    resilience: ResiliencePolicy = Field(default_factory=ResiliencePolicy)
    telemetry: TelemetryConfig = Field(default_factory=TelemetryConfig)
    nodes: List[NodeConfig] = Field(default_factory=list, description="Explicit node slot configurations")

    @model_validator(mode="after")
    def validate_fleet_integrity(self) -> PoolConfig:
        if self.port_range.total_slots < self.pool_size:
            raise ValueError(
                f"port_range ({self.port_range.start}-{self.port_range.end}) provides "
                f"{self.port_range.total_slots} ports, but pool_size is {self.pool_size}"
            )
        # If nodes are explicitly provided, verify no duplicate ports or node_ids
        if self.nodes:
            seen_ports = set()
            seen_ids = set()
            for node in self.nodes:
                if node.node_id in seen_ids:
                    raise ValueError(f"Duplicate node_id found: {node.node_id}")
                if not self.port_range.contains_port(node.cdp_port):
                    raise ValueError(
                        f"Node {node.node_id} port {node.cdp_port} is outside port_range "
                        f"{self.port_range.start}-{self.port_range.end}"
                    )
                if node.cdp_port in seen_ports:
                    raise ValueError(f"Duplicate cdp_port found: {node.cdp_port}")
                seen_ids.add(node.node_id)
                seen_ports.add(node.cdp_port)
        return self

    def generate_nodes(self) -> List[NodeConfig]:
        """Automatically generates node slots from pool_size and port_range."""
        generated: List[NodeConfig] = []
        for i in range(self.pool_size):
            node_id = f"node_{i:02d}"
            port = self.port_range.port_for_slot(i)
            profile_dir = self.base_user_data_dir / f"profile_{i:02d}"
            generated.append(
                NodeConfig(
                    node_id=node_id,
                    slot_index=i,
                    cdp_port=port,
                    user_data_dir=profile_dir,
                    headless=True,
                    memory_limit_mb=1024,
                    max_sessions=100,
                    tags=["standard", "general-compute"],
                    active=True,
                )
            )
        self.nodes = generated
        return self.nodes

    @classmethod
    def from_yaml(cls, path: str | Path) -> PoolConfig:
        """Load configuration from a YAML file."""
        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(f"Configuration file not found at: {file_path}")
        with open(file_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        # Handle base_user_data_dir string conversion
        if "base_user_data_dir" in data and isinstance(data["base_user_data_dir"], str):
            data["base_user_data_dir"] = Path(data["base_user_data_dir"])

        config = cls.model_validate(data)
        if not config.nodes:
            config.generate_nodes()
        return config

    def to_yaml(self, path: str | Path) -> None:
        """Serialize configuration to a YAML file with POSIX path separators."""
        file_path = Path(path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        raw_dict = self.model_dump(mode="json")

        def _normalize_paths(obj: Any) -> Any:
            if isinstance(obj, dict):
                return {k: _normalize_paths(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [_normalize_paths(elem) for elem in obj]
            elif isinstance(obj, str) and ("\\" in obj):
                return obj.replace("\\", "/")
            return obj

        clean_dict = _normalize_paths(raw_dict)
        with open(file_path, "w", encoding="utf-8") as f:
            yaml.dump(clean_dict, f, default_flow_style=False, sort_keys=False)
