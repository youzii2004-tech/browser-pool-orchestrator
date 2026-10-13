"""
Self-Healing Anomaly Classifier & Quarantine Resilience Engine.

Distinguishes between transient false-alarm content rejections and genuine
account/API rate limits (HTTP 429). Manages quarantine holding states, exponential
backoff with jitter, and automatic cool-down recovery timers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import logging
import math
import random
import re
import time
from typing import Dict, List, Optional

from .config import ResiliencePolicy

logger = logging.getLogger("browser_pool.resilience")


class AnomalyType(str, Enum):
    """Categorized runtime failure types."""

    FALSE_ALARM = "FALSE_ALARM"              # Transient content filter, minor DOM timing race
    HARD_RATE_LIMIT = "HARD_RATE_LIMIT"      # HTTP 429, quota exhausted, genuine account throttle
    SESSION_CONTAMINATED = "SESSION_CONTAMINATED"  # Corrupted auth cookies or profile lock
    PROCESS_CRASH = "PROCESS_CRASH"          # Out of memory, SIGSEGV, browser target crashed
    UNKNOWN_ERROR = "UNKNOWN_ERROR"          # Generic uncategorized error


@dataclass
class ClassificationResult:
    """Diagnostic outcome and operational recommendation from the Resilience Engine."""

    anomaly_type: AnomalyType
    recommended_action: str
    cooldown_seconds: float
    should_quarantine: bool
    matched_pattern: Optional[str] = None
    confidence: float = 1.0


@dataclass
class QuarantineRecord:
    """Historical and active quarantine holding record for a specific node."""

    node_id: str
    anomaly_type: AnomalyType
    reason: str
    quarantined_at: float
    duration_sec: float
    expires_at: float
    failure_count: int

    @property
    def is_expired(self) -> bool:
        return time.time() >= self.expires_at


def _contains(haystack: str, needle: str) -> bool:
    """Case-insensitive match that respects token boundaries ('429' must not match '14290')."""
    return re.search(r"(?<![a-z0-9])" + re.escape(needle.lower()) + r"(?![a-z0-9])", haystack) is not None


_CRASH_TERMS = ("target crashed", "sigsegv", "out of memory", "oom", "crash", "crashed", "browser disconnected")
_AUTH_TERMS = ("unauthorized", "session expired", "cookie invalid", "auth required")


class ResilienceEngine:
    """Self-healing policy enforcer evaluating errors and managing node quarantine states."""

    def __init__(self, policy: Optional[ResiliencePolicy] = None) -> None:
        self.policy = policy or ResiliencePolicy()
        self.quarantine_registry: Dict[str, QuarantineRecord] = {}
        self.historical_quarantines: List[QuarantineRecord] = []

    def classify_error(
        self,
        error_message: str,
        http_status: Optional[int] = None,
        consecutive_failures: int = 0,
    ) -> ClassificationResult:
        """
        Classifies runtime anomalies using pattern heuristics and HTTP status codes.
        
        Args:
            error_message: Textual stack trace, CDP error message, or API response body.
            http_status: Optional HTTP response code (e.g. 429, 503).
            consecutive_failures: Current failure counter on the subject node.
        """
        lowered = (error_message or "").lower()

        # 0. Status codes other than 429 carry their own meaning
        if http_status in (401, 407):
            return ClassificationResult(
                anomaly_type=AnomalyType.SESSION_CONTAMINATED,
                recommended_action="PURGE_SESSION",
                cooldown_seconds=30.0,
                should_quarantine=True,
                matched_pattern=f"HTTP_{http_status}",
                confidence=1.0,
            )
        if http_status in (500, 502, 503, 504) and consecutive_failures < self.policy.max_consecutive_failures:
            return ClassificationResult(
                anomaly_type=AnomalyType.FALSE_ALARM,
                recommended_action="FAST_RETRY",
                cooldown_seconds=self.calculate_cooldown(AnomalyType.FALSE_ALARM, consecutive_failures),
                should_quarantine=False,
                matched_pattern=f"HTTP_{http_status}",
                confidence=0.8,
            )

        # 1. Hard Rate Limits (HTTP 429 or explicit quota text)
        if http_status == 429:
            cooldown = self.calculate_cooldown(AnomalyType.HARD_RATE_LIMIT, consecutive_failures)
            return ClassificationResult(
                anomaly_type=AnomalyType.HARD_RATE_LIMIT,
                recommended_action="QUARANTINE",
                cooldown_seconds=cooldown,
                should_quarantine=True,
                matched_pattern="HTTP_429",
                confidence=1.0,
            )

        for pat in self.policy.rate_limit_patterns:
            if _contains(lowered, pat):
                cooldown = self.calculate_cooldown(AnomalyType.HARD_RATE_LIMIT, consecutive_failures)
                return ClassificationResult(
                    anomaly_type=AnomalyType.HARD_RATE_LIMIT,
                    recommended_action="QUARANTINE",
                    cooldown_seconds=cooldown,
                    should_quarantine=True,
                    matched_pattern=pat,
                    confidence=0.95,
                )

        # 2. Process Crashes or Target Disconnections
        if any(_contains(lowered, term) for term in _CRASH_TERMS):
            return ClassificationResult(
                anomaly_type=AnomalyType.PROCESS_CRASH,
                recommended_action="RECYCLE_NODE",
                cooldown_seconds=15.0,
                should_quarantine=False,
                matched_pattern="process_crash",
                confidence=0.98,
            )

        # 3. Session Contamination
        if any(_contains(lowered, term) for term in _AUTH_TERMS):
            return ClassificationResult(
                anomaly_type=AnomalyType.SESSION_CONTAMINATED,
                recommended_action="PURGE_SESSION",
                cooldown_seconds=30.0,
                should_quarantine=True,
                matched_pattern="session_contamination",
                confidence=0.92,
            )

        # 4. Soft Transient False Alarms (Content rejections, timeouts, DOM races)
        for pat in self.policy.transient_patterns:
            if _contains(lowered, pat):
                # If consecutive failures exceed threshold, escalate to quarantine
                if consecutive_failures >= self.policy.max_consecutive_failures:
                    cooldown = self.calculate_cooldown(AnomalyType.HARD_RATE_LIMIT, consecutive_failures)
                    return ClassificationResult(
                        anomaly_type=AnomalyType.HARD_RATE_LIMIT,
                        recommended_action="QUARANTINE_ESCALATION",
                        cooldown_seconds=cooldown,
                        should_quarantine=True,
                        matched_pattern=f"escalated_{pat}",
                        confidence=0.85,
                    )

                cooldown = self.calculate_cooldown(AnomalyType.FALSE_ALARM, consecutive_failures)
                return ClassificationResult(
                    anomaly_type=AnomalyType.FALSE_ALARM,
                    recommended_action="FAST_RETRY",
                    cooldown_seconds=cooldown,
                    should_quarantine=False,
                    matched_pattern=pat,
                    confidence=0.90,
                )

        # 5. Fallback Unknown
        cooldown = self.calculate_cooldown(AnomalyType.UNKNOWN_ERROR, consecutive_failures)
        should_quar = consecutive_failures >= self.policy.max_consecutive_failures
        return ClassificationResult(
            anomaly_type=AnomalyType.UNKNOWN_ERROR,
            recommended_action="QUARANTINE" if should_quar else "EXPONENTIAL_BACKOFF",
            cooldown_seconds=cooldown,
            should_quarantine=should_quar,
            matched_pattern=None,
            confidence=0.50,
        )

    def calculate_cooldown(self, anomaly_type: AnomalyType, consecutive_failures: int = 0) -> float:
        """Computes exponential backoff with optional pseudo-random jitter."""
        if anomaly_type == AnomalyType.FALSE_ALARM:
            base = self.policy.soft_limit_cooldown_sec
        elif anomaly_type == AnomalyType.HARD_RATE_LIMIT:
            base = self.policy.hard_limit_cooldown_sec
        else:
            base = self.policy.soft_limit_cooldown_sec * 1.5

        # Exponential scaling
        exp_factor = math.pow(self.policy.backoff_multiplier, min(consecutive_failures, 4))
        cooldown = base * exp_factor

        # Add +/- 10% jitter if enabled to avoid synchronized thundering herd
        if self.policy.jitter:
            jitter_factor = random.uniform(0.9, 1.1)
            cooldown *= jitter_factor

        return round(cooldown, 2)

    def record_quarantine(
        self,
        node_id: str,
        anomaly_type: AnomalyType,
        reason: str,
        duration_sec: Optional[float] = None,
        consecutive_failures: int = 1,
    ) -> QuarantineRecord:
        """Places a node into holding quarantine with expiration tracking."""
        now = time.time()
        dur = duration_sec or self.policy.quarantine_duration_sec
        record = QuarantineRecord(
            node_id=node_id,
            anomaly_type=anomaly_type,
            reason=reason,
            quarantined_at=now,
            duration_sec=dur,
            expires_at=now + dur,
            failure_count=consecutive_failures,
        )
        self.quarantine_registry[node_id] = record
        self.historical_quarantines.append(record)
        logger.warning(
            "Node %s entered quarantine until +%.1fs (Type: %s, Reason: %s)",
            node_id,
            dur,
            anomaly_type.value,
            reason,
        )
        return record

    def is_quarantined(self, node_id: str, current_time: Optional[float] = None) -> bool:
        """Checks if a node currently resides within an unexpired quarantine period."""
        record = self.quarantine_registry.get(node_id)
        if not record:
            return False
        now = time.time() if current_time is None else current_time
        if now >= record.expires_at:
            # Expired; remove from active registry
            del self.quarantine_registry[node_id]
            return False
        return True

    def check_and_clear_expired_quarantines(self, current_time: Optional[float] = None) -> List[str]:
        """Scans active quarantines, purging expired entries and returning recovered node IDs."""
        now = time.time() if current_time is None else current_time
        recovered: List[str] = []
        expired_keys = [nid for nid, rec in self.quarantine_registry.items() if now >= rec.expires_at]

        for nid in expired_keys:
            del self.quarantine_registry[nid]
            recovered.append(nid)
            logger.info("Quarantine expired for %s; returned to available fleet", nid)

        return recovered

    def clear_quarantine(self, node_id: str) -> bool:
        """Manually releases a node from holding quarantine."""
        if node_id in self.quarantine_registry:
            del self.quarantine_registry[node_id]
            logger.info("Manually cleared quarantine for %s", node_id)
            return True
        return False
