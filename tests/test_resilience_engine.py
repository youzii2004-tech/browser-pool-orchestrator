"""
Tests for ResilienceEngine anomaly classification and self-healing recovery.
"""

import time
import pytest

from src.config import ResiliencePolicy
from src.resilience_engine import (
    AnomalyType,
    ClassificationResult,
    ResilienceEngine,
)


def test_classify_hard_rate_limit_http_429() -> None:
    engine = ResilienceEngine()
    result = engine.classify_error("Rate limit hit", http_status=429)

    assert result.anomaly_type == AnomalyType.HARD_RATE_LIMIT
    assert result.recommended_action == "QUARANTINE"
    assert result.should_quarantine is True
    assert result.matched_pattern == "HTTP_429"
    assert result.cooldown_seconds >= 250.0


def test_classify_hard_rate_limit_pattern() -> None:
    engine = ResilienceEngine()
    result = engine.classify_error("API Error: account quota exceeded for current billing cycle")

    assert result.anomaly_type == AnomalyType.HARD_RATE_LIMIT
    assert result.recommended_action == "QUARANTINE"
    assert result.should_quarantine is True
    assert result.matched_pattern == "quota exceeded"


def test_classify_false_alarm_transient() -> None:
    engine = ResilienceEngine()
    result = engine.classify_error("Error: DOM race condition, navigation timeout after 30s")

    assert result.anomaly_type == AnomalyType.FALSE_ALARM
    assert result.recommended_action == "FAST_RETRY"
    assert result.should_quarantine is False
    assert result.cooldown_seconds <= 40.0


def test_consecutive_failure_escalation() -> None:
    policy = ResiliencePolicy(max_consecutive_failures=3)
    engine = ResilienceEngine(policy)

    # 3rd failure on transient error escalates to quarantine
    result = engine.classify_error("navigation timeout", consecutive_failures=3)
    assert result.anomaly_type == AnomalyType.HARD_RATE_LIMIT
    assert result.recommended_action == "QUARANTINE_ESCALATION"
    assert result.should_quarantine is True


def test_quarantine_tracking_and_expiration() -> None:
    engine = ResilienceEngine()
    record = engine.record_quarantine(
        node_id="node_07",
        anomaly_type=AnomalyType.HARD_RATE_LIMIT,
        reason="Quota reached",
        duration_sec=30.0,
    )

    assert engine.is_quarantined("node_07") is True
    assert engine.is_quarantined("node_08") is False

    # Simulate future time
    future = time.time() + 45.0
    assert engine.is_quarantined("node_07", current_time=future) is False

    # Check recovery list
    engine.record_quarantine("node_12", AnomalyType.HARD_RATE_LIMIT, "reason", duration_sec=10.0)
    recovered = engine.check_and_clear_expired_quarantines(current_time=time.time() + 15.0)
    assert "node_12" in recovered


def test_exponential_backoff_calculation() -> None:
    policy = ResiliencePolicy(
        soft_limit_cooldown_sec=10.0,
        backoff_multiplier=2.0,
        jitter=False,
    )
    engine = ResilienceEngine(policy)

    cd_0 = engine.calculate_cooldown(AnomalyType.FALSE_ALARM, consecutive_failures=0)
    cd_1 = engine.calculate_cooldown(AnomalyType.FALSE_ALARM, consecutive_failures=1)
    cd_2 = engine.calculate_cooldown(AnomalyType.FALSE_ALARM, consecutive_failures=2)

    assert cd_0 == 10.0
    assert cd_1 == 20.0
    assert cd_2 == 40.0


@pytest.mark.parametrize("message", ["no room left in layout", "zoom level changed", "listening on port 14290"])
def test_substrings_do_not_trigger_false_classifications(message) -> None:
    result = ResilienceEngine().classify_error(message)
    assert result.anomaly_type == AnomalyType.UNKNOWN_ERROR


@pytest.mark.parametrize("message", ["HTTP 429 returned", "status=429", "Too Many Requests"])
def test_real_rate_limit_text_still_matches(message) -> None:
    assert ResilienceEngine().classify_error(message).anomaly_type == AnomalyType.HARD_RATE_LIMIT


def test_http_status_classification() -> None:
    engine = ResilienceEngine()
    assert engine.classify_error("", http_status=503).anomaly_type == AnomalyType.FALSE_ALARM
    assert engine.classify_error("", http_status=401).anomaly_type == AnomalyType.SESSION_CONTAMINATED
    assert engine.classify_error("", http_status=429).should_quarantine


def test_quarantine_check_with_time_zero_is_not_treated_as_now() -> None:
    engine = ResilienceEngine()
    engine.record_quarantine("n", AnomalyType.HARD_RATE_LIMIT, "r", duration_sec=100)
    assert engine.is_quarantined("n", current_time=0.0) is True
