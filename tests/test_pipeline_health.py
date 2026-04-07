"""Tests for PipelineHealthMonitor in local mode."""
from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def local_mode(monkeypatch):
    monkeypatch.setenv("KINESIS_MODE", "local")


def test_health_check_returns_status(tmp_path):
    from monitoring.pipeline_health import PipelineHealthMonitor, Status
    monitor = PipelineHealthMonitor(dlq_path=tmp_path / "dlq.jsonl")
    health = monitor.check()
    assert health.status in (Status.GREEN, Status.YELLOW, Status.RED)


def test_health_check_fields(tmp_path):
    from monitoring.pipeline_health import PipelineHealthMonitor
    monitor = PipelineHealthMonitor(dlq_path=tmp_path / "dlq.jsonl")
    health = monitor.check()
    assert isinstance(health.events_per_minute, float)
    assert isinstance(health.dead_letter_count, int)
    assert isinstance(health.consumer_lag_seconds, float)
    assert isinstance(health.details, dict)


def test_red_status_when_no_data(tmp_path):
    """With no local kinesis file, lag should be very high → RED."""
    from monitoring.pipeline_health import PipelineHealthMonitor, Status
    monitor = PipelineHealthMonitor(dlq_path=tmp_path / "dlq.jsonl")
    health = monitor.check()
    # No data → lag=9999 → RED
    assert health.status == Status.RED


def test_dlq_count_reflects_file(tmp_path):
    from monitoring.pipeline_health import PipelineHealthMonitor
    dlq = tmp_path / "dlq.jsonl"
    dlq.write_text(
        json.dumps({"reason": "validation_error", "raw": {}}) + "\n" +
        json.dumps({"reason": "validation_error", "raw": {}}) + "\n"
    )
    monitor = PipelineHealthMonitor(dlq_path=dlq)
    health = monitor.check()
    assert health.dead_letter_count == 2


def test_zero_dlq_when_no_file(tmp_path):
    from monitoring.pipeline_health import PipelineHealthMonitor
    monitor = PipelineHealthMonitor(dlq_path=tmp_path / "missing.jsonl")
    health = monitor.check()
    assert health.dead_letter_count == 0


def test_compute_status_green():
    from monitoring.pipeline_health import PipelineHealthMonitor, Status
    m = PipelineHealthMonitor.__new__(PipelineHealthMonitor)
    m.EPM_YELLOW_THRESHOLD = 10.0
    m.EPM_RED_THRESHOLD = 1.0
    m.DLQ_YELLOW_THRESHOLD = 50
    m.DLQ_RED_THRESHOLD = 200
    m.LAG_YELLOW_SECONDS = 120
    m.LAG_RED_SECONDS = 300
    assert m._compute_status(50.0, 0, 5.0) == Status.GREEN


def test_compute_status_yellow_low_epm():
    from monitoring.pipeline_health import PipelineHealthMonitor, Status
    m = PipelineHealthMonitor.__new__(PipelineHealthMonitor)
    m.EPM_YELLOW_THRESHOLD = 10.0
    m.EPM_RED_THRESHOLD = 1.0
    m.DLQ_YELLOW_THRESHOLD = 50
    m.DLQ_RED_THRESHOLD = 200
    m.LAG_YELLOW_SECONDS = 120
    m.LAG_RED_SECONDS = 300
    assert m._compute_status(5.0, 0, 10.0) == Status.YELLOW


def test_compute_status_red_high_lag():
    from monitoring.pipeline_health import PipelineHealthMonitor, Status
    m = PipelineHealthMonitor.__new__(PipelineHealthMonitor)
    m.EPM_YELLOW_THRESHOLD = 10.0
    m.EPM_RED_THRESHOLD = 1.0
    m.DLQ_YELLOW_THRESHOLD = 50
    m.DLQ_RED_THRESHOLD = 200
    m.LAG_YELLOW_SECONDS = 120
    m.LAG_RED_SECONDS = 300
    assert m._compute_status(50.0, 0, 999.0) == Status.RED


def test_compute_status_red_high_dlq():
    from monitoring.pipeline_health import PipelineHealthMonitor, Status
    m = PipelineHealthMonitor.__new__(PipelineHealthMonitor)
    m.EPM_YELLOW_THRESHOLD = 10.0
    m.EPM_RED_THRESHOLD = 1.0
    m.DLQ_YELLOW_THRESHOLD = 50
    m.DLQ_RED_THRESHOLD = 200
    m.LAG_YELLOW_SECONDS = 120
    m.LAG_RED_SECONDS = 300
    assert m._compute_status(50.0, 500, 5.0) == Status.RED
