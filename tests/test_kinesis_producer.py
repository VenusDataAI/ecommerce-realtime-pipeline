"""Tests for KinesisProducer in local mode."""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from producer.schemas import CartEvent, OrderEvent, OrderItem, SessionEvent


def _make_order() -> OrderEvent:
    return OrderEvent(
        event_id="ev-001",
        event_type="order_placed",
        order_id="ord-001",
        user_id="usr-001",
        session_id="ses-001",
        items=[OrderItem(product_id="PROD-0001", quantity=1, unit_price=9.99)],
        currency="USD",
        total_amount=9.99,
        discount_amount=0.0,
        payment_method="credit_card",
        country_code="US",
        timestamp="2024-01-01T00:00:00+00:00",
    )


def _make_session() -> SessionEvent:
    return SessionEvent(
        event_id="ev-002",
        event_type="page_view",
        session_id="ses-001",
        user_id="usr-001",
        page_url="/products",
        referrer="",
        device_type="desktop",
        country_code="US",
        timestamp="2024-01-01T00:01:00+00:00",
    )


def _make_cart() -> CartEvent:
    return CartEvent(
        event_id="ev-003",
        event_type="add_to_cart",
        cart_id="crt-001",
        session_id="ses-001",
        user_id="usr-001",
        product_id="PROD-0001",
        quantity=1,
        unit_price=9.99,
        timestamp="2024-01-01T00:02:00+00:00",
    )


@pytest.fixture(autouse=True)
def local_mode(monkeypatch):
    monkeypatch.setenv("KINESIS_MODE", "local")


@pytest.fixture()
def producer(tmp_path):
    from producer.kinesis_producer import KinesisProducer
    return KinesisProducer(local_output_path=tmp_path / "out.jsonl")


def test_send_single_event_returns_sent(producer):
    result = producer.send_events([_make_order()])
    assert result["sent"] == 1
    assert result["failed"] == 0


def test_send_multiple_events(producer, tmp_path):
    events = [_make_order(), _make_session(), _make_cart()]
    result = producer.send_events(events)
    assert result["sent"] == 3
    assert result["failed"] == 0


def test_local_file_written(producer, tmp_path):
    producer.send_events([_make_order(), _make_session()])
    path = tmp_path / "out.jsonl"
    assert path.exists()
    lines = path.read_text().strip().split("\n")
    assert len(lines) == 2


def test_local_record_structure(producer, tmp_path):
    producer.send_events([_make_order()])
    lines = (tmp_path / "out.jsonl").read_text().strip().split("\n")
    record = json.loads(lines[0])
    assert "partition_key" in record
    assert "event_type" in record
    assert "data" in record
    assert record["partition_key"] == "usr-001"
    assert record["event_type"] == "OrderEvent"


def test_send_empty_batch(producer):
    result = producer.send_events([])
    assert result["sent"] == 0
    assert result["failed"] == 0


def test_large_batch_chunked(tmp_path, monkeypatch):
    """Batches > 500 should be chunked into multiple writes."""
    from producer.kinesis_producer import KinesisProducer
    p = KinesisProducer(local_output_path=tmp_path / "big.jsonl")
    events = [_make_order() for _ in range(600)]
    result = p.send_events(events)
    assert result["sent"] == 600


def test_partition_key_is_user_id(producer, tmp_path):
    producer.send_events([_make_session()])
    record = json.loads((tmp_path / "out.jsonl").read_text().strip())
    assert record["partition_key"] == "usr-001"
