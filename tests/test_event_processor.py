"""Tests for EventProcessor: routing, DLQ, buffer flush."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from consumer.event_processor import EventProcessor
from producer.schemas import CartEvent, OrderEvent, SessionEvent


def _order_raw() -> dict:
    return {
        "event_type": "OrderEvent",
        "data": {
            "event_id": "ev-001",
            "event_type": "order_placed",
            "order_id": "ord-001",
            "user_id": "usr-001",
            "session_id": "ses-001",
            "items": [{"product_id": "PROD-0001", "quantity": 2, "unit_price": 9.99}],
            "currency": "USD",
            "total_amount": 19.98,
            "discount_amount": 0.0,
            "payment_method": "credit_card",
            "country_code": "US",
            "timestamp": "2024-01-01T00:00:00+00:00",
        },
    }


def _session_raw() -> dict:
    return {
        "event_type": "SessionEvent",
        "data": {
            "event_id": "ev-002",
            "event_type": "page_view",
            "session_id": "ses-001",
            "user_id": "usr-001",
            "page_url": "/products",
            "referrer": "https://google.com",
            "device_type": "desktop",
            "country_code": "US",
            "timestamp": "2024-01-01T00:01:00+00:00",
        },
    }


def _cart_raw() -> dict:
    return {
        "event_type": "CartEvent",
        "data": {
            "event_id": "ev-003",
            "event_type": "add_to_cart",
            "cart_id": "crt-001",
            "session_id": "ses-001",
            "user_id": "usr-001",
            "product_id": "PROD-0001",
            "quantity": 1,
            "unit_price": 9.99,
            "timestamp": "2024-01-01T00:02:00+00:00",
        },
    }


@pytest.fixture()
def tmp_dlq(tmp_path):
    return tmp_path / "dlq.jsonl"


@pytest.fixture()
def processor(tmp_dlq):
    return EventProcessor(flush_size=100, flush_interval=9999.0, dlq_path=tmp_dlq)


def test_valid_order_routed(processor):
    event = processor.process(_order_raw())
    assert isinstance(event, OrderEvent)
    assert len(processor.buffer) == 1


def test_valid_session_routed(processor):
    event = processor.process(_session_raw())
    assert isinstance(event, SessionEvent)


def test_valid_cart_routed(processor):
    event = processor.process(_cart_raw())
    assert isinstance(event, CartEvent)


def test_invalid_event_goes_to_dlq(processor, tmp_dlq):
    bad = {"event_type": "OrderEvent", "data": {"bad_field": "garbage"}}
    result = processor.process(bad)
    assert result is None
    assert tmp_dlq.exists()
    lines = tmp_dlq.read_text().strip().split("\n")
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["reason"] == "validation_error"


def test_unknown_event_type_goes_to_dlq(processor, tmp_dlq):
    result = processor.process({"event_type": "UnknownEvent", "data": {}})
    assert result is None
    assert tmp_dlq.exists()


def test_buffer_flush_on_size(tmp_dlq):
    writer = MagicMock()
    proc = EventProcessor(flush_size=3, flush_interval=9999.0, dlq_path=tmp_dlq, writer=writer)
    for _ in range(3):
        proc.process(_order_raw())
    writer.assert_called_once()
    assert len(proc.buffer) == 0


def test_manual_flush_returns_events(processor):
    processor.process(_order_raw())
    processor.process(_session_raw())
    flushed = processor.flush()
    assert len(flushed) == 2
    assert len(processor.buffer) == 0


def test_dlq_count(processor, tmp_dlq):
    processor.process({"event_type": "BadEvent", "data": {}})
    processor.process({"event_type": "BadEvent", "data": {}})
    assert processor.dlq_count == 2


def test_multiple_valid_types_buffered(processor):
    processor.process(_order_raw())
    processor.process(_session_raw())
    processor.process(_cart_raw())
    assert len(processor.buffer) == 3


def test_flush_clears_buffer(processor):
    processor.process(_order_raw())
    assert len(processor.buffer) == 1
    processor.flush()
    assert len(processor.buffer) == 0


def test_writer_called_with_correct_events(tmp_dlq):
    writer = MagicMock()
    proc = EventProcessor(flush_size=2, flush_interval=9999.0, dlq_path=tmp_dlq, writer=writer)
    proc.process(_order_raw())
    proc.process(_session_raw())
    writer.assert_called_once()
    batch = writer.call_args[0][0]
    assert len(batch) == 2
    assert isinstance(batch[0], OrderEvent)
    assert isinstance(batch[1], SessionEvent)
