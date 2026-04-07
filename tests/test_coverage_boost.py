"""
Additional targeted tests to boost coverage of edge cases:
- consume_continuous KeyboardInterrupt and exception paths
- invalid events in consumer batch (invalid path)
- event_processor maybe_flush time-based path
- s3_checkpoint cache hit path
- pipeline_health count_dlq_last_hour
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# -----------------------------------------------------------------------
# KinesisConsumer: consume_continuous with KeyboardInterrupt
# -----------------------------------------------------------------------

@pytest.fixture(autouse=True)
def local_mode(monkeypatch):
    monkeypatch.setenv("KINESIS_MODE", "local")


def test_consume_continuous_stops_on_keyboard_interrupt(tmp_path):
    from consumer.event_processor import EventProcessor
    from consumer.kinesis_consumer import KinesisConsumer
    from storage.s3_checkpoint import S3Checkpoint

    processor = EventProcessor(flush_size=100, flush_interval=9999.0, dlq_path=tmp_path / "dlq.jsonl")
    checkpoint = S3Checkpoint(local_path=tmp_path / "cp.json")
    consumer = KinesisConsumer(
        local_input_path=tmp_path / "empty.jsonl",
        checkpoint=checkpoint,
        processor=processor,
    )

    call_count = 0

    def _raise_on_second(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            raise KeyboardInterrupt
        return {"total": 0, "valid": 0, "invalid": 0}

    with patch.object(consumer, "consume_batch", side_effect=_raise_on_second):
        consumer.consume_continuous(poll_interval=0.0)

    assert call_count == 2


def test_consume_continuous_handles_exception(tmp_path):
    from consumer.event_processor import EventProcessor
    from consumer.kinesis_consumer import KinesisConsumer
    from storage.s3_checkpoint import S3Checkpoint

    processor = EventProcessor(flush_size=100, flush_interval=9999.0, dlq_path=tmp_path / "dlq.jsonl")
    checkpoint = S3Checkpoint(local_path=tmp_path / "cp.json")
    consumer = KinesisConsumer(
        local_input_path=tmp_path / "empty.jsonl",
        checkpoint=checkpoint,
        processor=processor,
    )

    call_count = 0

    def _raise_then_interrupt(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("transient error")
        raise KeyboardInterrupt

    with patch.object(consumer, "consume_batch", side_effect=_raise_then_interrupt):
        consumer.consume_continuous(poll_interval=0.0)

    assert call_count == 2


# -----------------------------------------------------------------------
# KinesisConsumer: invalid events counted
# -----------------------------------------------------------------------

def test_consume_batch_counts_invalid(tmp_path):
    from consumer.event_processor import EventProcessor
    from consumer.kinesis_consumer import KinesisConsumer
    from storage.s3_checkpoint import S3Checkpoint

    path = tmp_path / "events.jsonl"
    # One valid, one invalid
    with path.open("w") as f:
        f.write(json.dumps({
            "event_type": "OrderEvent",
            "data": {
                "event_id": "ev-001", "event_type": "order_placed",
                "order_id": "ord-001", "user_id": "usr-001", "session_id": "ses-001",
                "items": [{"product_id": "P1", "quantity": 1, "unit_price": 5.0}],
                "currency": "USD", "total_amount": 5.0, "discount_amount": 0.0,
                "payment_method": "credit_card", "country_code": "US",
                "timestamp": "2024-01-01T00:00:00+00:00",
            }
        }) + "\n")
        f.write(json.dumps({"event_type": "OrderEvent", "data": {"garbage": True}}) + "\n")

    processor = EventProcessor(flush_size=100, flush_interval=9999.0, dlq_path=tmp_path / "dlq.jsonl")
    checkpoint = S3Checkpoint(local_path=tmp_path / "cp.json")
    consumer = KinesisConsumer(local_input_path=path, checkpoint=checkpoint, processor=processor)

    stats = consumer.consume_batch(max_records=10)
    assert stats["valid"] == 1
    assert stats["invalid"] == 1


# -----------------------------------------------------------------------
# EventProcessor: maybe_flush time-based trigger
# -----------------------------------------------------------------------

def test_maybe_flush_triggers_after_interval(tmp_path):
    from consumer.event_processor import EventProcessor
    writer = MagicMock()
    proc = EventProcessor(
        flush_size=1000,
        flush_interval=0.01,   # very short
        dlq_path=tmp_path / "dlq.jsonl",
        writer=writer,
    )
    # Add an event
    proc._buffer.append(MagicMock())
    # Force _last_flush to be old enough
    proc._last_flush = time.monotonic() - 1.0
    proc.maybe_flush()
    writer.assert_called_once()


def test_maybe_flush_no_op_when_empty(tmp_path):
    from consumer.event_processor import EventProcessor
    writer = MagicMock()
    proc = EventProcessor(flush_size=1000, flush_interval=0.0, dlq_path=tmp_path / "dlq.jsonl", writer=writer)
    proc.maybe_flush()
    writer.assert_not_called()


def test_flush_empty_buffer_returns_empty_list(tmp_path):
    """flush() on empty buffer returns [] (line 108)."""
    from consumer.event_processor import EventProcessor
    proc = EventProcessor(flush_size=100, flush_interval=9999.0, dlq_path=tmp_path / "dlq.jsonl")
    result = proc.flush()
    assert result == []


def test_infer_class_from_order_event_type(tmp_path):
    """_infer_class routes order event_types (lines 144-145)."""
    from consumer.event_processor import EventProcessor
    proc = EventProcessor(flush_size=100, flush_interval=9999.0, dlq_path=tmp_path / "dlq.jsonl")
    # Raw dict with no envelope class — only data event_type
    result = proc.process({"data": {
        "event_id": "e1", "event_type": "order_placed", "order_id": "o1",
        "user_id": "u1", "session_id": "s1",
        "items": [{"product_id": "P1", "quantity": 1, "unit_price": 1.0}],
        "currency": "USD", "total_amount": 1.0, "discount_amount": 0.0,
        "payment_method": "credit_card", "country_code": "US",
        "timestamp": "2024-01-01T00:00:00+00:00",
    }, "event_type": "order_placed"})
    # event_type "order_placed" is in _EVENT_CLASS_MAP? No — it maps to OrderEvent
    # The envelope key is "order_placed" not "OrderEvent", so _infer_class falls through
    assert result is not None


def test_infer_class_from_session_event_type(tmp_path):
    """_infer_class routes session event_types (lines 146-147)."""
    from consumer.event_processor import EventProcessor
    proc = EventProcessor(flush_size=100, flush_interval=9999.0, dlq_path=tmp_path / "dlq.jsonl")
    result = proc.process({"data": {
        "event_id": "e2", "event_type": "page_view", "session_id": "s1",
        "user_id": "u1", "page_url": "/", "referrer": "", "device_type": "desktop",
        "country_code": "US", "timestamp": "2024-01-01T00:00:00+00:00",
    }, "event_type": "page_view"})
    assert result is not None


def test_infer_class_from_cart_event_type(tmp_path):
    """_infer_class routes cart event_types (lines 148-149)."""
    from consumer.event_processor import EventProcessor
    proc = EventProcessor(flush_size=100, flush_interval=9999.0, dlq_path=tmp_path / "dlq.jsonl")
    result = proc.process({"data": {
        "event_id": "e3", "event_type": "add_to_cart", "cart_id": "c1",
        "session_id": "s1", "user_id": "u1", "product_id": "P1",
        "quantity": 1, "unit_price": 9.99, "timestamp": "2024-01-01T00:00:00+00:00",
    }, "event_type": "add_to_cart"})
    assert result is not None


def test_consumer_max_records_respected(tmp_path):
    """Consumer stops reading at max_records (line 129)."""
    import json
    path = tmp_path / "events.jsonl"
    with path.open("w") as f:
        for i in range(10):
            f.write(json.dumps({
                "event_type": "OrderEvent",
                "data": {
                    "event_id": f"ev-{i:03d}", "event_type": "order_placed",
                    "order_id": f"ord-{i:03d}", "user_id": "u1", "session_id": "s1",
                    "items": [{"product_id": "P1", "quantity": 1, "unit_price": 1.0}],
                    "currency": "USD", "total_amount": 1.0, "discount_amount": 0.0,
                    "payment_method": "credit_card", "country_code": "US",
                    "timestamp": "2024-01-01T00:00:00+00:00",
                }
            }) + "\n")

    from consumer.event_processor import EventProcessor
    from consumer.kinesis_consumer import KinesisConsumer
    from storage.s3_checkpoint import S3Checkpoint

    processor = EventProcessor(flush_size=100, flush_interval=9999.0, dlq_path=tmp_path / "dlq.jsonl")
    checkpoint = S3Checkpoint(local_path=tmp_path / "cp.json")
    consumer = KinesisConsumer(local_input_path=path, checkpoint=checkpoint, processor=processor)

    stats = consumer.consume_batch(max_records=3)
    assert stats["total"] == 3


# -----------------------------------------------------------------------
# S3Checkpoint: get returns cached value without re-reading file
# -----------------------------------------------------------------------

def test_checkpoint_get_uses_cache(tmp_path):
    from storage.s3_checkpoint import S3Checkpoint
    cp = S3Checkpoint(local_path=tmp_path / "cp.json")
    cp.save("key", "value1")
    # Corrupt the file after save — get should still return cached value
    (tmp_path / "cp.json").write_text("{}")
    assert cp.get("key") == "value1"


# -----------------------------------------------------------------------
# PipelineHealthMonitor: _count_dlq_last_hour
# -----------------------------------------------------------------------

def test_count_dlq_last_hour_counts_all_lines(tmp_path):
    from monitoring.pipeline_health import PipelineHealthMonitor
    dlq = tmp_path / "dlq.jsonl"
    dlq.write_text("line1\nline2\nline3\n")
    monitor = PipelineHealthMonitor(dlq_path=dlq)
    count = monitor._count_dlq_last_hour()
    assert count == 3


def test_count_dlq_last_hour_missing_file(tmp_path):
    from monitoring.pipeline_health import PipelineHealthMonitor
    monitor = PipelineHealthMonitor(dlq_path=tmp_path / "missing.jsonl")
    assert monitor._count_dlq_last_hour() == 0


def test_count_dlq_last_hour_os_error(tmp_path):
    """OSError when reading DLQ should be swallowed, returning 0."""
    from monitoring.pipeline_health import PipelineHealthMonitor
    dlq = tmp_path / "dlq.jsonl"
    dlq.write_text("line1\n")
    monitor = PipelineHealthMonitor(dlq_path=dlq)
    with patch.object(type(dlq), "open", side_effect=OSError("locked")):
        count = monitor._count_dlq_last_hour()
    assert count == 0


def test_send_to_dlq_os_error_swallowed(tmp_path):
    """OSError when writing DLQ should be swallowed."""
    from consumer.event_processor import EventProcessor
    proc = EventProcessor(flush_size=100, flush_interval=9999.0, dlq_path=tmp_path / "dlq.jsonl")
    with patch.object(type(proc._dlq_path), "open", side_effect=OSError("disk full")):
        # Should not raise
        proc._send_to_dlq({"bad": "record"}, reason="test_error")


# -----------------------------------------------------------------------
# KinesisProducer: failed write returns correct counts
# -----------------------------------------------------------------------

def test_local_write_failure_returns_failed(tmp_path, monkeypatch):
    from pathlib import Path
    from producer.kinesis_producer import KinesisProducer
    from producer.schemas import OrderEvent, OrderItem

    monkeypatch.setenv("KINESIS_MODE", "local")
    p = KinesisProducer(local_output_path=tmp_path / "readonly.jsonl")

    event = OrderEvent(
        event_id="ev-1", event_type="order_placed", order_id="o-1",
        user_id="u-1", session_id="s-1",
        items=[OrderItem(product_id="P1", quantity=1, unit_price=1.0)],
        currency="USD", total_amount=1.0, discount_amount=0.0,
        payment_method="credit_card", country_code="US",
        timestamp="2024-01-01T00:00:00+00:00",
    )

    # Patch Path.open on the producer's local path instance
    with patch.object(type(p._local_path), "open", side_effect=OSError("permission denied")):
        sent, failed = p._write_local([event])

    assert sent == 0
    assert failed == 1


# -----------------------------------------------------------------------
# KinesisProducer: send_events logs batch complete even for 0 events
# -----------------------------------------------------------------------

def test_send_events_returns_dict_structure(tmp_path, monkeypatch):
    from producer.kinesis_producer import KinesisProducer
    from producer.schemas import SessionEvent

    monkeypatch.setenv("KINESIS_MODE", "local")
    p = KinesisProducer(local_output_path=tmp_path / "out.jsonl")
    ev = SessionEvent(
        event_id="ev-s", event_type="session_start", session_id="s1",
        user_id="u1", page_url="/", referrer="", device_type="mobile",
        country_code="US", timestamp="2024-01-01T00:00:00+00:00",
    )
    result = p.send_events([ev])
    assert "sent" in result
    assert "failed" in result
