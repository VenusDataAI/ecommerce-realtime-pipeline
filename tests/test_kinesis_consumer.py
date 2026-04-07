"""Tests for KinesisConsumer in local mode."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from producer.schemas import CartEvent, OrderEvent, SessionEvent


def _make_local_record(event_type_name: str, data: dict) -> str:
    return json.dumps({"event_type": event_type_name, "data": data}) + "\n"


def _order_data() -> dict:
    return {
        "event_id": "ev-001",
        "event_type": "order_placed",
        "order_id": "ord-001",
        "user_id": "usr-001",
        "session_id": "ses-001",
        "items": [{"product_id": "PROD-0001", "quantity": 1, "unit_price": 9.99}],
        "currency": "USD",
        "total_amount": 9.99,
        "discount_amount": 0.0,
        "payment_method": "credit_card",
        "country_code": "US",
        "timestamp": "2024-01-01T00:00:00+00:00",
    }


@pytest.fixture(autouse=True)
def local_mode(monkeypatch):
    monkeypatch.setenv("KINESIS_MODE", "local")


@pytest.fixture()
def jsonl_file(tmp_path):
    """A local JSONL file with 3 valid events."""
    path = tmp_path / "events.jsonl"
    with path.open("w") as f:
        f.write(_make_local_record("OrderEvent", _order_data()))
        f.write(_make_local_record("OrderEvent", {**_order_data(), "event_id": "ev-002", "order_id": "ord-002"}))
        f.write(_make_local_record("OrderEvent", {**_order_data(), "event_id": "ev-003", "order_id": "ord-003"}))
    return path


def test_consume_batch_processes_records(tmp_path, jsonl_file):
    from consumer.event_processor import EventProcessor
    from consumer.kinesis_consumer import KinesisConsumer
    from storage.s3_checkpoint import S3Checkpoint

    processor = EventProcessor(flush_size=100, flush_interval=9999.0, dlq_path=tmp_path / "dlq.jsonl")
    checkpoint = S3Checkpoint(local_path=tmp_path / "cp.json")
    consumer = KinesisConsumer(local_input_path=jsonl_file, checkpoint=checkpoint, processor=processor)

    stats = consumer.consume_batch(max_records=10)
    assert stats["total"] == 3
    assert stats["valid"] == 3
    assert stats["invalid"] == 0


def test_consume_batch_missing_file(tmp_path):
    from consumer.event_processor import EventProcessor
    from consumer.kinesis_consumer import KinesisConsumer
    from storage.s3_checkpoint import S3Checkpoint

    processor = EventProcessor(flush_size=100, flush_interval=9999.0, dlq_path=tmp_path / "dlq.jsonl")
    checkpoint = S3Checkpoint(local_path=tmp_path / "cp.json")
    consumer = KinesisConsumer(
        local_input_path=tmp_path / "missing.jsonl",
        checkpoint=checkpoint,
        processor=processor,
    )
    stats = consumer.consume_batch()
    assert stats["total"] == 0


def test_checkpoint_advances_after_consume(tmp_path, jsonl_file):
    from consumer.event_processor import EventProcessor
    from consumer.kinesis_consumer import KinesisConsumer
    from storage.s3_checkpoint import S3Checkpoint

    processor = EventProcessor(flush_size=100, flush_interval=9999.0, dlq_path=tmp_path / "dlq.jsonl")
    checkpoint = S3Checkpoint(local_path=tmp_path / "cp.json")
    consumer = KinesisConsumer(local_input_path=jsonl_file, checkpoint=checkpoint, processor=processor)

    consumer.consume_batch(max_records=10)
    offset_key = f"local:{jsonl_file}"
    saved = checkpoint.get(offset_key)
    assert saved == "3"


def test_second_consume_reads_no_new_records(tmp_path, jsonl_file):
    """After consuming all 3 records, a second batch reads 0."""
    from consumer.event_processor import EventProcessor
    from consumer.kinesis_consumer import KinesisConsumer
    from storage.s3_checkpoint import S3Checkpoint

    processor = EventProcessor(flush_size=100, flush_interval=9999.0, dlq_path=tmp_path / "dlq.jsonl")
    checkpoint = S3Checkpoint(local_path=tmp_path / "cp.json")
    consumer = KinesisConsumer(local_input_path=jsonl_file, checkpoint=checkpoint, processor=processor)

    consumer.consume_batch(max_records=10)
    stats2 = consumer.consume_batch(max_records=10)
    assert stats2["total"] == 0


def test_invalid_json_line_skipped(tmp_path):
    path = tmp_path / "bad.jsonl"
    path.write_text("not json\n" + json.dumps({"event_type": "OrderEvent", "data": _order_data()}) + "\n")

    from consumer.event_processor import EventProcessor
    from consumer.kinesis_consumer import KinesisConsumer
    from storage.s3_checkpoint import S3Checkpoint

    processor = EventProcessor(flush_size=100, flush_interval=9999.0, dlq_path=tmp_path / "dlq.jsonl")
    checkpoint = S3Checkpoint(local_path=tmp_path / "cp.json")
    consumer = KinesisConsumer(local_input_path=path, checkpoint=checkpoint, processor=processor)

    stats = consumer.consume_batch(max_records=10)
    # One bad line skipped, one valid line processed
    assert stats["valid"] == 1
