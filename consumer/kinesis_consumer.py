"""
KinesisConsumer: reads from Kinesis shard iterator or local JSONL file.
Routes deserialized events to EventProcessor.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Iterator, Optional

import structlog

from consumer.event_processor import EventProcessor
from producer.schemas import CartEvent, OrderEvent, SessionEvent
from storage.s3_checkpoint import S3Checkpoint

logger = structlog.get_logger(__name__)

_EVENT_CLASS_MAP = {
    "OrderEvent": OrderEvent,
    "SessionEvent": SessionEvent,
    "CartEvent": CartEvent,
}


class KinesisConsumer:
    """
    Consumes events from Kinesis or a local JSONL file.

    Parameters
    ----------
    stream_name : str
        Kinesis stream name.
    shard_id : str
        Shard to consume from (default SHARDID-000000000000).
    region_name : str
        AWS region.
    local_input_path : Path | str | None
        JSONL file to read in local mode.
    checkpoint : S3Checkpoint | None
        Checkpoint store for sequence numbers.
    processor : EventProcessor | None
        Processor to route events to.
    """

    def __init__(
        self,
        stream_name: str = "ecommerce-events",
        shard_id: str = "shardId-000000000000",
        region_name: str = "us-east-1",
        local_input_path: Path | str | None = None,
        checkpoint: Optional[S3Checkpoint] = None,
        processor: Optional[EventProcessor] = None,
    ) -> None:
        self.stream_name = stream_name
        self.shard_id = shard_id
        self._mode = os.environ.get("KINESIS_MODE", "local").lower()
        self._checkpoint = checkpoint or S3Checkpoint()
        self._processor = processor or EventProcessor()

        if self._mode == "local":
            path = local_input_path or Path("data/kinesis_local.jsonl")
            self._local_path = Path(path)
            self._client = None
            logger.info("KinesisConsumer in LOCAL mode", path=str(self._local_path))
        else:
            import boto3
            self._client = boto3.client("kinesis", region_name=region_name)
            self._local_path = None
            logger.info("KinesisConsumer in KINESIS mode", stream=stream_name)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def consume_batch(self, max_records: int = 1000) -> dict[str, int]:
        """Consume one batch of records; returns processing stats."""
        if self._mode == "local":
            records = list(self._read_local(max_records))
        else:
            records = list(self._read_kinesis(max_records))

        stats = {"total": len(records), "valid": 0, "invalid": 0}
        for raw in records:
            result = self._processor.process(raw)
            if result:
                stats["valid"] += 1
            else:
                stats["invalid"] += 1

        self._processor.maybe_flush()
        logger.info("Batch consumed", **stats)
        return stats

    def consume_continuous(self, poll_interval: float = 1.0) -> None:
        """Poll indefinitely."""
        logger.info("Starting continuous consumption", poll_interval=poll_interval)
        while True:
            try:
                self.consume_batch()
            except KeyboardInterrupt:
                logger.info("Consumer stopped by user")
                break
            except Exception as exc:
                logger.error("Unexpected error in consumer loop", error=str(exc))
            time.sleep(poll_interval)

    # ------------------------------------------------------------------
    # Local mode reader
    # ------------------------------------------------------------------

    def _read_local(self, max_records: int) -> Iterator[dict]:
        if not self._local_path.exists():
            logger.warning("Local JSONL not found", path=str(self._local_path))
            return

        offset_key = f"local:{self._local_path}"
        offset = int(self._checkpoint.get(offset_key) or 0)

        read = 0
        try:
            with self._local_path.open("r", encoding="utf-8") as f:
                for i, line in enumerate(f):
                    if i < offset:
                        continue
                    if read >= max_records:
                        break
                    try:
                        yield json.loads(line.strip())
                        read += 1
                        offset += 1
                    except json.JSONDecodeError as exc:
                        logger.warning("JSON decode error", line=i, error=str(exc))
        finally:
            self._checkpoint.save(offset_key, str(offset))

    # ------------------------------------------------------------------
    # Kinesis mode reader
    # ------------------------------------------------------------------

    def _read_kinesis(self, max_records: int) -> Iterator[dict]:
        checkpoint_key = f"{self.stream_name}:{self.shard_id}"
        seq = self._checkpoint.get(checkpoint_key)

        if seq:
            iterator_resp = self._client.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId=self.shard_id,
                ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                StartingSequenceNumber=seq,
            )
        else:
            iterator_resp = self._client.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId=self.shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )

        shard_iterator = iterator_resp["ShardIterator"]
        response = self._client.get_records(
            ShardIterator=shard_iterator,
            Limit=max_records,
        )

        last_seq = None
        for record in response.get("Records", []):
            try:
                data = json.loads(record["Data"].decode("utf-8"))
                last_seq = record["SequenceNumber"]
                yield data
            except Exception as exc:
                logger.warning("Failed to decode Kinesis record", error=str(exc))

        if last_seq:
            self._checkpoint.save(checkpoint_key, last_seq)
