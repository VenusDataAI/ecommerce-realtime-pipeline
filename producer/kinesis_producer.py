"""
KinesisProducer: wraps boto3 Kinesis put_records with batching and retry.
In local mode (KINESIS_MODE=local), writes to a JSONL file instead.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Union

import structlog

from producer.schemas import CartEvent, OrderEvent, SessionEvent

logger = structlog.get_logger(__name__)

Event = Union[OrderEvent, SessionEvent, CartEvent]

MAX_RECORDS_PER_CALL = 500
MAX_RETRIES = 5
BASE_BACKOFF = 0.5  # seconds


class KinesisProducer:
    """
    Publishes events to AWS Kinesis Data Streams or a local JSONL file.

    Parameters
    ----------
    stream_name : str
        Kinesis stream name (ignored in local mode).
    region_name : str
        AWS region (ignored in local mode).
    local_output_path : Path | str | None
        Path to write JSONL in local mode. Defaults to data/kinesis_local.jsonl.
    """

    def __init__(
        self,
        stream_name: str = "ecommerce-events",
        region_name: str = "us-east-1",
        local_output_path: Path | str | None = None,
    ) -> None:
        self.stream_name = stream_name
        self.region_name = region_name
        self._mode = os.environ.get("KINESIS_MODE", "local").lower()

        if self._mode == "local":
            path = local_output_path or Path("data/kinesis_local.jsonl")
            self._local_path = Path(path)
            self._local_path.parent.mkdir(parents=True, exist_ok=True)
            self._client = None
            logger.info("KinesisProducer running in LOCAL mode", path=str(self._local_path))
        else:
            import boto3
            self._client = boto3.client("kinesis", region_name=region_name)
            self._local_path = None
            logger.info("KinesisProducer running in KINESIS mode", stream=stream_name)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def send_events(self, events: list[Event]) -> dict[str, int]:
        """
        Send a list of events. Returns {"sent": N, "failed": M}.
        Automatically batches into groups of MAX_RECORDS_PER_CALL.
        """
        total_sent = 0
        total_failed = 0

        for chunk_start in range(0, len(events), MAX_RECORDS_PER_CALL):
            chunk = events[chunk_start : chunk_start + MAX_RECORDS_PER_CALL]
            if self._mode == "local":
                sent, failed = self._write_local(chunk)
            else:
                sent, failed = self._put_records_kinesis(chunk)
            total_sent += sent
            total_failed += failed

        logger.info("Batch complete", sent=total_sent, failed=total_failed)
        return {"sent": total_sent, "failed": total_failed}

    # ------------------------------------------------------------------
    # Local mode
    # ------------------------------------------------------------------

    def _write_local(self, events: list[Event]) -> tuple[int, int]:
        try:
            with self._local_path.open("a", encoding="utf-8") as f:
                for ev in events:
                    record = {
                        "partition_key": ev.user_id,
                        "event_type": ev.__class__.__name__,
                        "data": json.loads(ev.model_dump_json()),
                    }
                    f.write(json.dumps(record) + "\n")
            return len(events), 0
        except OSError as exc:
            logger.error("Local write failed", error=str(exc))
            return 0, len(events)

    # ------------------------------------------------------------------
    # Kinesis mode
    # ------------------------------------------------------------------

    def _put_records_kinesis(self, events: list[Event]) -> tuple[int, int]:
        records = [
            {
                "Data": ev.model_dump_json().encode("utf-8"),
                "PartitionKey": ev.user_id,
            }
            for ev in events
        ]

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = self._client.put_records(
                    StreamName=self.stream_name,
                    Records=records,
                )
                failed_count = response.get("FailedRecordCount", 0)
                sent = len(records) - failed_count

                if failed_count == 0:
                    return sent, 0

                # Retry only failed records
                failed_records = [
                    records[i]
                    for i, r in enumerate(response["Records"])
                    if "ErrorCode" in r
                ]
                records = failed_records
                logger.warning(
                    "Partial failure, retrying",
                    attempt=attempt,
                    failed=failed_count,
                )
                time.sleep(BASE_BACKOFF * (2 ** (attempt - 1)))

            except Exception as exc:
                logger.error("Kinesis put_records error", attempt=attempt, error=str(exc))
                time.sleep(BASE_BACKOFF * (2 ** (attempt - 1)))

        logger.error("Exhausted retries", remaining=len(records))
        return len(events) - len(records), len(records)
