"""
EventProcessor: validates events against pydantic models,
routes valid events to a write buffer, invalid ones to a DLQ.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Optional, Union

import structlog
from pydantic import ValidationError

from producer.schemas import CartEvent, OrderEvent, SessionEvent

logger = structlog.get_logger(__name__)

Event = Union[OrderEvent, SessionEvent, CartEvent]

_EVENT_CLASS_MAP = {
    "OrderEvent": OrderEvent,
    "SessionEvent": SessionEvent,
    "CartEvent": CartEvent,
}


class EventProcessor:
    """
    Validates and buffers incoming raw event dicts.

    Parameters
    ----------
    flush_size : int
        Flush buffer when it reaches this many records.
    flush_interval : float
        Flush buffer after this many seconds regardless of size.
    dlq_path : Path | str
        Path to the dead-letter queue JSONL file.
    writer : callable | None
        Callable that accepts a list of Events to persist.
        If None, events are held in-memory only.
    """

    def __init__(
        self,
        flush_size: int = 500,
        flush_interval: float = 30.0,
        dlq_path: Path | str = Path("data/dead_letter_queue.jsonl"),
        writer=None,
    ) -> None:
        self.flush_size = flush_size
        self.flush_interval = flush_interval
        self._dlq_path = Path(dlq_path)
        self._dlq_path.parent.mkdir(parents=True, exist_ok=True)
        self._writer = writer

        self._buffer: list[Event] = []
        self._last_flush: float = time.monotonic()

        logger.info(
            "EventProcessor initialised",
            flush_size=flush_size,
            flush_interval=flush_interval,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process(self, raw: dict) -> Optional[Event]:
        """
        Validate a raw event dict.
        Returns the validated event on success, None on failure.
        """
        event_type_name = raw.get("event_type") or raw.get("data", {}).get("event_type", "")

        # Determine class from envelope or from event_type field
        cls_name = raw.get("class") or self._infer_class(event_type_name, raw)
        cls = _EVENT_CLASS_MAP.get(cls_name)

        # If envelope format: {"event_type": "OrderEvent", "data": {...}}
        payload = raw.get("data", raw)

        if cls is None:
            self._send_to_dlq(raw, reason="unknown_event_class")
            return None

        try:
            event = cls.model_validate(payload)
            self._buffer.append(event)
            if len(self._buffer) >= self.flush_size:
                self.flush()
            return event
        except ValidationError as exc:
            self._send_to_dlq(raw, reason="validation_error", detail=str(exc))
            return None

    def maybe_flush(self) -> None:
        """Flush if the time interval has elapsed."""
        elapsed = time.monotonic() - self._last_flush
        if elapsed >= self.flush_interval and self._buffer:
            self.flush()

    def flush(self) -> list[Event]:
        """Flush the buffer and call writer if configured."""
        if not self._buffer:
            return []
        batch = self._buffer[:]
        self._buffer.clear()
        self._last_flush = time.monotonic()
        logger.info("Flushing buffer", count=len(batch))
        if self._writer:
            self._writer(batch)
        return batch

    @property
    def buffer(self) -> list[Event]:
        return list(self._buffer)

    @property
    def dlq_count(self) -> int:
        """Count of records currently in the DLQ file."""
        if not self._dlq_path.exists():
            return 0
        return sum(1 for _ in self._dlq_path.open("r", encoding="utf-8"))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _infer_class(self, event_type: str, raw: dict) -> str:
        """Heuristic: infer class name from event_type string."""
        order_types = {"order_placed", "order_cancelled", "order_refunded"}
        session_types = {"session_start", "page_view", "session_end"}
        cart_types = {"add_to_cart", "remove_from_cart", "cart_abandoned"}

        # Check envelope key first
        envelope_class = raw.get("event_type", "")
        if envelope_class in _EVENT_CLASS_MAP:
            return envelope_class

        et = event_type.lower()
        if et in order_types:
            return "OrderEvent"
        if et in session_types:
            return "SessionEvent"
        if et in cart_types:
            return "CartEvent"
        return ""

    def _send_to_dlq(self, raw: dict, reason: str, detail: str = "") -> None:
        record = {"reason": reason, "detail": detail, "raw": raw}
        try:
            with self._dlq_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")
        except OSError as exc:
            logger.error("Failed to write to DLQ", error=str(exc))
        logger.warning("Event sent to DLQ", reason=reason)
