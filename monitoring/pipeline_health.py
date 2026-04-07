"""
PipelineHealthMonitor: queries bronze tables for operational metrics.
Returns GREEN / YELLOW / RED status.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional

import structlog

logger = structlog.get_logger(__name__)


class Status(str, Enum):
    GREEN = "GREEN"
    YELLOW = "YELLOW"
    RED = "RED"


@dataclass
class HealthStatus:
    status: Status
    events_per_minute: float
    dead_letter_count: int
    consumer_lag_seconds: float
    details: dict


class PipelineHealthMonitor:
    """
    Monitors pipeline health by querying bronze tables and the DLQ.

    Parameters
    ----------
    dsn : str | None
        Redshift connection string (ignored in local mode).
    dlq_path : Path | str
        Path to the dead-letter queue JSONL file.
    """

    EPM_YELLOW_THRESHOLD = 10.0    # < 10 events/min -> YELLOW
    EPM_RED_THRESHOLD = 1.0         # < 1 event/min -> RED
    DLQ_YELLOW_THRESHOLD = 50      # > 50 DLQ records/hr -> YELLOW
    DLQ_RED_THRESHOLD = 200        # > 200 DLQ records/hr -> RED
    LAG_YELLOW_SECONDS = 120        # > 2 min lag -> YELLOW
    LAG_RED_SECONDS = 300           # > 5 min lag -> RED

    def __init__(
        self,
        dsn: str | None = None,
        dlq_path: Path | str = Path("data/dead_letter_queue.jsonl"),
    ) -> None:
        self._mode = os.environ.get("KINESIS_MODE", "local").lower()
        self._dsn = dsn or os.environ.get("REDSHIFT_DSN", "")
        self._dlq_path = Path(dlq_path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def check(self) -> HealthStatus:
        """Run all health checks and return a HealthStatus."""
        if self._mode == "local":
            return self._check_local()
        return self._check_redshift()

    # ------------------------------------------------------------------
    # Local mode
    # ------------------------------------------------------------------

    def _check_local(self) -> HealthStatus:
        """Approximate health check using local files."""
        import time
        from pathlib import Path

        # Count DLQ records as proxy for dead_letter_count
        dlq_count = 0
        if self._dlq_path.exists():
            with self._dlq_path.open("r", encoding="utf-8") as f:
                dlq_count = sum(1 for _ in f)

        # Approximate events/min from kinesis_local.jsonl modification time
        local_file = Path("data/kinesis_local.jsonl")
        lag_seconds = 9999.0
        epm = 0.0

        if local_file.exists():
            mtime = local_file.stat().st_mtime
            lag_seconds = time.time() - mtime

            # Rough EPM: count lines written in last 5 min window
            five_min_ago = time.time() - 300
            if mtime > five_min_ago:
                # Can't easily timestamp individual lines in local mode
                # Estimate based on file size growth
                size_kb = local_file.stat().st_size / 1024
                epm = max(0.0, size_kb / 0.5)  # rough heuristic

        status = self._compute_status(epm, dlq_count, lag_seconds)
        hs = HealthStatus(
            status=status,
            events_per_minute=round(epm, 2),
            dead_letter_count=dlq_count,
            consumer_lag_seconds=round(lag_seconds, 1),
            details={"mode": "local"},
        )
        logger.info("Health check complete (local)", status=status.value)
        return hs

    # ------------------------------------------------------------------
    # Redshift mode
    # ------------------------------------------------------------------

    def _check_redshift(self) -> HealthStatus:
        import psycopg2

        try:
            conn = psycopg2.connect(self._dsn)
            with conn.cursor() as cur:
                epm = self._query_epm(cur)
                lag = self._query_lag(cur)
            conn.close()
        except Exception as exc:
            logger.error("Redshift health query failed", error=str(exc))
            return HealthStatus(
                status=Status.RED,
                events_per_minute=0.0,
                dead_letter_count=0,
                consumer_lag_seconds=9999.0,
                details={"error": str(exc)},
            )

        dlq_count = self._count_dlq_last_hour()
        status = self._compute_status(epm, dlq_count, lag)
        hs = HealthStatus(
            status=status,
            events_per_minute=round(epm, 2),
            dead_letter_count=dlq_count,
            consumer_lag_seconds=round(lag, 1),
            details={"mode": "redshift"},
        )
        logger.info("Health check complete", status=status.value)
        return hs

    def _query_epm(self, cur) -> float:
        sql = """
            SELECT COUNT(*) / 5.0
            FROM (
                SELECT _ingested_at FROM bronze.raw_order_events WHERE _ingested_at > DATEADD(min, -5, GETDATE())
                UNION ALL
                SELECT _ingested_at FROM bronze.raw_session_events WHERE _ingested_at > DATEADD(min, -5, GETDATE())
                UNION ALL
                SELECT _ingested_at FROM bronze.raw_cart_events WHERE _ingested_at > DATEADD(min, -5, GETDATE())
            ) sub
        """
        cur.execute(sql)
        row = cur.fetchone()
        return float(row[0]) if row else 0.0

    def _query_lag(self, cur) -> float:
        sql = """
            SELECT EXTRACT(EPOCH FROM (GETDATE() - MAX(_ingested_at)))
            FROM (
                SELECT MAX(_ingested_at) AS _ingested_at FROM bronze.raw_order_events
                UNION ALL
                SELECT MAX(_ingested_at) FROM bronze.raw_session_events
                UNION ALL
                SELECT MAX(_ingested_at) FROM bronze.raw_cart_events
            ) sub
        """
        cur.execute(sql)
        row = cur.fetchone()
        return float(row[0]) if row and row[0] else 9999.0

    def _count_dlq_last_hour(self) -> int:
        if not self._dlq_path.exists():
            return 0
        import json
        import time
        count = 0
        try:
            with self._dlq_path.open("r", encoding="utf-8") as f:
                for line in f:
                    count += 1  # simplified; in prod parse timestamps
        except OSError:
            pass
        return count

    def _compute_status(
        self, epm: float, dlq_count: int, lag_seconds: float
    ) -> Status:
        if (
            epm < self.EPM_RED_THRESHOLD
            or dlq_count > self.DLQ_RED_THRESHOLD
            or lag_seconds > self.LAG_RED_SECONDS
        ):
            return Status.RED
        if (
            epm < self.EPM_YELLOW_THRESHOLD
            or dlq_count > self.DLQ_YELLOW_THRESHOLD
            or lag_seconds > self.LAG_YELLOW_SECONDS
        ):
            return Status.YELLOW
        return Status.GREEN
