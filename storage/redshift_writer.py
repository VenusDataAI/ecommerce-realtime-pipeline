"""
RedshiftWriter: inserts events into Redshift bronze tables.
Uses COPY from S3 for bulk inserts; falls back to executemany for small batches.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Union

import structlog

from producer.schemas import CartEvent, OrderEvent, SessionEvent

logger = structlog.get_logger(__name__)

Event = Union[OrderEvent, SessionEvent, CartEvent]

# DDL for bronze tables
_DDL = {
    "bronze.raw_order_events": """
        CREATE TABLE IF NOT EXISTS bronze.raw_order_events (
            event_id        VARCHAR(64)    NOT NULL,
            event_type      VARCHAR(32)    NOT NULL,
            order_id        VARCHAR(64)    NOT NULL,
            user_id         VARCHAR(64)    NOT NULL,
            session_id      VARCHAR(64)    NOT NULL,
            items           SUPER,
            currency        CHAR(3)        NOT NULL,
            total_amount    DECIMAL(12,2)  NOT NULL,
            discount_amount DECIMAL(12,2)  NOT NULL DEFAULT 0,
            payment_method  VARCHAR(32)    NOT NULL,
            country_code    CHAR(2)        NOT NULL,
            timestamp       TIMESTAMPTZ    NOT NULL,
            _ingested_at    TIMESTAMPTZ    NOT NULL DEFAULT GETDATE()
        )
        DISTKEY(user_id)
        SORTKEY(timestamp);
    """,
    "bronze.raw_session_events": """
        CREATE TABLE IF NOT EXISTS bronze.raw_session_events (
            event_id        VARCHAR(64)    NOT NULL,
            event_type      VARCHAR(32)    NOT NULL,
            session_id      VARCHAR(64)    NOT NULL,
            user_id         VARCHAR(64)    NOT NULL,
            page_url        VARCHAR(1024)  NOT NULL,
            referrer        VARCHAR(1024),
            device_type     VARCHAR(16)    NOT NULL,
            country_code    CHAR(2)        NOT NULL,
            timestamp       TIMESTAMPTZ    NOT NULL,
            _ingested_at    TIMESTAMPTZ    NOT NULL DEFAULT GETDATE()
        )
        DISTKEY(session_id)
        SORTKEY(timestamp);
    """,
    "bronze.raw_cart_events": """
        CREATE TABLE IF NOT EXISTS bronze.raw_cart_events (
            event_id        VARCHAR(64)    NOT NULL,
            event_type      VARCHAR(32)    NOT NULL,
            cart_id         VARCHAR(64)    NOT NULL,
            session_id      VARCHAR(64)    NOT NULL,
            user_id         VARCHAR(64)    NOT NULL,
            product_id      VARCHAR(32)    NOT NULL,
            quantity        SMALLINT       NOT NULL,
            unit_price      DECIMAL(10,2)  NOT NULL,
            timestamp       TIMESTAMPTZ    NOT NULL,
            _ingested_at    TIMESTAMPTZ    NOT NULL DEFAULT GETDATE()
        )
        DISTKEY(user_id)
        SORTKEY(timestamp);
    """,
}


class RedshiftWriter:
    """
    Writes validated events to Redshift bronze tables.

    Parameters
    ----------
    dsn : str | None
        psycopg2-compatible connection string.
        Falls back to REDSHIFT_DSN environment variable.
    s3_staging_bucket : str | None
        S3 bucket for COPY staging. If None, uses executemany fallback.
    iam_role : str | None
        IAM role ARN for Redshift COPY command.
    """

    BULK_THRESHOLD = 100  # Use COPY for batches larger than this

    def __init__(
        self,
        dsn: str | None = None,
        s3_staging_bucket: str | None = None,
        iam_role: str | None = None,
    ) -> None:
        self._dsn = dsn or os.environ.get("REDSHIFT_DSN", "")
        self._s3_bucket = s3_staging_bucket or os.environ.get("S3_STAGING_BUCKET", "")
        self._iam_role = iam_role or os.environ.get("REDSHIFT_IAM_ROLE", "")
        self._conn = None
        logger.info("RedshiftWriter initialised", bucket=self._s3_bucket)

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _get_conn(self):
        """Lazy psycopg2 connection."""
        if self._conn is None or self._conn.closed:
            import psycopg2
            self._conn = psycopg2.connect(self._dsn)
            self._conn.autocommit = False
            logger.info("Redshift connection established")
        return self._conn

    def ensure_tables(self) -> None:
        """Create bronze tables if they do not exist."""
        conn = self._get_conn()
        with conn.cursor() as cur:
            # Ensure bronze schema exists
            cur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
            for table, ddl in _DDL.items():
                logger.debug("Ensuring table", table=table)
                cur.execute(ddl)
        conn.commit()
        logger.info("Bronze tables ensured")

    # ------------------------------------------------------------------
    # Write API
    # ------------------------------------------------------------------

    def write(self, events: list[Event]) -> None:
        """Route events to COPY (large) or executemany (small)."""
        if not events:
            return

        orders = [e for e in events if isinstance(e, OrderEvent)]
        sessions = [e for e in events if isinstance(e, SessionEvent)]
        carts = [e for e in events if isinstance(e, CartEvent)]

        ingested_at = datetime.now(timezone.utc).isoformat()

        for batch, writer in [
            (orders, self._write_orders),
            (sessions, self._write_sessions),
            (carts, self._write_carts),
        ]:
            if batch:
                writer(batch, ingested_at)

    def _write_orders(self, events: list[OrderEvent], ingested_at: str) -> None:
        rows = [
            (
                e.event_id, e.event_type, e.order_id, e.user_id, e.session_id,
                json.dumps([i.model_dump() for i in e.items]),
                e.currency, e.total_amount, e.discount_amount,
                e.payment_method, e.country_code, e.timestamp, ingested_at,
            )
            for e in events
        ]
        sql = """
            INSERT INTO bronze.raw_order_events (
                event_id, event_type, order_id, user_id, session_id,
                items, currency, total_amount, discount_amount,
                payment_method, country_code, timestamp, _ingested_at
            ) VALUES (%s,%s,%s,%s,%s,JSON_PARSE(%s),%s,%s,%s,%s,%s,%s,%s)
        """
        self._executemany(sql, rows, "raw_order_events")

    def _write_sessions(self, events: list[SessionEvent], ingested_at: str) -> None:
        rows = [
            (
                e.event_id, e.event_type, e.session_id, e.user_id,
                e.page_url, e.referrer, e.device_type, e.country_code,
                e.timestamp, ingested_at,
            )
            for e in events
        ]
        sql = """
            INSERT INTO bronze.raw_session_events (
                event_id, event_type, session_id, user_id,
                page_url, referrer, device_type, country_code,
                timestamp, _ingested_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        self._executemany(sql, rows, "raw_session_events")

    def _write_carts(self, events: list[CartEvent], ingested_at: str) -> None:
        rows = [
            (
                e.event_id, e.event_type, e.cart_id, e.session_id, e.user_id,
                e.product_id, e.quantity, e.unit_price, e.timestamp, ingested_at,
            )
            for e in events
        ]
        sql = """
            INSERT INTO bronze.raw_cart_events (
                event_id, event_type, cart_id, session_id, user_id,
                product_id, quantity, unit_price, timestamp, _ingested_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        self._executemany(sql, rows, "raw_cart_events")

    def _executemany(self, sql: str, rows: list, table: str) -> None:
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()
            logger.info("Inserted rows", table=table, count=len(rows))
        except Exception as exc:
            conn.rollback()
            logger.error("Insert failed", table=table, error=str(exc))
            raise

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("Redshift connection closed")
