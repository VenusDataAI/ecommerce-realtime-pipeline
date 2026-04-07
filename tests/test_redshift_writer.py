"""Tests for RedshiftWriter: DDL creation, _ingested_at appended."""
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from producer.schemas import CartEvent, OrderEvent, OrderItem, SessionEvent
from storage.redshift_writer import RedshiftWriter


def _make_order() -> OrderEvent:
    return OrderEvent(
        event_id="ev-001",
        event_type="order_placed",
        order_id="ord-001",
        user_id="usr-001",
        session_id="ses-001",
        items=[OrderItem(product_id="PROD-0001", quantity=2, unit_price=9.99)],
        currency="USD",
        total_amount=19.98,
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
        referrer="https://google.com",
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


@pytest.fixture()
def mock_conn():
    conn = MagicMock()
    conn.closed = False
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cursor


@pytest.fixture()
def writer(mock_conn):
    conn, cur = mock_conn
    with patch("psycopg2.connect", return_value=conn):
        w = RedshiftWriter(dsn="mock://localhost/test")
        w._conn = conn
    return w, conn, cur


def test_ensure_tables_creates_schema(writer):
    w, conn, cur = writer
    w.ensure_tables()
    calls = [str(c) for c in cur.execute.call_args_list]
    assert any("CREATE SCHEMA" in c for c in calls)


def test_ensure_tables_creates_all_three(writer):
    w, conn, cur = writer
    w.ensure_tables()
    calls = " ".join(str(c) for c in cur.execute.call_args_list)
    assert "raw_order_events" in calls
    assert "raw_session_events" in calls
    assert "raw_cart_events" in calls


def test_write_order_includes_ingested_at(writer):
    w, conn, cur = writer
    w.write([_make_order()])
    cur.executemany.assert_called_once()
    rows = cur.executemany.call_args[0][1]
    assert len(rows) == 1
    ingested_at = rows[0][-1]
    datetime.fromisoformat(ingested_at)


def test_write_session_includes_ingested_at(writer):
    w, conn, cur = writer
    w.write([_make_session()])
    cur.executemany.assert_called_once()
    rows = cur.executemany.call_args[0][1]
    ingested_at = rows[0][-1]
    datetime.fromisoformat(ingested_at)


def test_write_cart_includes_ingested_at(writer):
    w, conn, cur = writer
    w.write([_make_cart()])
    rows = cur.executemany.call_args[0][1]
    ingested_at = rows[0][-1]
    datetime.fromisoformat(ingested_at)


def test_write_empty_noop(writer):
    w, conn, cur = writer
    w.write([])
    cur.executemany.assert_not_called()


def test_write_mixed_batch(writer):
    w, conn, cur = writer
    w.write([_make_order(), _make_session(), _make_cart()])
    assert cur.executemany.call_count == 3


def test_write_order_row_structure(writer):
    w, conn, cur = writer
    w.write([_make_order()])
    rows = cur.executemany.call_args[0][1]
    row = rows[0]
    # event_id, event_type, order_id, user_id, session_id, items,
    # currency, total_amount, discount_amount, payment_method, country_code,
    # timestamp, _ingested_at  → 13 columns
    assert len(row) == 13
    assert row[0] == "ev-001"
    assert row[1] == "order_placed"


def test_close_connection(writer):
    w, conn, cur = writer
    w.close()
    conn.close.assert_called_once()
