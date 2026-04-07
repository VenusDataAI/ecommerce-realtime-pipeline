"""
Tests for gold dbt models using DuckDB in-memory.
Runs SQL against sample data and asserts metric correctness.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

SAMPLE_DIR = Path(__file__).parent.parent / "data" / "sample_events"


def _load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    records = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


@pytest.fixture(scope="module")
def con():
    """DuckDB in-memory connection with bronze tables populated."""
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE raw_order_events (
            event_id        VARCHAR,
            event_type      VARCHAR,
            order_id        VARCHAR,
            user_id         VARCHAR,
            session_id      VARCHAR,
            items           JSON,
            currency        VARCHAR,
            total_amount    DOUBLE,
            discount_amount DOUBLE,
            payment_method  VARCHAR,
            country_code    VARCHAR,
            timestamp       TIMESTAMPTZ,
            _ingested_at    TIMESTAMPTZ
        )
    """)

    conn.execute("""
        CREATE TABLE raw_session_events (
            event_id     VARCHAR,
            event_type   VARCHAR,
            session_id   VARCHAR,
            user_id      VARCHAR,
            page_url     VARCHAR,
            referrer     VARCHAR,
            device_type  VARCHAR,
            country_code VARCHAR,
            timestamp    TIMESTAMPTZ,
            _ingested_at TIMESTAMPTZ
        )
    """)

    conn.execute("""
        CREATE TABLE raw_cart_events (
            event_id     VARCHAR,
            event_type   VARCHAR,
            cart_id      VARCHAR,
            session_id   VARCHAR,
            user_id      VARCHAR,
            product_id   VARCHAR,
            quantity     INTEGER,
            unit_price   DOUBLE,
            timestamp    TIMESTAMPTZ,
            _ingested_at TIMESTAMPTZ
        )
    """)

    now_str = "2024-06-01T12:00:00+00:00"

    orders = _load_jsonl(SAMPLE_DIR / "order_events.jsonl")
    sessions = _load_jsonl(SAMPLE_DIR / "session_events.jsonl")
    carts = _load_jsonl(SAMPLE_DIR / "cart_events.jsonl")

    for o in orders:
        conn.execute(
            "INSERT INTO raw_order_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                o["event_id"], o["event_type"], o["order_id"], o["user_id"],
                o["session_id"], json.dumps(o.get("items", [])),
                o["currency"], o["total_amount"], o["discount_amount"],
                o["payment_method"], o["country_code"], o["timestamp"], now_str,
            ],
        )

    for s in sessions:
        conn.execute(
            "INSERT INTO raw_session_events VALUES (?,?,?,?,?,?,?,?,?,?)",
            [
                s["event_id"], s["event_type"], s["session_id"], s["user_id"],
                s["page_url"], s["referrer"], s["device_type"],
                s["country_code"], s["timestamp"], now_str,
            ],
        )

    for c in carts:
        conn.execute(
            "INSERT INTO raw_cart_events VALUES (?,?,?,?,?,?,?,?,?,?)",
            [
                c["event_id"], c["event_type"], c["cart_id"], c["session_id"],
                c["user_id"], c["product_id"], c["quantity"], c["unit_price"],
                c["timestamp"], now_str,
            ],
        )

    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Gold revenue SQL (DuckDB-compatible version)
# ---------------------------------------------------------------------------

GOLD_REVENUE_SQL = """
WITH placed AS (
    SELECT DISTINCT ON (order_id)
        order_id,
        CAST(timestamp AS DATE)  AS date,
        country_code,
        payment_method,
        total_amount
    FROM raw_order_events
    WHERE event_type = 'order_placed'
    ORDER BY order_id, timestamp DESC
),
refunds AS (
    SELECT order_id FROM raw_order_events WHERE event_type = 'order_refunded'
)
SELECT
    date,
    country_code,
    payment_method,
    COUNT(DISTINCT p.order_id)                                          AS total_orders,
    SUM(p.total_amount)                                                 AS total_revenue_usd,
    AVG(p.total_amount)                                                 AS avg_order_value_usd,
    COUNT(r.order_id)::DOUBLE / NULLIF(COUNT(DISTINCT p.order_id), 0)  AS refund_rate
FROM placed p
LEFT JOIN refunds r ON p.order_id = r.order_id
GROUP BY 1, 2, 3
"""

GOLD_ABANDONED_SQL = """
WITH cart_adds AS (
    SELECT
        cart_id,
        session_id,
        CAST(timestamp AS DATE)     AS date,
        SUM(quantity * unit_price)  AS cart_value
    FROM raw_cart_events
    WHERE event_type = 'add_to_cart'
    GROUP BY 1, 2, 3
),
abandoned AS (
    SELECT DISTINCT cart_id FROM raw_cart_events WHERE event_type = 'cart_abandoned'
),
converted AS (
    SELECT DISTINCT ce.cart_id
    FROM raw_cart_events ce
    JOIN raw_order_events oe ON ce.session_id = oe.session_id
    WHERE ce.event_type = 'add_to_cart' AND oe.event_type = 'order_placed'
)
SELECT
    ca.date,
    COUNT(ab.cart_id)                                                     AS abandoned_carts,
    COUNT(cv.cart_id)                                                     AS converted_carts,
    COUNT(ab.cart_id)::DOUBLE / NULLIF(COUNT(ca.cart_id), 0)             AS abandonment_rate_pct,
    SUM(CASE WHEN ab.cart_id IS NOT NULL THEN ca.cart_value ELSE 0 END)  AS revenue_lost_usd
FROM cart_adds ca
LEFT JOIN abandoned ab ON ca.cart_id = ab.cart_id
LEFT JOIN converted cv ON ca.cart_id = cv.cart_id
GROUP BY 1
"""


# ---------------------------------------------------------------------------
# Revenue tests
# ---------------------------------------------------------------------------

def test_revenue_positive(con):
    rows = con.execute(GOLD_REVENUE_SQL).fetchall()
    if not rows:
        pytest.skip("No sample order data available")
    for row in rows:
        assert row[4] > 0, f"Revenue must be positive, got {row[4]}"


def test_avg_order_value_positive(con):
    rows = con.execute(GOLD_REVENUE_SQL).fetchall()
    if not rows:
        pytest.skip("No sample order data available")
    for row in rows:
        assert row[5] > 0, f"AOV must be positive, got {row[5]}"


def test_refund_rate_between_0_and_1(con):
    rows = con.execute(GOLD_REVENUE_SQL).fetchall()
    if not rows:
        pytest.skip("No sample order data available")
    for row in rows:
        rate = row[6]
        assert 0.0 <= rate <= 1.0, f"Refund rate {rate} out of [0,1]"


def test_total_orders_positive(con):
    rows = con.execute(GOLD_REVENUE_SQL).fetchall()
    if not rows:
        pytest.skip("No sample order data available")
    total = sum(row[3] for row in rows)
    assert total > 0


# ---------------------------------------------------------------------------
# Abandoned cart tests
# ---------------------------------------------------------------------------

def test_abandonment_rate_between_0_and_1(con):
    rows = con.execute(GOLD_ABANDONED_SQL).fetchall()
    if not rows:
        pytest.skip("No cart data available")
    for row in rows:
        rate = row[3]
        if rate is not None:
            assert 0.0 <= rate <= 1.0, f"Abandonment rate {rate} out of range"


def test_abandoned_carts_non_negative(con):
    rows = con.execute(GOLD_ABANDONED_SQL).fetchall()
    if not rows:
        pytest.skip("No cart data available")
    for row in rows:
        assert row[1] >= 0


def test_revenue_lost_non_negative(con):
    rows = con.execute(GOLD_ABANDONED_SQL).fetchall()
    if not rows:
        pytest.skip("No cart data available")
    for row in rows:
        assert row[4] >= 0, f"Revenue lost must be >= 0, got {row[4]}"


# ---------------------------------------------------------------------------
# Bronze table integrity tests
# ---------------------------------------------------------------------------

def test_order_count_positive(con):
    count = con.execute("SELECT COUNT(*) FROM raw_order_events").fetchone()[0]
    assert count > 0, "Expected at least some order events"


def test_no_negative_prices(con):
    result = con.execute(
        "SELECT COUNT(*) FROM raw_order_events WHERE total_amount < 0"
    ).fetchone()[0]
    assert result == 0


def test_session_event_types_valid(con):
    valid = {"session_start", "page_view", "session_end"}
    rows = con.execute("SELECT DISTINCT event_type FROM raw_session_events").fetchall()
    for row in rows:
        assert row[0] in valid, f"Invalid session event_type: {row[0]}"


def test_cart_event_types_valid(con):
    valid = {"add_to_cart", "remove_from_cart", "cart_abandoned"}
    rows = con.execute("SELECT DISTINCT event_type FROM raw_cart_events").fetchall()
    for row in rows:
        assert row[0] in valid, f"Invalid cart event_type: {row[0]}"


def test_all_events_have_event_id(con):
    for table in ("raw_order_events", "raw_session_events", "raw_cart_events"):
        nulls = con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE event_id IS NULL"
        ).fetchone()[0]
        assert nulls == 0, f"NULL event_ids found in {table}"


def test_order_currency_length(con):
    bad = con.execute(
        "SELECT COUNT(*) FROM raw_order_events WHERE LENGTH(currency) != 3"
    ).fetchone()[0]
    assert bad == 0, "All currency codes must be 3 characters"
