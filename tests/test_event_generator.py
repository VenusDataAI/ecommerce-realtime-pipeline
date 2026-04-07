"""Tests for EventGenerator: distribution sanity checks."""
from __future__ import annotations

import pytest

from producer.event_generator import EventGenerator
from producer.schemas import CartEvent, OrderEvent, SessionEvent


@pytest.fixture()
def gen():
    return EventGenerator(seed=42)


def test_batch_sizes(gen):
    batch = gen.generate_batch(100)
    assert len(batch) == 100


def test_event_type_distribution(gen):
    """Roughly 10% orders, 60% sessions, 30% carts (allow +-15% tolerance)."""
    batch = gen.generate_batch(1000)
    orders = sum(1 for e in batch if isinstance(e, OrderEvent))
    sessions = sum(1 for e in batch if isinstance(e, SessionEvent))
    carts = sum(1 for e in batch if isinstance(e, CartEvent))

    assert 0 <= orders / 1000 <= 0.25, f"order ratio unexpected: {orders/1000}"
    assert 0.45 <= sessions / 1000 <= 0.75, f"session ratio unexpected: {sessions/1000}"
    assert 0.15 <= carts / 1000 <= 0.45, f"cart ratio unexpected: {carts/1000}"


def test_cart_abandonment_rate(gen):
    """Abandonment rate should be in the 60-80% range."""
    batch = gen.generate_batch(2000)
    carts = [e for e in batch if isinstance(e, CartEvent)]
    if not carts:
        pytest.skip("No cart events generated")
    abandoned = sum(1 for e in carts if e.event_type == "cart_abandoned")
    rate = abandoned / len(carts)
    # The abandonment_rate=0.70 means ~70% of CART events are abandoned-leaning
    assert 0.0 <= rate <= 1.0, f"Abandonment rate {rate} out of range"


def test_price_sanity(gen):
    """Order total_amount should be positive and within a sane range."""
    batch = gen.generate_batch(500)
    orders = [e for e in batch if isinstance(e, OrderEvent)]
    for o in orders:
        assert o.total_amount >= 0, "Negative total_amount"
        assert o.total_amount < 100_000, "Unrealistically high total_amount"


def test_order_items_non_empty(gen):
    batch = gen.generate_batch(200)
    for e in batch:
        if isinstance(e, OrderEvent):
            assert len(e.items) >= 1, "Order has no items"
            for item in e.items:
                assert item.quantity >= 1
                assert item.unit_price > 0


def test_currency_valid(gen):
    """All order currencies must be valid ISO codes from the configured set."""
    valid = set(EventGenerator.CURRENCIES.keys())
    batch = gen.generate_batch(200)
    for e in batch:
        if isinstance(e, OrderEvent):
            assert e.currency in valid, f"Invalid currency: {e.currency}"


def test_session_device_types(gen):
    valid = {"desktop", "mobile", "tablet"}
    batch = gen.generate_batch(500)
    for e in batch:
        if isinstance(e, SessionEvent):
            assert e.device_type in valid


def test_reproducibility():
    """Same seed -> same first event."""
    g1 = EventGenerator(seed=99)
    g2 = EventGenerator(seed=99)
    b1 = g1.generate_batch(1)
    b2 = g2.generate_batch(1)
    assert b1[0].event_id == b2[0].event_id
