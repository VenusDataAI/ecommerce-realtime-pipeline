"""
Generates realistic synthetic e-commerce events using Faker.
Supports configurable events_per_second and event mix ratios.
"""
from __future__ import annotations

import math
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

import structlog
from faker import Faker

from producer.schemas import CartEvent, OrderEvent, OrderItem, SessionEvent

logger = structlog.get_logger(__name__)


class EventGenerator:
    """
    Generates realistic synthetic e-commerce events.

    Parameters
    ----------
    events_per_second : float
        Target event throughput (used in streaming mode).
    order_ratio : float
        Fraction of events that are OrderEvents (default 0.10).
    session_ratio : float
        Fraction of events that are SessionEvents (default 0.60).
    cart_ratio : float
        Fraction of events that are CartEvents (default 0.30).
    abandonment_rate : float
        Probability that a cart is abandoned (~0.70).
    seed : int | None
        Random seed for reproducibility.
    """

    # ISO 4217 currencies and approximate USD exchange rates
    CURRENCIES = {
        "USD": 1.00,
        "EUR": 1.08,
        "GBP": 1.26,
        "BRL": 0.19,
        "CAD": 0.74,
        "AUD": 0.65,
        "JPY": 0.0066,
        "MXN": 0.057,
    }

    PAYMENT_METHODS = [
        "credit_card", "debit_card", "paypal", "apple_pay",
        "google_pay", "bank_transfer", "crypto",
    ]
    DEVICE_TYPES = ["desktop", "mobile", "tablet"]
    COUNTRY_CODES = ["US", "GB", "DE", "FR", "BR", "CA", "AU", "JP", "MX", "IN"]
    PAGE_URLS = [
        "/", "/products", "/products/{id}", "/cart", "/checkout",
        "/account", "/orders", "/search", "/about", "/contact",
    ]
    REFERRERS = [
        "https://google.com", "https://facebook.com", "https://instagram.com",
        "https://twitter.com", "", "https://bing.com", "direct",
    ]

    def __init__(
        self,
        events_per_second: float = 10.0,
        order_ratio: float = 0.10,
        session_ratio: float = 0.60,
        cart_ratio: float = 0.30,
        abandonment_rate: float = 0.70,
        seed: Optional[int] = None,
    ) -> None:
        assert abs(order_ratio + session_ratio + cart_ratio - 1.0) < 1e-6, \
            "Ratios must sum to 1.0"
        self.events_per_second = events_per_second
        self.order_ratio = order_ratio
        self.session_ratio = session_ratio
        self.cart_ratio = cart_ratio
        self.abandonment_rate = abandonment_rate

        # Per-instance RNG and Faker so multiple instances with the same seed
        # are fully independent and reproducible.
        self._rng = random.Random(seed)
        self._fake = Faker()
        if seed is not None:
            self._fake.seed_instance(seed)

        # Track open carts to simulate abandonment
        self._open_carts: dict[str, dict] = {}
        # Product catalog (product_id -> price)
        self._products = self._build_product_catalog()
        # Active sessions
        self._active_sessions: dict[str, dict] = {}

        logger.info(
            "EventGenerator initialised",
            eps=events_per_second,
            order_ratio=order_ratio,
            abandonment_rate=abandonment_rate,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_batch(self, n: int) -> list[OrderEvent | SessionEvent | CartEvent]:
        """Return a list of n mixed events."""
        events: list[OrderEvent | SessionEvent | CartEvent] = []
        for _ in range(n):
            r = self._rng.random()
            if r < self.order_ratio:
                events.append(self._make_order_event())
            elif r < self.order_ratio + self.session_ratio:
                events.append(self._make_session_event())
            else:
                events.append(self._make_cart_event())
        return events

    def stream(self) -> Iterator[OrderEvent | SessionEvent | CartEvent]:
        """Infinite iterator respecting events_per_second."""
        interval = 1.0 / self.events_per_second
        while True:
            r = self._rng.random()
            if r < self.order_ratio:
                yield self._make_order_event()
            elif r < self.order_ratio + self.session_ratio:
                yield self._make_session_event()
            else:
                yield self._make_cart_event()
            time.sleep(interval)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_product_catalog(self) -> dict[str, float]:
        """Build a small synthetic product catalog with log-normal prices."""
        catalog: dict[str, float] = {}
        for i in range(1, 101):
            pid = f"PROD-{i:04d}"
            # Log-normal price: median ~$29.99, range roughly $1–$500
            price = round(math.exp(self._rng.gauss(3.4, 1.0)), 2)
            price = max(0.99, min(price, 999.99))
            catalog[pid] = price
        return catalog

    def _random_timestamp(self, within_seconds: int = 300) -> str:
        """ISO 8601 timestamp slightly in the past."""
        offset = self._rng.uniform(0, within_seconds)
        ts = datetime.now(timezone.utc).timestamp() - offset
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

    def _make_order_event(self) -> OrderEvent:
        user_id = str(self._fake.uuid4())
        session_id = str(self._fake.uuid4())
        currency = self._rng.choice(list(self.CURRENCIES.keys()))
        country = self._rng.choice(self.COUNTRY_CODES)

        # 1–5 items per order
        n_items = self._rng.randint(1, 5)
        items: list[OrderItem] = []
        for _ in range(n_items):
            pid = self._rng.choice(list(self._products.keys()))
            qty = self._rng.randint(1, 4)
            price = self._products[pid]
            items.append(OrderItem(product_id=pid, quantity=qty, unit_price=price))

        total = round(sum(i.quantity * i.unit_price for i in items), 2)
        discount = round(total * self._rng.choice([0.0, 0.0, 0.0, 0.05, 0.10, 0.15, 0.20]), 2)
        total_after_discount = round(total - discount, 2)

        event_type = self._rng.choices(
            ["order_placed", "order_cancelled", "order_refunded"],
            weights=[0.80, 0.12, 0.08],
        )[0]

        return OrderEvent(
            event_id=str(self._fake.uuid4()),
            event_type=event_type,
            order_id=str(self._fake.uuid4()),
            user_id=user_id,
            session_id=session_id,
            items=items,
            currency=currency,
            total_amount=total_after_discount,
            discount_amount=discount,
            payment_method=self._rng.choice(self.PAYMENT_METHODS),
            country_code=country,
            timestamp=self._random_timestamp(),
        )

    def _make_session_event(self) -> SessionEvent:
        session_id = str(self._fake.uuid4())
        user_id = str(self._fake.uuid4())
        device = self._rng.choice(self.DEVICE_TYPES)
        country = self._rng.choice(self.COUNTRY_CODES)

        event_type = self._rng.choices(
            ["session_start", "page_view", "session_end"],
            weights=[0.20, 0.65, 0.15],
        )[0]

        page = self._rng.choice(self.PAGE_URLS).replace("{id}", str(self._rng.randint(1, 100)))
        referrer = self._rng.choice(self.REFERRERS)

        return SessionEvent(
            event_id=str(self._fake.uuid4()),
            event_type=event_type,
            session_id=session_id,
            user_id=user_id,
            page_url=page,
            referrer=referrer,
            device_type=device,
            country_code=country,
            timestamp=self._random_timestamp(),
        )

    def _make_cart_event(self) -> CartEvent:
        cart_id = str(self._fake.uuid4())
        session_id = str(self._fake.uuid4())
        user_id = str(self._fake.uuid4())
        pid = self._rng.choice(list(self._products.keys()))
        price = self._products[pid]

        # Decide event type; ~70% of carts are abandoned
        abandon = self._rng.random() < self.abandonment_rate
        if abandon:
            event_type = self._rng.choices(
                ["add_to_cart", "cart_abandoned"],
                weights=[0.55, 0.45],
            )[0]
        else:
            event_type = "remove_from_cart" if self._rng.random() < 0.15 else "add_to_cart"

        return CartEvent(
            event_id=str(self._fake.uuid4()),
            event_type=event_type,
            cart_id=cart_id,
            session_id=session_id,
            user_id=user_id,
            product_id=pid,
            quantity=self._rng.randint(1, 3),
            unit_price=price,
            timestamp=self._random_timestamp(),
        )

    # ------------------------------------------------------------------
    # Sample data writer
    # ------------------------------------------------------------------

    def write_sample_data(
        self,
        output_dir: Path,
        n_orders: int = 1000,
        n_sessions: int = 5000,
        n_carts: int = 3000,
        seed: int = 42,
    ) -> None:
        """Write reproducible sample JSONL files."""
        self._rng.seed(seed)
        self._fake.seed_instance(seed)
        output_dir.mkdir(parents=True, exist_ok=True)

        def _write(path: Path, events: list) -> None:
            with path.open("w", encoding="utf-8") as f:
                for ev in events:
                    f.write(ev.model_dump_json() + "\n")
            logger.info("Sample data written", path=str(path), count=len(events))

        orders = [self._make_order_event() for _ in range(n_orders)]
        sessions = [self._make_session_event() for _ in range(n_sessions)]
        carts = [self._make_cart_event() for _ in range(n_carts)]

        _write(output_dir / "order_events.jsonl", orders)
        _write(output_dir / "session_events.jsonl", sessions)
        _write(output_dir / "cart_events.jsonl", carts)
