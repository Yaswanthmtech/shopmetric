"""Synthetic e-commerce order stream with labelled fraud scenarios and data-quality chaos.

The same simulator feeds two consumers:
  * the live Kafka producer (real-time pacing), and
  * the model trainer (`simulate_history`, simulated clock),
so the patterns the model learns offline are exactly the patterns it meets in the stream.

Fraud scenarios (labelled, ~2-4% of orders):
  card_testing        rapid burst of tiny orders on one account (velocity signal)
  account_takeover    unusually large order from a new device / foreign IP
  synthetic_identity  brand-new account, high-value electronics, risky payment rails

Chaos (unlabelled data-quality faults, ~0.5% of messages) exercises the quarantine path:
  malformed JSON, missing fields, negative amounts, bad currency, future timestamps,
  unsupported schema version, type mismatches and producer-retry duplicates.

Hard cases are deliberate: legit travellers, gift orders, new phones and quick repeat
orders overlap with fraud signals, so the model cannot be perfect - just like reality.
"""

from __future__ import annotations

import heapq
import itertools
import json
import math
import random
import uuid
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from .contract import FX_TO_USD, OrderEvent

COUNTRIES = ["US", "IN", "GB", "DE", "CA", "FR", "AU", "BR", "NL", "SG"]
COUNTRY_WEIGHTS = [36, 16, 11, 8, 8, 6, 5, 4, 3, 3]
COUNTRY_CURRENCY = {"US": "USD", "IN": "INR", "GB": "GBP", "DE": "EUR", "FR": "EUR", "NL": "EUR", "CA": "CAD"}

CATEGORY_PRICE_USD: dict[str, tuple[float, float]] = {
    "electronics": (40, 900),
    "fashion": (15, 180),
    "home": (20, 350),
    "beauty": (8, 90),
    "grocery": (10, 120),
    "sports": (20, 300),
    "toys": (10, 120),
    "books": (8, 60),
    "gift_cards": (10, 200),
}
CATEGORIES = list(CATEGORY_PRICE_USD)
LEGIT_CATEGORY_WEIGHTS = [14, 20, 14, 10, 16, 8, 7, 7, 4]
PAYMENTS = ["credit_card", "debit_card", "paypal", "apple_pay", "bnpl", "gift_card", "crypto"]
LEGIT_PAYMENT_WEIGHTS = [42, 20, 14, 12, 7, 4, 1]
CHANNELS = ["web", "mobile_app", "marketplace"]
CHANNEL_WEIGHTS = [45, 40, 15]
DEVICE_TYPES = ["desktop", "ios", "android", "tablet"]

FRAUD_SCENARIOS = {"card_testing": 0.30, "account_takeover": 0.40, "synthetic_identity": 0.30}
# ~one order per customer every 17 min at peak: 5 orders/s <-> 5,000 active customers.
ORDERS_PER_CUSTOMER_PER_SEC = 0.001

CHAOS_KINDS = [
    "malformed_json",
    "missing_field",
    "negative_amount",
    "invalid_currency",
    "future_timestamp",
    "bad_schema_version",
    "type_mismatch",
    "duplicate",
]


@dataclass
class Device:
    device_id: str
    device_type: str
    first_seen: datetime


@dataclass
class Customer:
    customer_id: str
    home_country: str
    spend_scale: float
    account_created: datetime
    devices: list[Device]
    preferred_payment: str
    preferred_channel: str


@dataclass(order=True)
class Emission:
    """One Kafka message: `kind` is "order" for contract-valid events, else the chaos type."""

    ts: datetime
    seq: int
    key: str = field(compare=False)
    value: bytes = field(compare=False)
    kind: str = field(compare=False, default="order")
    label: str | None = field(compare=False, default=None)


class OrderSimulator:
    def __init__(
        self,
        seed: int | None = None,
        n_customers: int = 5000,
        fraud_rate: float = 0.01,
        chaos_rate: float = 0.006,
        start: datetime | None = None,
    ) -> None:
        self.rng = random.Random(seed)
        self.fraud_rate = fraud_rate
        self.chaos_rate = chaos_rate
        ref = start or datetime.now(UTC)
        self.customers = [self._new_customer(f"C{i:06d}", ref) for i in range(n_customers)]
        # Activity is skewed (some customers shop a lot more than others) but capped, so even
        # the keenest legit shopper averages only a couple of orders per 10 minutes.
        self._cum_weights = list(
            itertools.accumulate(min(self.rng.lognormvariate(0, 0.5), 4.0) for _ in self.customers)
        )
        self._seq = itertools.count()
        self._heap: list[Emission] = []
        self._recent: deque[Emission] = deque(maxlen=200)

    # ------------------------------------------------------------------ helpers
    def _uuid(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def _other_country(self, home: str) -> str:
        return self.rng.choice([c for c in COUNTRIES if c != home])

    def _new_device(self, ts: datetime) -> Device:
        return Device(f"D{self.rng.getrandbits(40):010x}", self.rng.choice(DEVICE_TYPES), ts)

    def _new_customer(self, customer_id: str, ref: datetime) -> Customer:
        rng = self.rng
        age_days = rng.uniform(0, 6) if rng.random() < 0.03 else min(rng.lognormvariate(5.3, 0.9), 3500)
        devices = [  # devices were first seen some time after the account was opened
            Device(
                f"D{rng.getrandbits(40):010x}",
                rng.choice(DEVICE_TYPES),
                ref - timedelta(days=rng.uniform(0, min(age_days, 900))),
            )
            for _ in range(rng.randint(1, 3))
        ]
        return Customer(
            customer_id=customer_id,
            home_country=rng.choices(COUNTRIES, COUNTRY_WEIGHTS)[0],
            spend_scale=min(max(rng.lognormvariate(0, 0.35), 0.4), 3.0),
            account_created=ref - timedelta(days=age_days),
            devices=devices,
            preferred_payment=rng.choices(PAYMENTS[:5], LEGIT_PAYMENT_WEIGHTS[:5])[0],
            preferred_channel=rng.choices(CHANNELS, CHANNEL_WEIGHTS)[0],
        )

    def _pick_customer(self) -> Customer:
        return self.rng.choices(self.customers, cum_weights=self._cum_weights)[0]

    def _price_usd(self, category: str) -> float:
        lo, hi = CATEGORY_PRICE_USD[category]
        return math.exp(self.rng.uniform(math.log(lo), math.log(hi)))

    def _emit(
        self,
        ts: datetime,
        c: Customer,
        device: Device,
        ip_country: str,
        shipping_country: str,
        payment: str,
        category: str,
        amount_usd: float,
        channel: str,
        label: str,
        discount: float | None = None,
        items: int | None = None,
    ) -> Emission:
        rng = self.rng
        currency = COUNTRY_CURRENCY.get(c.home_country, "USD")
        if discount is None:
            discount = 0.0 if rng.random() < 0.7 else rng.choice([0.05, 0.1, 0.15, 0.2, 0.3])
        if items is None:
            items = 1 + min(int(rng.expovariate(0.8)), 20)
        event = OrderEvent(
            event_id=self._uuid(),
            order_id=f"O{self.rng.getrandbits(48):012x}",
            customer_id=c.customer_id,
            event_ts=ts,
            produced_at_ms=int(ts.timestamp() * 1000),
            channel=channel,
            device_type=device.device_type,
            device_id=device.device_id,
            device_age_days=max((ts - device.first_seen).days, 0),
            account_age_days=max((ts - c.account_created).days, 0),
            billing_country=c.home_country,
            ip_country=ip_country,
            shipping_country=shipping_country,
            payment_method=payment,
            category=category,
            item_count=items,
            amount=round(max(amount_usd, 0.5) / FX_TO_USD[currency], 2),
            currency=currency,
            discount_pct=discount,
            simulated_label=label,
        )
        return Emission(ts, next(self._seq), c.customer_id, event.model_dump_json().encode(), "order", label)

    # ---------------------------------------------------------------- scenarios
    def legit(
        self, ts: datetime, customer: Customer | None = None, allow_followup: bool = True
    ) -> list[Emission]:
        rng = self.rng
        c = customer or self._pick_customer()
        if rng.random() < 0.08:  # a real customer on a brand-new phone/laptop
            device = self._new_device(ts)
            c.devices.append(device)
        else:
            device = rng.choice(c.devices)
        ip = c.home_country if rng.random() < 0.94 else self._other_country(c.home_country)  # travel
        ship = c.home_country if rng.random() < 0.92 else self._other_country(c.home_country)  # gifts
        category = rng.choices(CATEGORIES, LEGIT_CATEGORY_WEIGHTS)[0]
        payment = (
            c.preferred_payment if rng.random() < 0.6 else rng.choices(PAYMENTS, LEGIT_PAYMENT_WEIGHTS)[0]
        )
        channel = c.preferred_channel if rng.random() < 0.7 else rng.choices(CHANNELS, CHANNEL_WEIGHTS)[0]
        amount = self._price_usd(category) * c.spend_scale
        out = [self._emit(ts, c, device, ip, ship, payment, category, amount, channel, "legit")]
        if allow_followup and rng.random() < 0.05:  # legit "forgot something" repeat orders
            t = ts
            for _ in range(rng.randint(1, 3)):
                t += timedelta(seconds=rng.uniform(30, 300))
                cat = rng.choices(CATEGORIES, LEGIT_CATEGORY_WEIGHTS)[0]
                out.append(
                    self._emit(
                        t,
                        c,
                        device,
                        ip,
                        ship,
                        payment,
                        cat,
                        self._price_usd(cat) * c.spend_scale,
                        channel,
                        "legit",
                    )
                )
        return out

    def card_testing(self, ts: datetime) -> list[Emission]:
        rng = self.rng
        # Each order on its own looks like an ordinary small basket; the burst is the signal.
        victim = self._pick_customer()
        device = self._new_device(ts) if rng.random() < 0.6 else rng.choice(victim.devices)
        ip = self._other_country(victim.home_country) if rng.random() < 0.55 else victim.home_country
        ship = self._other_country(victim.home_country) if rng.random() < 0.4 else victim.home_country
        out, t = [], ts
        for _ in range(rng.randint(4, 9)):
            category = (
                "gift_cards" if rng.random() < 0.35 else rng.choice(["books", "beauty", "toys", "grocery"])
            )
            payment = "credit_card" if rng.random() < 0.8 else "debit_card"
            out.append(
                self._emit(
                    t,
                    victim,
                    device,
                    ip,
                    ship,
                    payment,
                    category,
                    rng.uniform(4, 40),
                    "web" if rng.random() < 0.7 else "mobile_app",
                    "card_testing",
                    discount=0.0,
                    items=rng.randint(1, 2),
                )
            )
            t += timedelta(seconds=rng.uniform(3, 45))
        return out

    def account_takeover(self, ts: datetime) -> list[Emission]:
        rng = self.rng
        victim = self._pick_customer()
        device = self._new_device(ts) if rng.random() < 0.9 else rng.choice(victim.devices)
        ip = self._other_country(victim.home_country) if rng.random() < 0.8 else victim.home_country
        ship = self._other_country(victim.home_country) if rng.random() < 0.7 else victim.home_country
        out, t = [], ts
        for _ in range(2 if rng.random() < 0.4 else 1):
            category = "electronics" if rng.random() < 0.65 else "gift_cards"
            amount = min(max(150 * victim.spend_scale * rng.uniform(3, 8), 150), 5000)
            out.append(
                self._emit(
                    t,
                    victim,
                    device,
                    ip,
                    ship,
                    victim.preferred_payment,
                    category,
                    amount,
                    rng.choice(["web", "mobile_app"]),
                    "account_takeover",
                    discount=0.0,
                    items=rng.randint(1, 3),
                )
            )
            t += timedelta(seconds=rng.uniform(60, 240))
        return out

    def synthetic_identity(self, ts: datetime) -> list[Emission]:
        rng = self.rng
        home = rng.choices(COUNTRIES, COUNTRY_WEIGHTS)[0]
        c = Customer(
            customer_id=f"N{rng.getrandbits(40):010x}",
            home_country=home,
            spend_scale=1.0,
            account_created=ts - timedelta(hours=rng.uniform(0, 48)),
            devices=[self._new_device(ts)],
            preferred_payment="credit_card",
            preferred_channel="web",
        )
        ip = home if rng.random() < 0.6 else self._other_country(home)
        ship = home if rng.random() < 0.5 else self._other_country(home)
        payment = rng.choices(["crypto", "gift_card", "credit_card", "bnpl"], [35, 25, 30, 10])[0]
        category = "electronics" if rng.random() < 0.8 else "gift_cards"
        return [
            self._emit(
                ts,
                c,
                c.devices[0],
                ip,
                ship,
                payment,
                category,
                rng.uniform(250, 2500),
                rng.choice(CHANNELS),
                "synthetic_identity",
                discount=0.1 if rng.random() < 0.3 else 0.0,
                items=rng.randint(1, 4),
            )
        ]

    def scenario(self, name: str, ts: datetime) -> list[Emission]:
        """Public entry point used by `shopmetric.sh attack` to inject a fraud scenario on demand."""
        return {
            "card_testing": self.card_testing,
            "account_takeover": self.account_takeover,
            "synthetic_identity": self.synthetic_identity,
        }[name](ts)

    def chaos(self, ts: datetime, kind: str | None = None) -> list[Emission]:
        kind = kind or self.rng.choice(CHAOS_KINDS)
        if kind == "duplicate":
            if not self._recent:
                return self.legit(ts, allow_followup=False)
            orig = self.rng.choice(list(self._recent))
            return [Emission(ts, next(self._seq), orig.key, orig.value, "duplicate", orig.label)]
        base = self.legit(ts, allow_followup=False)[0]
        payload = json.loads(base.value)
        if kind == "malformed_json":
            value = base.value[: len(base.value) // 2]
        else:
            if kind == "missing_field":
                payload.pop("customer_id")
            elif kind == "negative_amount":
                payload["amount"] = -abs(payload["amount"])
            elif kind == "invalid_currency":
                payload["currency"] = "XYZ"
            elif kind == "future_timestamp":
                payload["event_ts"] = (ts + timedelta(days=2)).isoformat()
            elif kind == "bad_schema_version":
                payload["schema_version"] = "0.9"
            elif kind == "type_mismatch":
                payload["amount"] = f"{payload['amount']}USD"
            value = json.dumps(payload).encode()
        return [Emission(ts, next(self._seq), base.key, value, kind, base.label)]

    def _spawn(self, ts: datetime) -> list[Emission]:
        r = self.rng.random()
        if r < self.fraud_rate:
            name = self.rng.choices(list(FRAUD_SCENARIOS), list(FRAUD_SCENARIOS.values()))[0]
            return self.scenario(name, ts)
        if r < self.fraud_rate + self.chaos_rate:
            return self.chaos(ts)
        return self.legit(ts)

    # --------------------------------------------------------------------- run
    def run(
        self,
        start: datetime,
        events_per_sec: float,
        max_events: int | None = None,
        end: datetime | None = None,
    ) -> Iterator[Emission]:
        """Yield emissions in event-time order on a simulated clock (Poisson arrivals)."""
        next_arrival = start
        emitted = 0
        while True:
            while not self._heap or self._heap[0].ts > next_arrival:
                for em in self._spawn(next_arrival):
                    heapq.heappush(self._heap, em)
                next_arrival += timedelta(seconds=self.rng.expovariate(events_per_sec))
            em = heapq.heappop(self._heap)
            if end is not None and em.ts > end:
                return
            if em.kind == "order":
                self._recent.append(em)
            yield em
            emitted += 1
            if max_events is not None and emitted >= max_events:
                return


def customers_for_rate(events_per_sec: float) -> int:
    """Grow the customer base with traffic so each customer's order rate stays realistic.

    More traffic means more shoppers, not the same shoppers ordering faster. Keeping orders per
    customer constant keeps the per-customer velocity distribution the same at 5/s and 500/s,
    so a model trained at one rate does not drown in false positives at another.
    """
    return max(1000, int(events_per_sec / ORDERS_PER_CUSTOMER_PER_SEC))


def simulate_history(
    n_orders: int,
    rate: float = 5.0,
    seed: int = 42,
    fraud_rate: float = 0.01,
    n_customers: int | None = None,
    end: datetime | None = None,
) -> list[dict]:
    """Contract-valid historical orders (no chaos) for offline training.

    `rate` should match the live producer (PRODUCER_RATE): velocity features depend on it.
    """
    end = end or datetime.now(UTC).replace(microsecond=0)
    start = end - timedelta(seconds=n_orders / rate)
    sim = OrderSimulator(
        seed=seed,
        n_customers=n_customers or customers_for_rate(rate),
        fraud_rate=fraud_rate,
        chaos_rate=0.0,
        start=start,
    )
    return [json.loads(em.value) for em in sim.run(start, rate, max_events=n_orders)]
