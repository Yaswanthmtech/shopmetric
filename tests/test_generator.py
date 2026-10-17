import json
from collections import Counter
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from shopmetric.contract import OrderEvent
from shopmetric.generator import CHAOS_KINDS, OrderSimulator, simulate_history

START = datetime(2026, 10, 1, tzinfo=UTC)


def test_same_seed_same_stream():
    a = [e.value for e in OrderSimulator(seed=7, start=START).run(START, 10, max_events=200)]
    b = [e.value for e in OrderSimulator(seed=7, start=START).run(START, 10, max_events=200)]
    assert a == b


def test_stream_is_time_ordered_and_keyed_by_customer():
    events = list(OrderSimulator(seed=1, start=START).run(START, 20, max_events=2000))
    assert [e.ts for e in events] == sorted(e.ts for e in events)
    for e in events:
        if e.kind == "order":
            assert json.loads(e.value)["customer_id"] == e.key


def test_valid_orders_satisfy_contract_and_fraud_is_present():
    rows = simulate_history(20_000, rate=5, seed=3)
    for r in rows[:500]:
        OrderEvent.model_validate(r)
    labels = Counter(r["simulated_label"] for r in rows)
    assert set(labels) == {"legit", "card_testing", "account_takeover", "synthetic_identity"}
    assert 0.005 < 1 - labels["legit"] / len(rows) < 0.10


@pytest.mark.parametrize("kind", [k for k in CHAOS_KINDS if k != "duplicate"])
def test_chaos_records_break_the_contract(kind):
    sim = OrderSimulator(seed=11, start=START)
    em = sim.chaos(START, kind)[0]
    assert em.kind == kind
    if kind == "malformed_json":
        with pytest.raises(json.JSONDecodeError):
            json.loads(em.value)
        return
    payload = json.loads(em.value)
    if kind == "future_timestamp":  # valid shape, invalid business rule (checked in Spark)
        assert payload["event_ts"] > START.isoformat()
    else:
        with pytest.raises(ValidationError):
            OrderEvent.model_validate(payload)


def test_duplicate_chaos_resends_identical_bytes():
    sim = OrderSimulator(seed=5, start=START)
    sent = list(sim.run(START, 10, max_events=50))
    dup = sim.chaos(START, "duplicate")[0]
    assert dup.kind == "duplicate"
    assert dup.value in {e.value for e in sent}
