import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from shopmetric.contract import SPARK_FIELDS, OrderEvent, export_json_schema


def _valid(**overrides):
    base = dict(
        event_id="e1",
        order_id="o1",
        customer_id="c1",
        event_ts=datetime(2026, 10, 8, tzinfo=UTC),
        produced_at_ms=1,
        channel="web",
        device_type="ios",
        device_id="d1",
        device_age_days=10,
        account_age_days=100,
        billing_country="US",
        ip_country="US",
        shipping_country="US",
        payment_method="credit_card",
        category="books",
        item_count=1,
        amount=12.5,
        currency="USD",
        discount_pct=0.0,
        simulated_label="legit",
    )
    base.update(overrides)
    return base


def test_spark_schema_matches_pydantic_contract():
    assert [name for name, _ in SPARK_FIELDS] == list(OrderEvent.model_fields)


def test_valid_event_round_trips_as_json():
    event = OrderEvent(**_valid())
    payload = json.loads(event.model_dump_json())
    assert payload["schema_version"] == "1.0"
    assert payload["event_ts"].endswith("Z")


@pytest.mark.parametrize(
    "overrides",
    [
        {"amount": -1},
        {"currency": "XYZ"},
        {"event_ts": datetime(2026, 10, 8)},  # naive timestamp
        {"item_count": 0},
        {"unexpected": "field"},
    ],
)
def test_contract_rejects_bad_events(overrides):
    with pytest.raises(ValidationError):
        OrderEvent(**_valid(**overrides))


def test_json_schema_export(tmp_path):
    path = export_json_schema(tmp_path / "order_event.json")
    schema = json.loads(path.read_text())
    assert "order_id" in schema["properties"] and schema["$id"].endswith("v1.0")
