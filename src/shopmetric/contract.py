"""Data contract for the `orders.raw` Kafka topic (schema v1.0).

One source of truth, three enforcement points:
  1. Producer  - every legitimate event is built through `OrderEvent` (Pydantic) before it is sent.
  2. Spark     - `spark_schema()` mirrors the contract; anything that does not fit is quarantined
                 with a reason code instead of silently becoming NULLs downstream.
  3. Docs/CI   - `python -m shopmetric.contract` exports JSON Schema to contracts/ and a unit test
                 fails the build if the Pydantic model and the Spark schema drift apart.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

SCHEMA_VERSION = "1.0"

Currency = Literal["USD", "EUR", "GBP", "INR", "CAD"]
Channel = Literal["web", "mobile_app", "marketplace"]
DeviceType = Literal["desktop", "ios", "android", "tablet"]
PaymentMethod = Literal["credit_card", "debit_card", "paypal", "apple_pay", "bnpl", "gift_card", "crypto"]
Category = Literal[
    "electronics", "fashion", "home", "beauty", "grocery", "sports", "toys", "books", "gift_cards"
]
# Ground truth exists only because the data is simulated. It is used for model *evaluation*
# and live monitoring, and is never fed to the model as a feature.
SimulatedLabel = Literal["legit", "card_testing", "account_takeover", "synthetic_identity"]

SUPPORTED_CURRENCIES: tuple[str, ...] = Currency.__args__  # type: ignore[attr-defined]
FRAUD_LABELS = ("card_testing", "account_takeover", "synthetic_identity")

# Fixed demo FX rates. A production system would join a daily rates table instead.
FX_TO_USD: dict[str, float] = {"USD": 1.0, "EUR": 1.08, "GBP": 1.27, "INR": 0.012, "CAD": 0.73}


class OrderEvent(BaseModel):
    """One checkout event, as published by the order service."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["1.0"] = SCHEMA_VERSION
    event_id: str
    order_id: str
    customer_id: str
    event_ts: datetime
    produced_at_ms: int
    channel: Channel
    device_type: DeviceType
    device_id: str
    device_age_days: int = Field(ge=0)
    account_age_days: int = Field(ge=0)
    billing_country: str = Field(min_length=2, max_length=2)
    ip_country: str = Field(min_length=2, max_length=2)
    shipping_country: str = Field(min_length=2, max_length=2)
    payment_method: PaymentMethod
    category: Category
    item_count: int = Field(ge=1, le=100)
    amount: float = Field(gt=0)
    currency: Currency
    discount_pct: float = Field(ge=0, le=0.9)
    simulated_label: SimulatedLabel | None = None

    @field_validator("event_ts")
    @classmethod
    def _must_be_tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("event_ts must be timezone-aware (UTC)")
        return v


# (field name, Spark SQL type). event_ts arrives as an ISO-8601 string and is parsed
# explicitly so a bad timestamp is caught by data-quality rules rather than by the reader.
SPARK_FIELDS: list[tuple[str, str]] = [
    ("schema_version", "string"),
    ("event_id", "string"),
    ("order_id", "string"),
    ("customer_id", "string"),
    ("event_ts", "string"),
    ("produced_at_ms", "long"),
    ("channel", "string"),
    ("device_type", "string"),
    ("device_id", "string"),
    ("device_age_days", "int"),
    ("account_age_days", "int"),
    ("billing_country", "string"),
    ("ip_country", "string"),
    ("shipping_country", "string"),
    ("payment_method", "string"),
    ("category", "string"),
    ("item_count", "int"),
    ("amount", "double"),
    ("currency", "string"),
    ("discount_pct", "double"),
    ("simulated_label", "string"),
]


def spark_schema():
    """Spark StructType mirroring `OrderEvent` (imported lazily: the producer image has no Spark)."""
    from pyspark.sql import types as T

    mapping = {
        "string": T.StringType(),
        "long": T.LongType(),
        "int": T.IntegerType(),
        "double": T.DoubleType(),
    }
    return T.StructType([T.StructField(name, mapping[kind], True) for name, kind in SPARK_FIELDS])


def export_json_schema(path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = OrderEvent.model_json_schema()
    schema["$id"] = f"shopmetric/orders.raw/v{SCHEMA_VERSION}"
    path.write_text(json.dumps(schema, indent=2) + "\n")
    return path


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else f"contracts/order_event.v{SCHEMA_VERSION}.json"
    print(f"wrote {export_json_schema(out)}")
