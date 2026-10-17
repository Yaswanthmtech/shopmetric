"""Data-quality rules -> one reason code per record (first failing rule wins).

Records with a reason are never dropped silently: they are quarantined to the lake with the
raw payload, Kafka coordinates and the reason, so they can be inspected and replayed.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

from .contract import SCHEMA_VERSION, SUPPORTED_CURRENCIES

DQ_REASONS: dict[str, str] = {
    "MALFORMED_JSON": "Payload is not valid JSON",
    "UNSUPPORTED_SCHEMA_VERSION": f"schema_version missing or not {SCHEMA_VERSION}",
    "MISSING_OR_INVALID_FIELD": "Required field missing or of the wrong type",
    "NON_POSITIVE_AMOUNT": "amount <= 0",
    "UNSUPPORTED_CURRENCY": f"currency not in {SUPPORTED_CURRENCIES}",
    "FUTURE_EVENT_TIMESTAMP": "event_ts more than 10 minutes in the future",
    "OUT_OF_RANGE_VALUE": "item_count / discount_pct outside contract range",
}
# Routing reasons added after scoring (not data-quality failures, but still quarantined).
DUPLICATE_REASON = "DUPLICATE_ORDER"
FRAUD_REASON = "SUSPECTED_FRAUD"

REQUIRED_FIELDS = [
    "order_id",
    "customer_id",
    "event_ts",
    "amount",
    "currency",
    "item_count",
    "channel",
    "device_type",
    "billing_country",
    "ip_country",
    "shipping_country",
    "payment_method",
    "category",
    "device_age_days",
    "account_age_days",
    "discount_pct",
]
FUTURE_TOLERANCE = "INTERVAL 10 MINUTES"


def dq_reason_col() -> Column:
    """Expects the parsed columns plus `raw_value`; returns NULL when the record is clean."""
    any_required_null = F.lit(False)
    for name in REQUIRED_FIELDS:
        any_required_null = any_required_null | F.col(name).isNull()

    return (
        F.when(F.try_parse_json("raw_value").isNull(), F.lit("MALFORMED_JSON"))
        .when(
            F.coalesce(F.col("schema_version"), F.lit("")) != SCHEMA_VERSION,
            F.lit("UNSUPPORTED_SCHEMA_VERSION"),
        )
        .when(any_required_null, F.lit("MISSING_OR_INVALID_FIELD"))
        .when(F.col("amount") <= 0, F.lit("NON_POSITIVE_AMOUNT"))
        .when(~F.col("currency").isin(*SUPPORTED_CURRENCIES), F.lit("UNSUPPORTED_CURRENCY"))
        .when(
            F.col("event_ts") > F.current_timestamp() + F.expr(FUTURE_TOLERANCE),
            F.lit("FUTURE_EVENT_TIMESTAMP"),
        )
        .when(
            (F.col("item_count") < 1)
            | (F.col("item_count") > 100)
            | (F.col("discount_pct") < 0)
            | (F.col("discount_pct") > 0.9),
            F.lit("OUT_OF_RANGE_VALUE"),
        )
        .otherwise(F.lit(None).cast("string"))
    )
