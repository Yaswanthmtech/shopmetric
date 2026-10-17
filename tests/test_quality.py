from datetime import UTC, datetime

import pandas as pd
import pytest

from shopmetric.generator import OrderSimulator

EXPECTED = {
    "malformed_json": "MALFORMED_JSON",
    "missing_field": "MISSING_OR_INVALID_FIELD",
    "negative_amount": "NON_POSITIVE_AMOUNT",
    "invalid_currency": "UNSUPPORTED_CURRENCY",
    "future_timestamp": "FUTURE_EVENT_TIMESTAMP",
    "bad_schema_version": "UNSUPPORTED_SCHEMA_VERSION",
    "type_mismatch": "MISSING_OR_INVALID_FIELD",
}


@pytest.mark.spark
def test_every_chaos_kind_gets_the_right_reason_and_valid_rows_pass(spark):
    from pyspark.sql import functions as F

    from shopmetric.transforms import parse_orders

    now = datetime.now(UTC)
    sim = OrderSimulator(seed=21, start=now)
    rows = [(em.value.decode(), em.kind) for kind in EXPECTED for em in sim.chaos(now, kind)]
    rows += [(em.value.decode(), "order") for em in sim.legit(now, allow_followup=False)]
    src = spark.createDataFrame(pd.DataFrame(rows, columns=["raw_value", "sim_kind"])).select(
        "raw_value",
        F.lit("k").alias("kafka_key"),
        F.lit("t").alias("kafka_topic"),
        F.lit(0).alias("kafka_partition"),
        F.monotonically_increasing_id().alias("kafka_offset"),
        F.current_timestamp().alias("kafka_ts"),
        "sim_kind",
    )
    got = {r.sim_kind: r.dq_reason for r in parse_orders(src).select("sim_kind", "dq_reason").collect()}
    assert got.pop("order") is None
    assert got == EXPECTED
