import json
import random

import numpy as np
import pandas as pd
import pytest

from shopmetric.features import (
    VELOCITY_AMOUNT,
    VELOCITY_COUNT,
    VelocityState,
    add_velocity_features_batch,
    make_velocity_fn,
)
from shopmetric.generator import simulate_history

WINDOW_MS = 600_000


def test_velocity_window_counts_and_boundaries():
    vs = VelocityState(WINDOW_MS)
    assert vs.observe("a", 0, 10.0, True) == (1.0, 10.0, False)
    assert vs.observe("b", 60_000, 5.0, True) == (2.0, 15.0, False)
    assert vs.observe("c", 600_000, 1.0, True) == (3.0, 16.0, False)  # t-window is inclusive
    assert vs.observe("d", 600_001, 1.0, True) == (3.0, 7.0, False)  # "a" fell out


def test_velocity_flags_duplicates_and_ignores_invalid():
    vs = VelocityState(WINDOW_MS)
    vs.observe("a", 1_000, 10.0, True)
    assert vs.observe("a", 1_000, 10.0, True) == (1.0, 10.0, True)
    assert vs.observe("bad", 2_000, 99.0, False) == (0.0, 0.0, False)
    assert vs.ids == ["a"]


def test_state_prunes_and_serialises():
    vs = VelocityState(WINDOW_MS)
    for i in range(5):
        vs.observe(f"o{i}", i * 300_000, 1.0, True)
    vs.prune()
    restored = VelocityState.from_json(vs.to_json(), WINDOW_MS)
    assert restored.ts == [600_000, 900_000, 1_200_000]


class FakeState:
    """Minimal stand-in for pyspark's GroupState."""

    hasTimedOut = False

    def __init__(self):
        self._value = None

    @property
    def exists(self):
        return self._value is not None

    @property
    def get(self):
        return self._value

    def update(self, value):
        self._value = value

    def remove(self):
        self._value = None

    def setTimeoutDuration(self, _ms):
        pass


@pytest.mark.spark
def test_streaming_and_batch_velocity_are_identical(spark):
    """Training/serving parity: the stateful stream path must reproduce the batch window exactly."""
    from pyspark.sql import functions as F

    from shopmetric.transforms import parse_orders

    rows = simulate_history(4000, rate=0.2, seed=9, n_customers=150)
    raw = spark.createDataFrame(pd.DataFrame({"raw_value": [json.dumps(r) for r in rows]}))
    src = raw.select(
        "raw_value",
        F.lit(None).cast("string").alias("kafka_key"),
        F.lit("t").alias("kafka_topic"),
        F.lit(0).alias("kafka_partition"),
        F.monotonically_increasing_id().alias("kafka_offset"),
        F.current_timestamp().alias("kafka_ts"),
        F.lit("order").alias("sim_kind"),
    )
    df = parse_orders(src).filter(F.col("dq_reason").isNull()).dropDuplicates(["order_id"]).cache()

    batch = (
        add_velocity_features_batch(df, WINDOW_MS)
        .select("order_id", VELOCITY_COUNT, VELOCITY_AMOUNT)
        .toPandas()
    )

    cols = ["order_id", "customer_id", "event_ts_ms", "kafka_offset", "amount_usd", "dq_reason"]
    arrivals = df.select(*cols).toPandas().sort_values("event_ts_ms").reset_index(drop=True)
    fn = make_velocity_fn(cols + [VELOCITY_COUNT, VELOCITY_AMOUNT, "is_duplicate"], WINDOW_MS, 60_000)

    rng = random.Random(0)
    states: dict[str, FakeState] = {}
    outputs, i = [], 0
    while i < len(arrivals):  # replay as random-sized micro-batches
        chunk = arrivals.iloc[i : i + rng.randint(1, 120)]
        i += len(chunk)
        for cust, group in chunk.groupby("customer_id"):
            outputs.extend(fn((cust,), iter([group.copy()]), states.setdefault(cust, FakeState())))
    stream = pd.concat(outputs)

    merged = batch.merge(stream, on="order_id", suffixes=("_batch", "_stream"))
    assert len(merged) == len(batch) == len(arrivals)
    assert (merged[f"{VELOCITY_COUNT}_batch"] == merged[f"{VELOCITY_COUNT}_stream"]).all()
    assert np.allclose(merged[f"{VELOCITY_AMOUNT}_batch"], merged[f"{VELOCITY_AMOUNT}_stream"], atol=0.02)
    assert merged[f"{VELOCITY_COUNT}_batch"].max() >= 4  # bursts are actually exercised
