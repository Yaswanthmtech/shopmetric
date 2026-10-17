"""Feature engineering: one feature definition, two execution paths.

  * Training (batch):  per-customer velocity via a range window over event time.
  * Serving (stream):  the same numbers computed incrementally with `applyInPandasWithState`,
                       keeping a small per-customer state (RocksDB-backed) that expires when idle.

`tests/test_features.py` asserts both paths produce identical values (training/serving parity),
which is the classic source of silent model degradation in real-time ML systems.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass, field

import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming.state import GroupStateTimeout

from .contract import FX_TO_USD

CATEGORICAL_FEATURES = ["payment_method", "category", "channel", "device_type"]
NUMERIC_FEATURES = [
    "log_amount_usd",
    "item_count",
    "discount_pct",
    "account_age_days",
    "device_age_days",
    "is_new_device",
    "is_new_account",
    "ip_billing_mismatch",
    "ship_billing_mismatch",
    "cust_txn_count_10m",
    "cust_amount_usd_10m",
]
VELOCITY_COUNT = "cust_txn_count_10m"
VELOCITY_AMOUNT = "cust_amount_usd_10m"


def add_stateless_features(df: DataFrame) -> DataFrame:
    fx = F.create_map(*[F.lit(x) for pair in FX_TO_USD.items() for x in pair])
    return (
        df.withColumn("amount_usd", F.round(F.col("amount") * fx[F.col("currency")], 2))
        .withColumn("log_amount_usd", F.log1p(F.greatest(F.col("amount_usd"), F.lit(0.0))))
        .withColumn("is_new_device", (F.col("device_age_days") <= 1).cast("double"))
        .withColumn("is_new_account", (F.col("account_age_days") < 7).cast("double"))
        .withColumn("ip_billing_mismatch", (F.col("ip_country") != F.col("billing_country")).cast("double"))
        .withColumn(
            "ship_billing_mismatch", (F.col("shipping_country") != F.col("billing_country")).cast("double")
        )
        .withColumn("event_ts_ms", F.unix_millis("event_ts"))
    )


# --------------------------------------------------------------------------- batch path
def add_velocity_features_batch(df: DataFrame, window_ms: int) -> DataFrame:
    """Count and USD sum of the customer's orders in [event_ts - window, event_ts]."""
    w = Window.partitionBy("customer_id").orderBy("event_ts_ms").rangeBetween(-window_ms, 0)
    return df.withColumn(VELOCITY_COUNT, F.count(F.lit(1)).over(w).cast("double")).withColumn(
        VELOCITY_AMOUNT, F.round(F.sum("amount_usd").over(w), 2)
    )


# ------------------------------------------------------------------------ stream path
@dataclass
class VelocityState:
    """Per-customer sliding window of recent orders (also used for in-stream de-duplication)."""

    window_ms: int
    ts: list[int] = field(default_factory=list)
    amt: list[float] = field(default_factory=list)
    ids: list[str] = field(default_factory=list)

    @classmethod
    def from_json(cls, payload: str | None, window_ms: int) -> VelocityState:
        if not payload:
            return cls(window_ms)
        d = json.loads(payload)
        return cls(window_ms, d["ts"], d["amt"], d["ids"])

    def to_json(self) -> str:
        return json.dumps({"ts": self.ts, "amt": self.amt, "ids": self.ids}, separators=(",", ":"))

    def observe(
        self, order_id: str, ts_ms: int | None, amount_usd: float, is_valid: bool
    ) -> tuple[float, float, bool]:
        """Returns (count, usd_sum, is_duplicate) for this event. Invalid events are not counted."""
        if not is_valid or ts_ms is None:
            return 0.0, 0.0, False
        is_dup = order_id in self.ids
        if not is_dup:
            self.ts.append(ts_ms)
            self.amt.append(amount_usd)
            self.ids.append(order_id)
        lo = ts_ms - self.window_ms
        count, total = 0, 0.0
        for t, a in zip(self.ts, self.amt, strict=True):
            if lo <= t <= ts_ms:
                count += 1
                total += a
        return float(count), round(total, 2), is_dup

    def prune(self) -> None:
        if not self.ts:
            return
        horizon = max(self.ts) - self.window_ms
        keep = [i for i, t in enumerate(self.ts) if t >= horizon]
        self.ts = [self.ts[i] for i in keep]
        self.amt = [self.amt[i] for i in keep]
        self.ids = [self.ids[i] for i in keep]


def pdf_cols(frames: list[pd.DataFrame]) -> set[str]:
    return set(frames[0].columns) if frames else set()


def _isna(v) -> bool:
    return v is None or (isinstance(v, float) and v != v) or v is pd.NaT


def make_velocity_fn(output_columns: list[str], window_ms: int, idle_timeout_ms: int):
    def velocity(key: tuple, pdfs: Iterator[pd.DataFrame], state) -> Iterator[pd.DataFrame]:
        if state.hasTimedOut:  # customer idle for too long -> free the state
            state.remove()
            return
        frames = [p for p in pdfs if len(p)]
        if not frames:
            return
        # Event-time order; ties (e.g. a producer retry re-sending identical bytes) are broken by
        # Kafka offset, so the later copy is the one flagged as the duplicate.
        sort_cols = ["event_ts_ms"] + (["kafka_offset"] if "kafka_offset" in pdf_cols(frames) else [])
        pdf = pd.concat(frames, ignore_index=True).sort_values(sort_cols, kind="stable", na_position="last")
        customer_id = key[0]
        if customer_id is None:  # malformed records: pass through untouched, no state
            pdf[VELOCITY_COUNT], pdf[VELOCITY_AMOUNT], pdf["is_duplicate"] = 0.0, 0.0, False
            yield pdf[output_columns]
            return

        vs = VelocityState.from_json(state.get[0] if state.exists else None, window_ms)
        counts, sums, dups = [], [], []
        for oid, ts, amt, reason in zip(
            pdf["order_id"], pdf["event_ts_ms"], pdf["amount_usd"], pdf["dq_reason"], strict=True
        ):
            c, s, d = vs.observe(
                oid,
                None if _isna(ts) else int(ts),
                0.0 if _isna(amt) else float(amt),
                is_valid=_isna(reason),
            )
            counts.append(c)
            sums.append(s)
            dups.append(d)
        vs.prune()
        state.update((vs.to_json(),))
        state.setTimeoutDuration(idle_timeout_ms)
        pdf[VELOCITY_COUNT], pdf[VELOCITY_AMOUNT], pdf["is_duplicate"] = counts, sums, dups
        yield pdf[output_columns]

    return velocity


def add_velocity_features_stream(df: DataFrame, window_ms: int, idle_timeout_ms: int) -> DataFrame:
    out_schema = T.StructType(
        list(df.schema.fields)
        + [
            T.StructField(VELOCITY_COUNT, T.DoubleType()),
            T.StructField(VELOCITY_AMOUNT, T.DoubleType()),
            T.StructField("is_duplicate", T.BooleanType()),
        ]
    )
    # Note: array-typed state is not supported by the pandas state serializer, so the window is
    # stored as one compact JSON string per customer.
    state_schema = T.StructType([T.StructField("payload", T.StringType())])
    fn = make_velocity_fn([f.name for f in out_schema.fields], window_ms, idle_timeout_ms)
    return df.groupBy("customer_id").applyInPandasWithState(
        fn, out_schema, state_schema, "append", GroupStateTimeout.ProcessingTimeTimeout
    )
