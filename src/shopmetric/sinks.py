"""foreachBatch fan-out: one micro-batch -> S3 lake + Kafka alerts + warehouse + metrics.

Order of writes is chosen for latency: alerts -> clean facts -> lake quarantine -> metrics.

Routing per record:
    data-quality failure  -> quarantine (reason = rule code)
    duplicate order       -> quarantine (DUPLICATE_ORDER)
    fraud_score >= thr    -> quarantine (SUSPECTED_FRAUD) + real-time alert on Kafka
    everything else       -> clean -> warehouse fact_orders

Cost design: full raw payloads (bronze + quarantine) go to cheap object storage as zstd parquet;
only clean, analytics-ready rows plus small metadata tables reach the compute-billed warehouse.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import Settings
from .contract import FRAUD_LABELS
from .model import ModelProvider
from .quality import DUPLICATE_REASON, FRAUD_REASON
from .warehouse import Warehouse

log = logging.getLogger("shopmetric.sinks")

ALERT_FIELDS = [
    "order_id",
    "customer_id",
    "event_ts",
    "amount_usd",
    "category",
    "payment_method",
    "billing_country",
    "ip_country",
    "cust_txn_count_10m",
    "fraud_score",
    "model_version",
]


class LakeWriter:
    """Writes quarantined records (raw payload + reason) to the lake, idempotently per batch."""

    def __init__(self, s: Settings) -> None:
        self.base = s.lake_path("quarantine", "orders")

    def write_quarantine(self, sdf: DataFrame, batch_id: int, dt: str) -> str:
        # One directory per micro-batch, written with overwrite: a replayed batch replaces its own
        # files instead of duplicating them.
        path = f"{self.base}/dt={dt}/batch_id={batch_id:010d}"
        sdf.coalesce(1).write.mode("overwrite").partitionBy("quarantine_reason").parquet(path)
        return path


class KafkaAlertWriter:
    def __init__(self, s: Settings) -> None:
        self.bootstrap = s.kafka_bootstrap
        self.topic = s.topic_alerts

    def publish(self, sdf: DataFrame) -> None:
        payload = sdf.select(
            F.col("order_id").alias("key"),
            F.to_json(
                F.struct(*[F.col(c) for c in ALERT_FIELDS], F.current_timestamp().alias("alerted_at"))
            ).alias("value"),
        )
        payload.write.format("kafka").option("kafka.bootstrap.servers", self.bootstrap).option(
            "topic", self.topic
        ).save()


def _epoch_ms(series: pd.Series) -> pd.Series:
    ts = pd.to_datetime(series, utc=True)
    return (ts - pd.Timestamp(0, tz="UTC")) / pd.Timedelta(milliseconds=1)


def _pct(values: pd.Series | np.ndarray, q: float) -> float | None:
    arr = np.asarray(values, dtype="float64")
    arr = arr[~np.isnan(arr)]
    return round(float(np.percentile(arr, q)), 1) if arr.size else None


class BatchProcessor:
    def __init__(
        self,
        warehouse: Warehouse,
        models: ModelProvider,
        lake: LakeWriter,
        alerts: KafkaAlertWriter | None = None,
    ) -> None:
        self.wh = warehouse
        self.models = models
        self.lake = lake
        self.alerts = alerts

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        t0 = time.time()
        model = self.models.get()
        df = batch_df.persist()
        routed = None
        try:
            is_valid = F.col("dq_reason").isNull() & ~F.col("is_duplicate")
            scored = model.score(df.filter(is_valid)).withColumn(
                "quarantine_reason",
                F.when(F.col("fraud_score") >= F.lit(model.threshold), F.lit(FRAUD_REASON)).otherwise(
                    F.lit(None).cast("string")
                ),
            )
            others = (
                df.filter(~is_valid)
                .withColumn("fraud_score", F.lit(None).cast("double"))
                .withColumn("quarantine_reason", F.coalesce(F.col("dq_reason"), F.lit(DUPLICATE_REASON)))
            )
            routed = (
                scored.unionByName(others)
                .withColumn("model_version", F.lit(model.version))
                .withColumn("batch_id", F.lit(batch_id).cast("long"))
                .persist()
            )
            pdf = routed.toPandas()
            if pdf.empty:  # no-data batches still run (state timeouts); nothing to write
                return
            t_scored = time.time()
            now = datetime.now(UTC)
            quarantined = pdf["quarantine_reason"].notna()
            is_fraud = pdf["quarantine_reason"].eq(FRAUD_REASON)

            # 1) Real-time alerts first: fraud response is the most latency-sensitive output.
            if self.alerts is not None and is_fraud.any():
                self.alerts.publish(routed.filter(F.col("quarantine_reason") == FRAUD_REASON))
            t_alerts = time.time()

            # 2) Warehouse: clean facts next (that is the latency the business feels).
            clean = pdf.loc[~quarantined].assign(processed_at=now)
            self.wh.upsert("fact_orders", clean)
            t_commit = time.time()

            # 3) Lake: full raw payload of everything we did not trust, then its warehouse index.
            lake_path = None
            if quarantined.any():
                lake_path = self.lake.write_quarantine(
                    routed.filter(F.col("quarantine_reason").isNotNull()), batch_id, now.strftime("%Y-%m-%d")
                )
            t_lake = time.time()
            qlog = pdf.loc[quarantined].assign(quarantined_at=now, lake_path=lake_path)
            self.wh.upsert("quarantine_log", qlog)

            # 4) Metrics: throughput, latency, data-quality catch rate, live model quality.
            produced_ms = _epoch_ms(pdf["kafka_ts"])
            chaos = ~pdf["sim_kind"].isin(["order", "unknown"])
            scored_mask = pdf["dq_reason"].isna() & ~pdf["is_duplicate"].astype(bool)
            actual = pdf["simulated_label"].isin(FRAUD_LABELS)
            metrics = pd.DataFrame(
                [
                    {
                        "batch_id": batch_id,
                        "batch_ts": now,
                        "model_version": model.version,
                        "input_rows": len(pdf),
                        "clean_rows": int((~quarantined).sum()),
                        "invalid_rows": int(pdf["dq_reason"].notna().sum()),
                        "duplicate_rows": int(pdf["quarantine_reason"].eq(DUPLICATE_REASON).sum()),
                        "fraud_rows": int(is_fraud.sum()),
                        "chaos_injected": int(chaos.sum()),
                        "chaos_caught": int((chaos & quarantined).sum()),
                        "tp": int((scored_mask & is_fraud & actual).sum()),
                        "fp": int((scored_mask & is_fraud & ~actual).sum()),
                        "fn": int((scored_mask & ~is_fraud & actual).sum()),
                        "tn": int((scored_mask & ~is_fraud & ~actual).sum()),
                        "ingest_p50_ms": _pct(t_scored * 1000 - produced_ms, 50),
                        "ingest_p95_ms": _pct(t_scored * 1000 - produced_ms, 95),
                        "e2e_p50_ms": _pct(t_commit * 1000 - produced_ms[~quarantined], 50),
                        "e2e_p95_ms": _pct(t_commit * 1000 - produced_ms[~quarantined], 95),
                        "batch_duration_ms": round((time.time() - t0) * 1000, 1),
                    }
                ]
            )
            self.wh.upsert("pipeline_metrics", metrics)
            m = metrics.iloc[0]
            log.info(
                "batch=%d rows=%d clean=%d quarantined=%d (fraud=%d dup=%d dq=%d) e2e_p95=%sms took=%.0fms "
                "[compute=%.0f alerts=%.0f warehouse=%.0f lake=%.0f] model=%s",
                batch_id,
                m.input_rows,
                m.clean_rows,
                int(quarantined.sum()),
                m.fraud_rows,
                m.duplicate_rows,
                m.invalid_rows,
                m.e2e_p95_ms,
                m.batch_duration_ms,
                (t_scored - t0) * 1000,
                (t_alerts - t_scored) * 1000,
                (t_commit - t_alerts) * 1000,
                (t_lake - t_commit) * 1000,
                model.version,
            )
        finally:
            if routed is not None:
                routed.unpersist()
            df.unpersist()
