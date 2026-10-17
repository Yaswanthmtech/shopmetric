"""End-to-end test of the real streaming graph, with a file source standing in for Kafka.

Everything after the source is production code: parsing, data-quality rules, stateful velocity +
dedup, scoring, lake quarantine, idempotent Postgres upserts and per-batch metrics.
"""

from __future__ import annotations

import dataclasses
import os
import time
from collections import Counter
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from shopmetric.generator import OrderSimulator

pytestmark = pytest.mark.spark

KIND_TO_REASON = {
    "malformed_json": "MALFORMED_JSON",
    "missing_field": "MISSING_OR_INVALID_FIELD",
    "type_mismatch": "MISSING_OR_INVALID_FIELD",
    "negative_amount": "NON_POSITIVE_AMOUNT",
    "invalid_currency": "UNSUPPORTED_CURRENCY",
    "future_timestamp": "FUTURE_EVENT_TIMESTAMP",
    "bad_schema_version": "UNSUPPORTED_SCHEMA_VERSION",
    "duplicate": "DUPLICATE_ORDER",
}


class CollectingAlerts:
    def __init__(self):
        self.order_ids: list[str] = []

    def publish(self, sdf):
        self.order_ids += [r.order_id for r in sdf.select("order_id").collect()]


def _write_source(path: str, n: int = 1500, files: int = 3) -> pd.DataFrame:
    start = datetime.now(UTC) - timedelta(minutes=10)
    sim = OrderSimulator(seed=4, n_customers=300, fraud_rate=0.03, chaos_rate=0.05, start=start)
    offsets: Counter[int] = Counter()
    rows = []
    for em in sim.run(start, 3, max_events=n):
        part = sum(em.key.encode()) % 3  # stable "partitioner": same customer -> same partition
        rows.append(
            {
                "raw_value": em.value.decode("utf-8", errors="replace"),
                "kafka_key": em.key,
                "kafka_topic": "orders.raw",
                "kafka_partition": part,
                "kafka_offset": offsets[part],
                "kafka_ts": em.ts,
                "sim_kind": em.kind,
            }
        )
        offsets[part] += 1
    pdf = pd.DataFrame(rows)
    os.makedirs(path, exist_ok=True)
    size = -(-len(pdf) // files)
    for i in range(files):  # contiguous chunks, like consecutive Kafka offset ranges
        pdf.iloc[i * size : (i + 1) * size].to_parquet(
            f"{path}/part-{i}.parquet", index=False, coerce_timestamps="us", allow_truncated_timestamps=True
        )
    return pdf


def _run_stream(spark, s, processor, src_path, ckpt, expected_rows):
    from shopmetric.streaming_job import build_processing_stream

    schema = spark.read.parquet(src_path).schema
    source = spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).parquet(src_path)
    q = (
        build_processing_stream(source, s)
        .writeStream.foreachBatch(processor)
        .option("checkpointLocation", ckpt)
        .trigger(processingTime="1 second")
        .start()
    )
    # Processing-time state timeouts make Spark schedule a (cheap) no-data batch every trigger so
    # idle customers can expire; that means the query never reports "idle", so neither
    # availableNow nor processAllAvailable() return. Poll the progress instead.
    deadline = time.time() + 240
    try:
        while time.time() < deadline and q.exception() is None:
            if sum(p["numInputRows"] for p in q.recentProgress) >= expected_rows:
                break
            time.sleep(1)
    finally:
        q.stop()
    assert q.exception() is None, q.exception()
    assert sum(p["numInputRows"] for p in q.recentProgress) >= expected_rows


def test_pipeline_routes_every_record_correctly(spark, tmp_dir, pg_settings):
    from shopmetric.model import ModelProvider
    from shopmetric.sinks import BatchProcessor, LakeWriter
    from shopmetric.warehouse import PostgresWarehouse

    s = dataclasses.replace(pg_settings, lake_uri=f"file://{tmp_dir}/lake", checkpoint_dir=f"{tmp_dir}/ckpt")
    src = _write_source(f"{tmp_dir}/in")
    wh = PostgresWarehouse(s)
    wh.init_schema()
    wh.truncate_all()
    alerts = CollectingAlerts()
    processor = BatchProcessor(wh, ModelProvider(f"{tmp_dir}/no-model"), LakeWriter(s), alerts)

    _run_stream(spark, s, processor, f"{tmp_dir}/in", f"{tmp_dir}/ckpt/a", len(src))

    facts = wh.query("select count(*) as n, count(distinct order_id) as d from fact_orders").iloc[0]
    q = wh.query("select sim_kind, quarantine_reason, count(*) as n from quarantine_log group by 1, 2")
    m = wh.query(
        "select count(*) as batches, sum(input_rows) as rows, sum(chaos_injected) as inj, "
        "sum(chaos_caught) as caught, sum(fraud_rows) as fraud from pipeline_metrics"
    ).iloc[0]

    # Every message is accounted for exactly once: clean + quarantined == input.
    assert int(m.rows) == len(src) == int(facts.n) + int(q.n.sum())
    assert int(facts.n) == int(facts.d)
    assert int(m.batches) == 3

    # Data quality: 100% of injected chaos is caught, each with the right reason code.
    chaos = q[q.sim_kind != "order"]
    assert int(m.inj) == int(m.caught) == int((src.sim_kind != "order").sum()) == int(chaos.n.sum())
    for row in chaos.itertuples():
        assert KIND_TO_REASON[row.sim_kind] == row.quarantine_reason

    # Fraud: quarantined, alerted, and kept out of the fact table.
    fraud_ids = set(
        wh.query("select order_id from quarantine_log where quarantine_reason = 'SUSPECTED_FRAUD'").order_id
    )
    assert int(m.fraud) == len(fraud_ids) > 0
    assert set(alerts.order_ids) == fraud_ids

    # Lake: quarantined raw payloads are in parquet, partitioned by reason.
    lake = spark.read.parquet(f"{tmp_dir}/lake/quarantine/orders")
    assert lake.count() == int(q.n.sum())
    assert {"raw_value", "quarantine_reason", "dt", "batch_id"} <= set(lake.columns)

    # Idempotency: replaying the whole stream from scratch changes nothing in the warehouse.
    _run_stream(spark, s, processor, f"{tmp_dir}/in", f"{tmp_dir}/ckpt/b", len(src))
    again = wh.query("select count(*) as n from fact_orders").iloc[0]
    assert int(again.n) == int(facts.n)
    wh.close()
