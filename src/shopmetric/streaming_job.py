"""ShopMetric streaming job (Spark Structured Streaming).

Two independent queries read the same Kafka topic:

  bronze_raw         Kafka -> lake/bronze/orders (parquet, every 30 s by default)
                     Immutable, replayable copy of every message. Large triggers = fewer, bigger
                     files = cheaper storage and faster scans.

  orders_processing  Kafka -> parse + data-quality reason -> stateful per-customer velocity
                     features + in-stream dedup -> MLlib fraud scoring -> foreachBatch fan-out
                     (lake quarantine, Kafka alerts, warehouse facts + metrics).

Each query has its own checkpoint, so they recover (and can be scaled/tuned) independently.
"""

from __future__ import annotations

import logging
import signal
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery, StreamingQueryListener

from .config import Settings, get_settings
from .features import add_velocity_features_stream
from .model import ModelProvider
from .sinks import BatchProcessor, KafkaAlertWriter, LakeWriter
from .spark_session import build_spark
from .transforms import normalize_kafka, parse_orders
from .warehouse import get_warehouse

log = logging.getLogger("shopmetric.stream")


def read_orders(spark: SparkSession, s: Settings) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", s.kafka_bootstrap)
        .option("subscribe", s.topic_orders)
        .option("startingOffsets", s.starting_offsets)
        .option("maxOffsetsPerTrigger", s.max_offsets_per_trigger)  # back-pressure: bounded batches
        .option("failOnDataLoss", "false")
        .option("includeHeaders", "true")
        .load()
    )


def start_bronze(raw: DataFrame, s: Settings) -> StreamingQuery:
    return (
        raw.withColumn("ingest_date", F.to_date("kafka_ts"))
        .writeStream.queryName("bronze_raw")
        .format("parquet")
        .option("path", s.lake_path("bronze", "orders"))
        .option("checkpointLocation", f"{s.checkpoint_dir}/bronze")
        .partitionBy("ingest_date")
        .trigger(processingTime=s.bronze_trigger_interval)
        .start()
    )


def build_processing_stream(raw: DataFrame, s: Settings) -> DataFrame:
    return add_velocity_features_stream(
        parse_orders(raw),
        window_ms=s.velocity_window_ms,
        idle_timeout_ms=s.state_idle_timeout_min * 60_000,
    )


def start_processing(stream: DataFrame, s: Settings, processor: BatchProcessor) -> StreamingQuery:
    return (
        stream.writeStream.queryName("orders_processing")
        .foreachBatch(processor)
        .option("checkpointLocation", f"{s.checkpoint_dir}/processing")
        .trigger(processingTime=s.trigger_interval)
        .start()
    )


class ProgressLogger(StreamingQueryListener):
    """Compact per-batch progress for `docker compose logs spark-stream`."""

    def onQueryStarted(self, event) -> None:
        log.info("query started: %s (%s)", event.name, event.id)

    def onQueryProgress(self, event) -> None:
        p = event.progress
        if p.numInputRows:
            log.info(
                "[%s] batch=%d rows=%d in=%.1f/s processed=%.1f/s trigger=%sms",
                p.name,
                p.batchId,
                p.numInputRows,
                p.inputRowsPerSecond or 0.0,
                p.processedRowsPerSecond or 0.0,
                p.durationMs.get("triggerExecution"),
            )

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        log.warning("query terminated: %s exception=%s", event.id, event.exception)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    s = get_settings()
    spark = build_spark(s, "shopmetric-stream")
    spark.streams.addListener(ProgressLogger())

    raw = normalize_kafka(read_orders(spark, s))
    warehouse = get_warehouse(s)
    processor = BatchProcessor(
        warehouse=warehouse,
        models=ModelProvider(s.model_dir, s.model_reload_s),
        lake=LakeWriter(s),
        alerts=KafkaAlertWriter(s) if s.alerts_enabled else None,
    )
    queries = [start_bronze(raw, s), start_processing(build_processing_stream(raw, s), s, processor)]
    log.info(
        "streaming: kafka=%s topic=%s lake=%s warehouse=%s",
        s.kafka_bootstrap,
        s.topic_orders,
        s.lake_uri,
        s.warehouse,
    )

    def _shutdown(sig, _frame):
        log.info("signal %s: stopping queries gracefully", sig)
        for q in queries:
            q.stop()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    try:
        spark.streams.awaitAnyTermination()
    finally:
        failed = [q for q in queries if q.exception() is not None]
        for q in queries:
            if q.isActive:
                q.stop()
        warehouse.close()
        spark.stop()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
