"""Source-agnostic transformations shared by the streaming job, the trainer and the tests."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .contract import spark_schema
from .features import add_stateless_features
from .quality import dq_reason_col

# Columns every downstream step can rely on, regardless of whether the source is Kafka or a test file.
SOURCE_COLUMNS = [
    "raw_value",
    "kafka_key",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_ts",
    "sim_kind",
]


def normalize_kafka(kafka_df: DataFrame) -> DataFrame:
    """Kafka source rows -> SOURCE_COLUMNS (+ a globally unique `source_offset` natural key)."""
    return kafka_df.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.coalesce(
            F.expr("cast(filter(headers, h -> h.key = 'x-sim-kind')[0].value as string)"), F.lit("unknown")
        ).alias("sim_kind"),
    )


def parse_orders(src: DataFrame) -> DataFrame:
    """Parse JSON against the contract, attach a data-quality reason and stateless features."""
    parsed = src.withColumn("_p", F.from_json("raw_value", spark_schema(), {"mode": "PERMISSIVE"}))
    cols = [F.col(f"_p.{f.name}").alias(f.name) for f in spark_schema().fields if f.name != "event_ts"]
    df = parsed.select(
        *[F.col(c) for c in src.columns],
        *cols,
        F.to_timestamp(F.col("_p.event_ts")).alias("event_ts"),
    )
    df = df.withColumn(
        "source_offset",
        F.concat_ws(
            ":",
            F.col("kafka_topic"),
            F.col("kafka_partition").cast("string"),
            F.col("kafka_offset").cast("string"),
        ),
    ).withColumn("dq_reason", dq_reason_col())
    return add_stateless_features(df)
