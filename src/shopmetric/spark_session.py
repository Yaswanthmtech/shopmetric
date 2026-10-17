"""One place to build a correctly configured SparkSession (stream job, trainer, tests)."""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from .config import Settings

ROCKSDB_PROVIDER = "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"


def use_current_python_for_workers() -> None:
    """Make Spark's Python workers use this interpreter (e.g. the venv), not whatever `python3`
    is first on PATH - a mismatch fails with PYTHON_VERSION_MISMATCH or missing packages."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def build_spark(s: Settings, app_name: str, extra: dict[str, str] | None = None) -> SparkSession:
    use_current_python_for_workers()
    conf: dict[str, str] = {
        "spark.sql.session.timeZone": "UTC",
        "spark.driver.memory": s.spark_driver_memory,
        "spark.sql.shuffle.partitions": str(s.spark_shuffle_partitions),
        "spark.ui.showConsoleProgress": "false",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        # Streaming state lives in RocksDB (off-heap, scales far beyond the JVM heap) and only
        # changelogs are uploaded each micro-batch, which keeps checkpoint latency low.
        "spark.sql.streaming.stateStore.providerClass": ROCKSDB_PROVIDER,
        "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled": "true",
        # Cost: zstd parquet is ~30-40% smaller than snappy for this data -> cheaper S3 storage/scans.
        "spark.sql.parquet.compression.codec": "zstd",
    }
    if s.spark_driver_java_opts:  # e.g. a lighter GC on low-memory laptops
        conf["spark.driver.extraJavaOptions"] = s.spark_driver_java_opts
    if s.spark_packages:
        conf["spark.jars.packages"] = s.spark_packages
    if s.spark_ivy_dir:
        conf["spark.jars.ivy"] = s.spark_ivy_dir

    if s.lake_is_s3:
        conf.update(
            {
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.endpoint.region": s.aws_region,
                "spark.hadoop.fs.s3a.fast.upload": "true",
            }
        )
        if s.s3_endpoint:  # S3-compatible store (SeaweedFS locally)
            conf.update(
                {
                    "spark.hadoop.fs.s3a.endpoint": s.s3_endpoint,
                    "spark.hadoop.fs.s3a.path.style.access": "true",
                    # Stay on the plain S3 API surface that every S3-compatible store implements:
                    # no conditional PUTs, and the classic reader instead of Hadoop 3.5's default
                    # AWS analytics-accelerator stream.
                    "spark.hadoop.fs.s3a.create.conditional.enabled": "false",
                    "spark.hadoop.fs.s3a.input.stream.type": "classic",
                    "spark.hadoop.fs.s3a.connection.ssl.enabled": str(
                        s.s3_endpoint.startswith("https")
                    ).lower(),
                }
            )
        if s.aws_access_key_id and s.aws_secret_access_key:
            conf.update(
                {
                    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                    "spark.hadoop.fs.s3a.access.key": s.aws_access_key_id,
                    "spark.hadoop.fs.s3a.secret.key": s.aws_secret_access_key,
                }
            )
        # else: default AWS provider chain (env vars, ~/.aws profile, instance/IRSA role).

    builder = SparkSession.builder.appName(app_name).master(s.spark_master)
    for key, value in {**conf, **(extra or {})}.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
