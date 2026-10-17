"""Central, environment-driven configuration (12-factor style).

Every service (producer, Spark job, trainer, dashboard) reads the same variables,
so switching from the local stack to AWS S3 + Snowflake is a .env change, not a code change.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from urllib.parse import urlparse


def _get(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def _int(name: str, default: int) -> int:
    return int(_get(name, str(default)))


def _bool(name: str, default: bool) -> bool:
    return str(_get(name, str(default))).lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Settings:
    # --- Kafka ---------------------------------------------------------------
    kafka_bootstrap: str
    topic_orders: str
    topic_alerts: str
    alerts_enabled: bool

    # --- Lake: S3, S3-compatible (SeaweedFS) or local filesystem ------------
    lake_uri: str
    s3_endpoint: str | None
    aws_region: str
    aws_access_key_id: str | None
    aws_secret_access_key: str | None

    # --- Warehouse ------------------------------------------------------------
    warehouse: str  # "postgres" | "snowflake"
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str
    pg_reader_user: str
    pg_reader_password: str
    sf_account: str | None
    sf_user: str | None
    sf_private_key_file: str | None
    sf_private_key_passphrase: str | None
    sf_role: str | None
    sf_reader_role: str | None
    sf_warehouse: str | None
    sf_database: str | None
    sf_schema: str | None

    # --- Streaming ------------------------------------------------------------
    checkpoint_dir: str
    trigger_interval: str
    bronze_trigger_interval: str
    max_offsets_per_trigger: int
    starting_offsets: str
    velocity_window_s: int
    state_idle_timeout_min: int
    spark_master: str
    spark_driver_memory: str
    spark_driver_java_opts: str | None
    spark_shuffle_partitions: int
    spark_packages: str | None
    spark_ivy_dir: str | None

    # --- Model ----------------------------------------------------------------
    model_dir: str
    model_reload_s: int

    # --- AI copilot -----------------------------------------------------------
    anthropic_api_key: str | None
    copilot_model: str

    @property
    def lake_is_s3(self) -> bool:
        return self.lake_uri.startswith(("s3a://", "s3://"))

    @property
    def bucket(self) -> str | None:
        return urlparse(self.lake_uri).netloc if self.lake_is_s3 else None

    @property
    def velocity_window_ms(self) -> int:
        return self.velocity_window_s * 1000

    def lake_path(self, *parts: str) -> str:
        return "/".join([self.lake_uri.rstrip("/"), *[p.strip("/") for p in parts]])


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings(
        kafka_bootstrap=_get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"),
        topic_orders=_get("TOPIC_ORDERS", "orders.raw"),
        topic_alerts=_get("TOPIC_ALERTS", "orders.fraud_alerts"),
        alerts_enabled=_bool("ALERTS_ENABLED", True),
        lake_uri=_get("LAKE_URI", "s3a://shopmetric-lake"),
        s3_endpoint=_get("S3_ENDPOINT"),
        aws_region=_get("AWS_REGION", "us-east-1"),
        aws_access_key_id=_get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=_get("AWS_SECRET_ACCESS_KEY"),
        warehouse=_get("WAREHOUSE", "postgres").lower(),
        pg_host=_get("PG_HOST", "localhost"),
        pg_port=_int("PG_PORT", 5432),
        pg_db=_get("PG_DB", "shopmetric"),
        pg_user=_get("PG_USER", "shopmetric"),
        pg_password=_get("PG_PASSWORD", "shopmetric"),
        pg_reader_user=_get("PG_READER_USER", "shopmetric_reader"),
        pg_reader_password=_get("PG_READER_PASSWORD", "reader"),
        sf_account=_get("SNOWFLAKE_ACCOUNT"),
        sf_user=_get("SNOWFLAKE_USER"),
        sf_private_key_file=_get("SNOWFLAKE_PRIVATE_KEY_FILE"),
        sf_private_key_passphrase=_get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
        sf_role=_get("SNOWFLAKE_ROLE", "SHOPMETRIC_WRITER"),
        sf_reader_role=_get("SNOWFLAKE_READER_ROLE", "SHOPMETRIC_READER"),
        sf_warehouse=_get("SNOWFLAKE_WAREHOUSE", "SHOPMETRIC_WH"),
        sf_database=_get("SNOWFLAKE_DATABASE", "SHOPMETRIC"),
        sf_schema=_get("SNOWFLAKE_SCHEMA", "ANALYTICS"),
        checkpoint_dir=_get("CHECKPOINT_DIR", "/checkpoints"),
        trigger_interval=_get("TRIGGER_INTERVAL", "1 second"),
        bronze_trigger_interval=_get("BRONZE_TRIGGER_INTERVAL", "30 seconds"),
        max_offsets_per_trigger=_int("MAX_OFFSETS_PER_TRIGGER", 20000),
        starting_offsets=_get("STARTING_OFFSETS", "earliest"),
        velocity_window_s=_int("VELOCITY_WINDOW_S", 600),
        state_idle_timeout_min=_int("STATE_IDLE_TIMEOUT_MIN", 30),
        spark_master=_get("SPARK_MASTER", "local[*]"),
        spark_driver_memory=_get("SPARK_DRIVER_MEMORY", "2g"),
        spark_driver_java_opts=_get("SPARK_DRIVER_JAVA_OPTS"),
        spark_shuffle_partitions=_int("SPARK_SHUFFLE_PARTITIONS", 4),
        spark_packages=_get("SPARK_PACKAGES"),
        spark_ivy_dir=_get("SPARK_IVY_DIR"),
        model_dir=_get("MODEL_DIR", "/models/fraud"),
        model_reload_s=_int("MODEL_RELOAD_S", 30),
        anthropic_api_key=_get("ANTHROPIC_API_KEY"),
        copilot_model=_get("COPILOT_MODEL", "claude-haiku-5-5"),
    )
