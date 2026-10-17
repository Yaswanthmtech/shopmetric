from __future__ import annotations

import os
import shutil
import tempfile

import pytest


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    from shopmetric.spark_session import ROCKSDB_PROVIDER, use_current_python_for_workers

    use_current_python_for_workers()

    # Python UDF workers are separate processes: make `shopmetric` importable for them too
    # (not needed when the package is pip-installed, as it is in Docker and CI).
    src = os.path.join(os.path.dirname(os.path.dirname(__file__)), "src")
    os.environ["PYTHONPATH"] = os.pathsep.join(p for p in (src, os.environ.get("PYTHONPATH")) if p)

    session = (
        SparkSession.builder.master("local[2]")
        .appName("shopmetric-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.streaming.stateStore.providerClass", ROCKSDB_PROVIDER)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture()
def tmp_dir():
    d = tempfile.mkdtemp(prefix="shopmetric-test-")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture()
def pg_settings(monkeypatch):
    """Points the warehouse at a test Postgres (CI service or local). Skips if unreachable."""
    psycopg = pytest.importorskip("psycopg")
    host = os.getenv("TEST_PG_HOST", "localhost")
    port = int(os.getenv("TEST_PG_PORT", "5432"))
    try:
        psycopg.connect(
            host=host,
            port=port,
            dbname="shopmetric",
            user="shopmetric",
            password="shopmetric",
            connect_timeout=3,
        ).close()
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"no test postgres at {host}:{port} ({exc})")

    from shopmetric import config

    for k, v in {"PG_HOST": host, "PG_PORT": str(port), "WAREHOUSE": "postgres"}.items():
        monkeypatch.setenv(k, v)
    config.get_settings.cache_clear()
    settings = config.get_settings()
    yield settings
    config.get_settings.cache_clear()
