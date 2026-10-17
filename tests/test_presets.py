"""The .env presets must load into valid Settings and keep the two modes consistent."""

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _read_env(path: Path) -> dict[str, str]:
    out = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            out[key] = value
    return out


@pytest.fixture()
def load_preset(monkeypatch):
    from shopmetric import config

    def _load(name: str):
        for key, value in _read_env(ROOT / name).items():
            monkeypatch.setenv(key, value)
        config.get_settings.cache_clear()
        return config.get_settings()

    yield _load
    config.get_settings.cache_clear()


def test_presets_define_the_same_keys():
    full, laptop = _read_env(ROOT / ".env.example"), _read_env(ROOT / ".env.laptop")
    assert set(laptop) - set(full) <= {"SPARK_DRIVER_JAVA_OPTS"}
    assert set(full) <= set(laptop)


def test_laptop_preset(load_preset):
    s = load_preset(".env.laptop")
    assert not s.lake_is_s3 and s.lake_path("bronze") == "file:///lake/bronze"
    assert (s.spark_master, s.spark_driver_memory, s.spark_shuffle_partitions) == ("local[2]", "768m", 2)
    assert s.warehouse == "postgres"
    env = _read_env(ROOT / ".env.laptop")
    assert env["COMPOSE_PROFILES"] == "local" and env["LOW_MEMORY"] == "true"


def test_full_preset(load_preset):
    s = load_preset(".env.example")
    assert s.lake_is_s3 and s.bucket == "shopmetric-lake" and s.s3_endpoint == "http://seaweedfs:8333"
    assert _read_env(ROOT / ".env.example")["COMPOSE_PROFILES"] == "local,s3local,ui"


def test_spark_session_conf_for_laptop(load_preset, monkeypatch):
    """build_spark must pass the low-memory JVM options through to the driver."""
    from shopmetric import spark_session

    captured = {}

    class FakeBuilder:
        def appName(self, _):
            return self

        def master(self, m):
            captured["master"] = m
            return self

        def config(self, k, v):
            captured[k] = v
            return self

        def getOrCreate(self):
            raise RuntimeError("stop before starting a JVM")

    monkeypatch.setattr(spark_session.SparkSession, "builder", FakeBuilder())
    with pytest.raises(RuntimeError):
        spark_session.build_spark(load_preset(".env.laptop"), "t")
    assert captured["master"] == "local[2]"
    assert captured["spark.driver.memory"] == "768m"
    assert captured["spark.driver.extraJavaOptions"] == "-XX:+UseSerialGC"
    assert not any(k.startswith("spark.hadoop.fs.s3a") for k in captured)
