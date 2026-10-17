"""Warehouse sinks: Snowflake (cloud) or Postgres (local stand-in), behind one interface.

Writes are idempotent upserts keyed on natural keys (order_id, Kafka source offset, batch_id).
Spark may replay a micro-batch after a failure; idempotent sinks turn at-least-once delivery
into effectively-once results in the warehouse.

All read queries are written once in Postgres dialect and transpiled to Snowflake with sqlglot,
so the dashboard and the AI copilot run unchanged on either backend.
"""

from __future__ import annotations

import logging
import uuid
from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from .config import Settings

log = logging.getLogger("shopmetric.warehouse")

# (column, logical type). Logical types map to each dialect below.
TABLES: dict[str, dict[str, Any]] = {
    "fact_orders": {
        "key": ["order_id"],
        "columns": [
            ("order_id", "STRING"),
            ("event_id", "STRING"),
            ("customer_id", "STRING"),
            ("event_ts", "TS"),
            ("kafka_ts", "TS"),
            ("processed_at", "TS"),
            ("channel", "STRING"),
            ("device_type", "STRING"),
            ("billing_country", "STRING"),
            ("ip_country", "STRING"),
            ("shipping_country", "STRING"),
            ("payment_method", "STRING"),
            ("category", "STRING"),
            ("item_count", "INT"),
            ("amount", "DOUBLE"),
            ("currency", "STRING"),
            ("amount_usd", "DOUBLE"),
            ("discount_pct", "DOUBLE"),
            ("cust_txn_count_10m", "DOUBLE"),
            ("fraud_score", "DOUBLE"),
            ("model_version", "STRING"),
            ("simulated_label", "STRING"),
            ("source_offset", "STRING"),
            ("batch_id", "BIGINT"),
        ],
        "indexes": ["event_ts", "processed_at"],
    },
    "quarantine_log": {
        "key": ["source_offset"],
        "columns": [
            ("source_offset", "STRING"),
            ("order_id", "STRING"),
            ("customer_id", "STRING"),
            ("event_ts", "TS"),
            ("kafka_ts", "TS"),
            ("quarantined_at", "TS"),
            ("quarantine_reason", "STRING"),
            ("fraud_score", "DOUBLE"),
            ("amount_usd", "DOUBLE"),
            ("category", "STRING"),
            ("payment_method", "STRING"),
            ("billing_country", "STRING"),
            ("ip_country", "STRING"),
            ("cust_txn_count_10m", "DOUBLE"),
            ("sim_kind", "STRING"),
            ("simulated_label", "STRING"),
            ("model_version", "STRING"),
            ("batch_id", "BIGINT"),
            ("lake_path", "STRING"),
        ],
        "indexes": ["quarantined_at"],
    },
    "pipeline_metrics": {
        "key": ["batch_id"],
        "columns": [
            ("batch_id", "BIGINT"),
            ("batch_ts", "TS"),
            ("model_version", "STRING"),
            ("input_rows", "INT"),
            ("clean_rows", "INT"),
            ("invalid_rows", "INT"),
            ("duplicate_rows", "INT"),
            ("fraud_rows", "INT"),
            ("chaos_injected", "INT"),
            ("chaos_caught", "INT"),
            ("tp", "INT"),
            ("fp", "INT"),
            ("fn", "INT"),
            ("tn", "INT"),
            ("ingest_p50_ms", "DOUBLE"),
            ("ingest_p95_ms", "DOUBLE"),
            ("e2e_p50_ms", "DOUBLE"),
            ("e2e_p95_ms", "DOUBLE"),
            ("batch_duration_ms", "DOUBLE"),
        ],
        "indexes": ["batch_ts"],
    },
}

TYPES = {
    "postgres": {
        "STRING": "TEXT",
        "INT": "INTEGER",
        "BIGINT": "BIGINT",
        "DOUBLE": "DOUBLE PRECISION",
        "TS": "TIMESTAMPTZ",
    },
    "snowflake": {
        "STRING": "VARCHAR",
        "INT": "INTEGER",
        "BIGINT": "BIGINT",
        "DOUBLE": "FLOAT",
        "TS": "TIMESTAMP_NTZ",
    },
}


def table_columns(table: str) -> list[str]:
    return [c for c, _ in TABLES[table]["columns"]]


def _ts_columns(table: str) -> list[str]:
    return [c for c, t in TABLES[table]["columns"] if t == "TS"]


def _prepare(table: str, pdf: pd.DataFrame, tz_aware: bool) -> pd.DataFrame:
    """Align a batch to the table: column order, UTC timestamps, real integers (not 3.0)."""
    out = pdf.reindex(columns=table_columns(table)).copy()
    for c, kind in TABLES[table]["columns"]:
        if kind == "TS":
            ts = pd.to_datetime(out[c], utc=True)
            out[c] = ts if tz_aware else ts.dt.tz_localize(None)  # Snowflake TIMESTAMP_NTZ holds UTC
        elif kind in ("INT", "BIGINT"):
            out[c] = pd.to_numeric(out[c]).round().astype("Int64")
        elif kind == "DOUBLE":
            out[c] = pd.to_numeric(out[c]).astype("float64")
    return out


def _py(value):
    """pandas/numpy scalars -> plain Python values for DB drivers."""
    if value is None or value is pd.NaT or (isinstance(value, float) and value != value) or value is pd.NA:
        return None
    return value.item() if hasattr(value, "item") and not isinstance(value, pd.Timestamp) else value


class Warehouse(ABC):
    dialect: str

    @abstractmethod
    def init_schema(self) -> None: ...

    @abstractmethod
    def upsert(self, table: str, pdf: pd.DataFrame) -> int: ...

    @abstractmethod
    def _run_query(self, sql: str) -> pd.DataFrame: ...

    @abstractmethod
    def truncate_all(self) -> None: ...

    @abstractmethod
    def close(self) -> None: ...

    def query(self, sql_postgres: str) -> pd.DataFrame:
        """Run a read query written in Postgres dialect (transpiled when needed)."""
        sql = transpile(sql_postgres, self.dialect)
        df = self._run_query(sql)
        df.columns = [str(c).lower() for c in df.columns]
        return df


def transpile(sql: str, dialect: str) -> str:
    if dialect == "postgres":
        return sql
    import sqlglot

    return sqlglot.transpile(sql, read="postgres", write=dialect)[0]


# ------------------------------------------------------------------------- Postgres
class PostgresWarehouse(Warehouse):
    dialect = "postgres"

    def __init__(self, s: Settings, read_only: bool = False) -> None:
        self.s = s
        self.read_only = read_only
        self._conn = None

    def _connect(self):
        import psycopg

        if self._conn is None or self._conn.closed:
            user, pwd = (
                (self.s.pg_reader_user, self.s.pg_reader_password)
                if self.read_only
                else (self.s.pg_user, self.s.pg_password)
            )
            self._conn = psycopg.connect(
                host=self.s.pg_host,
                port=self.s.pg_port,
                dbname=self.s.pg_db,
                user=user,
                password=pwd,
                options="-c timezone=UTC",
                connect_timeout=10,
                autocommit=self.read_only,
            )
        return self._conn

    def init_schema(self) -> None:
        from psycopg import sql

        conn = self._connect()
        with conn.transaction(), conn.cursor() as cur:
            for name, spec in TABLES.items():
                cols = ", ".join(f"{c} {TYPES['postgres'][t]}" for c, t in spec["columns"])
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS {name} ({cols}, PRIMARY KEY ({', '.join(spec['key'])}))"
                )
                for col in spec["indexes"]:
                    cur.execute(f"CREATE INDEX IF NOT EXISTS ix_{name}_{col} ON {name} ({col})")
            # Least-privilege, read-only login for the dashboard and the AI copilot.
            reader = self.s.pg_reader_user
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (reader,))
            if cur.fetchone() is None:
                cur.execute(
                    sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                        sql.Identifier(reader), sql.Literal(self.s.pg_reader_password)
                    )
                )
            cur.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                    sql.Identifier(self.s.pg_db), sql.Identifier(reader)
                )
            )
            cur.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(reader)))
            cur.execute(
                sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}").format(sql.Identifier(reader))
            )
            cur.execute(
                sql.SQL("ALTER ROLE {} SET default_transaction_read_only = on").format(sql.Identifier(reader))
            )
            cur.execute(sql.SQL("ALTER ROLE {} SET statement_timeout = '15s'").format(sql.Identifier(reader)))
        log.info("postgres schema ready")

    def upsert(self, table: str, pdf: pd.DataFrame) -> int:
        if pdf.empty:
            return 0
        spec = TABLES[table]
        cols = table_columns(table)
        data = _prepare(table, pdf, tz_aware=True).drop_duplicates(subset=spec["key"])
        rows = ([_py(v) for v in row] for row in data.astype(object).itertuples(index=False, name=None))
        col_list, key_list = ", ".join(cols), ", ".join(spec["key"])
        stage = f"_stg_{table}"
        conn = self._connect()
        try:
            with conn.transaction(), conn.cursor() as cur:
                cur.execute(f"CREATE TEMP TABLE {stage} (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP")
                with cur.copy(f"COPY {stage} ({col_list}) FROM STDIN") as copy:
                    for row in rows:
                        copy.write_row(row)
                cur.execute(
                    f"INSERT INTO {table} ({col_list}) SELECT {col_list} FROM {stage} "
                    f"ON CONFLICT ({key_list}) DO NOTHING"
                )
                return cur.rowcount
        except Exception:
            self.close()  # drop a possibly broken connection; next call reconnects
            raise

    def _run_query(self, sql: str) -> pd.DataFrame:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                cols = [d.name for d in cur.description]
            if not self.read_only:
                conn.rollback()
            return pd.DataFrame(rows, columns=cols)
        except Exception:
            self.close()
            raise

    def truncate_all(self) -> None:
        conn = self._connect()
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(f"TRUNCATE {', '.join(TABLES)}")

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None


# ------------------------------------------------------------------------ Snowflake
class SnowflakeWarehouse(Warehouse):
    dialect = "snowflake"

    def __init__(self, s: Settings, read_only: bool = False) -> None:
        missing = [
            n
            for n, v in {
                "SNOWFLAKE_ACCOUNT": s.sf_account,
                "SNOWFLAKE_USER": s.sf_user,
                "SNOWFLAKE_PRIVATE_KEY_FILE": s.sf_private_key_file,
            }.items()
            if not v
        ]
        if missing:
            raise RuntimeError(f"Snowflake mode needs {', '.join(missing)} (key-pair auth; see README).")
        self.s = s
        self.read_only = read_only
        self._conn = None

    def _connect(self):
        import snowflake.connector

        if self._conn is None or self._conn.is_closed():
            params = {
                "account": self.s.sf_account,
                "user": self.s.sf_user,
                # Key-pair auth: Snowflake is phasing out single-factor passwords for service users.
                "authenticator": "SNOWFLAKE_JWT",
                "private_key_file": self.s.sf_private_key_file,
                "role": self.s.sf_reader_role if self.read_only else self.s.sf_role,
                "warehouse": self.s.sf_warehouse,
                "database": self.s.sf_database,
                "schema": self.s.sf_schema,
                "client_session_keep_alive": True,
                "session_parameters": {"TIMEZONE": "UTC", "QUERY_TAG": "shopmetric"},
            }
            if self.s.sf_private_key_passphrase:
                params["private_key_file_pwd"] = self.s.sf_private_key_passphrase
            self._conn = snowflake.connector.connect(**params)
        return self._conn

    def init_schema(self) -> None:
        conn = self._connect()
        with conn.cursor() as cur:
            for name, spec in TABLES.items():
                cols = ", ".join(f"{c} {TYPES['snowflake'][t]}" for c, t in spec["columns"])
                # PRIMARY KEY is informational in Snowflake; uniqueness is enforced by MERGE below.
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS {name} ({cols}, PRIMARY KEY ({', '.join(spec['key'])}))"
                )
        log.info("snowflake schema ready")

    def upsert(self, table: str, pdf: pd.DataFrame) -> int:
        from snowflake.connector.pandas_tools import write_pandas

        if pdf.empty:
            return 0
        spec = TABLES[table]
        cols = table_columns(table)
        data = _prepare(table, pdf, tz_aware=False).drop_duplicates(subset=spec["key"])
        data.columns = [c.upper() for c in data.columns]
        stage = f"{table}_stg_{uuid.uuid4().hex[:8]}".upper()
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(f"CREATE TEMPORARY TABLE {stage} LIKE {table}")
            try:
                write_pandas(conn, data, stage, quote_identifiers=False, use_logical_type=True)
                on = " AND ".join(f"t.{k} = s.{k}" for k in spec["key"])
                cur.execute(
                    f"MERGE INTO {table} t USING {stage} s ON {on} "
                    f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) "
                    f"VALUES ({', '.join('s.' + c for c in cols)})"
                )
                return cur.rowcount or 0
            finally:
                cur.execute(f"DROP TABLE IF EXISTS {stage}")

    def _run_query(self, sql: str) -> pd.DataFrame:
        with self._connect().cursor() as cur:
            cur.execute(sql)
            return cur.fetch_pandas_all()

    def truncate_all(self) -> None:
        with self._connect().cursor() as cur:
            for name in TABLES:
                cur.execute(f"TRUNCATE TABLE IF EXISTS {name}")

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None


def get_warehouse(s: Settings, read_only: bool = False) -> Warehouse:
    if s.warehouse == "snowflake":
        return SnowflakeWarehouse(s, read_only=read_only)
    if s.warehouse == "postgres":
        return PostgresWarehouse(s, read_only=read_only)
    raise ValueError(f"unknown WAREHOUSE={s.warehouse!r} (expected postgres|snowflake)")
