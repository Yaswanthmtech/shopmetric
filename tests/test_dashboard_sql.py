"""Every dashboard query must run on Postgres and transpile cleanly to Snowflake."""

import pytest

from shopmetric.dashboard_queries import ALL_QUERIES
from shopmetric.warehouse import transpile


@pytest.mark.parametrize("name", sorted(ALL_QUERIES))
def test_query_transpiles_to_snowflake(name):
    out = transpile(ALL_QUERIES[name], "snowflake")
    assert out and "::" not in out and "CAST(" not in out.upper().replace("CAST(COUNT", "")


def test_queries_run_on_postgres(pg_settings):
    from shopmetric.warehouse import PostgresWarehouse

    wh = PostgresWarehouse(pg_settings)
    wh.init_schema()
    for sql in ALL_QUERIES.values():
        wh.query(sql)  # raises on any SQL error
    wh.close()
