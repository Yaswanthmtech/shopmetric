import pytest

from shopmetric.copilot import MAX_ROWS, GuardrailError, validate_sql
from shopmetric.warehouse import transpile


@pytest.mark.parametrize(
    "sql",
    [
        "select category, sum(amount_usd) as revenue from fact_orders group by 1 order by 2 desc",
        "with f as (select * from quarantine_log where quarantine_reason = 'SUSPECTED_FRAUD') "
        "select billing_country, count(*) as n from f group by 1",
        "select order_id from fact_orders union all select order_id from quarantine_log",
        "select date_trunc('minute', batch_ts) as m, max(e2e_p95_ms) as p95 from pipeline_metrics "
        "where batch_ts >= now() - interval '15 minutes' group by 1 order by 1",
    ],
)
def test_allows_read_only_queries_and_caps_rows(sql):
    out = validate_sql(sql)
    assert f"LIMIT {MAX_ROWS}" in out.upper()
    assert transpile(out, "snowflake")  # also runs on Snowflake


def test_keeps_a_smaller_limit():
    assert validate_sql("select * from fact_orders limit 5").upper().endswith("LIMIT 5")


@pytest.mark.parametrize(
    "sql",
    [
        "drop table fact_orders",
        "delete from fact_orders",
        "update fact_orders set amount_usd = 0",
        "insert into fact_orders (order_id) values ('x')",
        "select 1; drop table fact_orders",
        "select * from pg_catalog.pg_user",
        "select * from pg_shadow",
        "select usename from information_schema.tables",
        "select pg_sleep(30)",
        "select * into new_table from fact_orders",
        "copy fact_orders to '/tmp/x'",
        "grant all on fact_orders to public",
    ],
)
def test_blocks_dangerous_sql(sql):
    with pytest.raises(GuardrailError):
        validate_sql(sql)


class _FakeMessages:
    def __init__(self, replies):
        self.replies = list(replies)
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        text = self.replies.pop(0)
        return type("Msg", (), {"content": [type("Block", (), {"type": "text", "text": text})()]})()


class _FakeClient:
    def __init__(self, replies):
        self.messages = _FakeMessages(replies)


def test_answer_flow_with_mocked_llm(monkeypatch, pg_settings):
    """Question -> LLM SQL (mocked) -> guardrails -> read-only query -> LLM summary (mocked)."""
    from shopmetric import copilot
    from shopmetric.warehouse import PostgresWarehouse

    PostgresWarehouse(pg_settings).init_schema()
    fake = _FakeClient(
        [
            "Here you go:\n<sql>select category, count(*) as orders from fact_orders group by 1;</sql>",
            "Electronics leads with the most orders.",
        ]
    )
    monkeypatch.setattr(copilot, "_client", lambda _key: fake)
    wh = PostgresWarehouse(pg_settings, read_only=True)
    ans = copilot.answer("orders by category?", wh, "test-key", "claude-haiku-5-5")
    wh.close()
    assert ans.error is None, ans.error
    assert "LIMIT 500" in ans.sql.upper() and list(ans.data.columns) == ["category", "orders"]
    assert ans.summary.startswith("Electronics")
    assert fake.messages.calls[0]["model"] == "claude-haiku-5-5"


def test_answer_flow_blocks_malicious_llm_output(monkeypatch):
    from shopmetric import copilot

    monkeypatch.setattr(copilot, "_client", lambda _key: _FakeClient(["<sql>drop table fact_orders</sql>"]))
    ans = copilot.answer("delete everything", warehouse=None, api_key="k", model="m")
    assert ans.error and ans.error.startswith("Blocked by guardrails") and ans.data is None
