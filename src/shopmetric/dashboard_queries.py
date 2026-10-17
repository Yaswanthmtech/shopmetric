"""SQL behind the Streamlit dashboard (Postgres dialect; transpiled to Snowflake automatically).

Numbers are rounded in the UI, not in SQL: a bare `::numeric` cast transpiles to Snowflake
NUMBER(38,0) and would silently drop the cents.

Kept out of the UI file so tests can run every query against a real Postgres and check that it
transpiles to Snowflake.
"""

from __future__ import annotations


def _since(col: str, minutes: int) -> str:
    return f"{col} >= now() - interval '{int(minutes)} minutes'"


def kpis(minutes: int) -> str:
    return f"""
select
  coalesce(sum(case when {_since("event_ts", minutes)} then amount_usd end), 0) as revenue,
  count(case when {_since("event_ts", minutes)} then 1 end) as orders,
  count(distinct case when {_since("event_ts", minutes)} then customer_id end) as customers,
  coalesce(sum(case when {_since("event_ts", 2 * minutes)} and not {_since("event_ts", minutes)}
                    then amount_usd end), 0) as revenue_prev,
  count(case when {_since("event_ts", 2 * minutes)} and not {_since("event_ts", minutes)} then 1 end) as orders_prev
from fact_orders
where {_since("event_ts", 2 * minutes)}
"""


def revenue_per_minute(minutes: int) -> str:
    return f"""
select date_trunc('minute', event_ts) as minute, sum(amount_usd) as revenue,
       count(*) as orders
from fact_orders where {_since("event_ts", minutes)}
group by 1 order by 1
"""


def breakdown(dimension: str, minutes: int) -> str:
    allowed = {"category", "billing_country", "channel", "payment_method", "device_type"}
    if dimension not in allowed:
        raise ValueError(dimension)
    return f"""
select {dimension} as label, sum(amount_usd) as revenue, count(*) as orders
from fact_orders where {_since("event_ts", minutes)}
group by 1 order by 2 desc
"""


def recent_orders(limit: int = 12) -> str:
    return f"""
select event_ts, order_id, customer_id, category, billing_country, payment_method,
       amount_usd as amount_usd, fraud_score as fraud_score
from fact_orders order by event_ts desc limit {int(limit)}
"""


def quarantine_by_reason(minutes: int) -> str:
    return f"""
select quarantine_reason, count(*) as records, coalesce(sum(amount_usd), 0) as amount_usd
from quarantine_log where {_since("quarantined_at", minutes)}
group by 1 order by 2 desc
"""


def fraud_per_minute(minutes: int) -> str:
    return f"""
select date_trunc('minute', quarantined_at) as minute, count(*) as alerts,
       sum(amount_usd) as blocked_usd
from quarantine_log
where quarantine_reason = 'SUSPECTED_FRAUD' and {_since("quarantined_at", minutes)}
group by 1 order by 1
"""


def recent_alerts(limit: int = 15) -> str:
    return f"""
select quarantined_at, order_id, customer_id, amount_usd as amount_usd, category,
       payment_method, billing_country, ip_country, cust_txn_count_10m as orders_10m,
       fraud_score as fraud_score, simulated_label as ground_truth
from quarantine_log where quarantine_reason = 'SUSPECTED_FRAUD'
order by quarantined_at desc limit {int(limit)}
"""


def score_distribution(minutes: int) -> str:
    return f"""
select fraud_score, 'clean' as routed from fact_orders
where {_since("event_ts", minutes)} and fraud_score is not null
union all
select fraud_score, 'blocked' as routed from quarantine_log
where quarantine_reason = 'SUSPECTED_FRAUD' and {_since("quarantined_at", minutes)}
"""


def pipeline_per_minute(minutes: int) -> str:
    return f"""
select date_trunc('minute', batch_ts) as minute,
       sum(input_rows) as events, count(*) as batches,
       sum(tp) as tp, sum(fp) as fp, sum(fn) as fn,
       sum(chaos_injected) as chaos_injected, sum(chaos_caught) as chaos_caught,
       avg(ingest_p50_ms) as ingest_p50_ms, max(ingest_p95_ms) as ingest_p95_ms,
       avg(e2e_p50_ms) as e2e_p50_ms, max(e2e_p95_ms) as e2e_p95_ms,
       avg(batch_duration_ms) as batch_duration_ms
from pipeline_metrics where {_since("batch_ts", minutes)}
group by 1 order by 1
"""


def last_batches(limit: int = 15) -> str:
    return f"""
select batch_id, batch_ts, model_version, input_rows, clean_rows, invalid_rows, duplicate_rows, fraud_rows,
       e2e_p95_ms as e2e_p95_ms, batch_duration_ms as batch_ms
from pipeline_metrics order by batch_id desc limit {int(limit)}
"""


def freshness() -> str:
    return """
select max(batch_ts) as last_batch_ts, max(batch_id) as last_batch_id,
       (select model_version from pipeline_metrics order by batch_id desc limit 1) as model_version
from pipeline_metrics
"""


ALL_QUERIES = {
    "kpis": kpis(60),
    "revenue_per_minute": revenue_per_minute(60),
    "breakdown_category": breakdown("category", 60),
    "recent_orders": recent_orders(),
    "quarantine_by_reason": quarantine_by_reason(60),
    "fraud_per_minute": fraud_per_minute(60),
    "recent_alerts": recent_alerts(),
    "score_distribution": score_distribution(60),
    "pipeline_per_minute": pipeline_per_minute(60),
    "last_batches": last_batches(),
    "freshness": freshness(),
}
