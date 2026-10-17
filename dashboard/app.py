"""ShopMetric Command Center - real-time Streamlit dashboard.

Reads only from the warehouse (read-only role), refreshes each panel independently with
`st.fragment(run_every=...)`, and works unchanged on Postgres (local) or Snowflake (cloud).
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime

import altair as alt
import pandas as pd
import streamlit as st

from shopmetric import copilot
from shopmetric import dashboard_queries as Q
from shopmetric.config import get_settings
from shopmetric.warehouse import get_warehouse

st.set_page_config(page_title="ShopMetric · Command Center", page_icon="🛒", layout="wide")

S = get_settings()
REFRESH_S = int(os.getenv("DASHBOARD_REFRESH_S", "5"))
TEAL, RED, AMBER, BLUE, GREY = "#14b8a6", "#ef4444", "#f59e0b", "#3b82f6", "#94a3b8"
REASON_COLORS = {"SUSPECTED_FRAUD": RED, "DUPLICATE_ORDER": AMBER}


# ------------------------------------------------------------------------ data access
@st.cache_resource
def warehouse():
    return get_warehouse(S, read_only=True)


def run(sql: str) -> pd.DataFrame:
    try:
        return warehouse().query(sql)
    except Exception as exc:  # noqa: BLE001 - show, don't crash, while the stack is warming up
        st.session_state["last_error"] = f"{exc.__class__.__name__}: {exc}"
        return pd.DataFrame()


def model_card() -> dict | None:
    try:
        with open(os.path.join(S.model_dir, "LATEST.json")) as fh:
            pointer = json.load(fh)
        with open(os.path.join(S.model_dir, "versions", pointer["version"], "metadata.json")) as fh:
            return json.load(fh)
    except (OSError, KeyError, ValueError):
        return None


def utc_naive(series: pd.Series) -> pd.Series:
    """UTC timestamps without tz info, so charts show the same UTC clock as the tables."""
    return pd.to_datetime(series, utc=True).dt.tz_localize(None)


def money(x: float) -> str:
    if abs(x) >= 1_000_000:
        return f"${x / 1_000_000:,.2f}M"
    if abs(x) >= 10_000:
        return f"${x / 1000:,.1f}K"
    return f"${x:,.0f}" if abs(x) >= 100 else f"${x:,.2f}"


def pct_delta(now: float, prev: float) -> str | None:
    return f"{(now - prev) / prev:+.0%}" if prev else None


def num(df: pd.DataFrame, col: str, default: float = 0.0) -> float:
    if df.empty or col not in df or pd.isna(df[col].iloc[0]):
        return default
    return float(df[col].iloc[0])


def empty_state(msg: str = "Waiting for data - the stream starts filling this in a few seconds.") -> None:
    st.info(msg, icon="⏳")


def bar(
    df: pd.DataFrame, x: str, y: str, color: str = TEAL, title: str = "", fmt: str = "$,.0f"
) -> alt.Chart:
    return (
        alt.Chart(df, title=title)
        .mark_bar(cornerRadiusEnd=3, color=color)
        .encode(
            x=alt.X(f"{x}:Q", title=None, axis=alt.Axis(format=fmt)),
            y=alt.Y(f"{y}:N", sort="-x", title=None, axis=alt.Axis(labelLimit=220)),
            tooltip=[alt.Tooltip(y), alt.Tooltip(x, format=fmt)],
        )
        .properties(height=alt.Step(22))
    )


# ---------------------------------------------------------------------------- sidebar
with st.sidebar:
    st.title("🛒 ShopMetric")
    st.caption("Real-time, AI-driven sales command center")
    window = st.select_slider(
        "Time window",
        options=[15, 30, 60, 180, 720],
        value=60,
        format_func=lambda m: f"{m} min" if m < 60 else f"{m // 60} h",
    )
    live = st.toggle("Live refresh", value=True, help=f"Panels refresh every {REFRESH_S}s")
    st.divider()
    st.markdown(f"**Warehouse** `{S.warehouse}`  \n**Lake** `{S.lake_uri}`  \n**Topic** `{S.topic_orders}`")
    st.caption("Live demo: `./shopmetric.sh attack card_testing` then watch the Fraud tab.")
    if err := st.session_state.get("last_error"):
        with st.expander("Last warehouse error"):
            st.code(err)

RUN_EVERY = REFRESH_S if live else None


# ----------------------------------------------------------------------------- header
@st.fragment(run_every=RUN_EVERY)
def header() -> None:
    k = run(Q.kpis(window))
    fresh = run(Q.freshness())
    reasons = run(Q.quarantine_by_reason(window))
    perf = run(Q.pipeline_per_minute(window))
    per_min = run(Q.revenue_per_minute(window))

    last_ts = pd.to_datetime(fresh["last_batch_ts"].iloc[0], utc=True) if not fresh.empty else pd.NaT
    age = (datetime.now(UTC) - last_ts).total_seconds() if pd.notna(last_ts) else None
    if age is None:
        st.badge("Waiting for first micro-batch", icon="⏳", color="gray")
    elif age < 15:
        st.badge(
            f"Live · last batch {age:.0f}s ago · model {fresh['model_version'].iloc[0]}",
            icon="🟢",
            color="green",
        )
    elif age < 120:
        st.badge(f"Lagging · last batch {age:.0f}s ago", icon="🟠", color="orange")
    else:
        st.badge(
            f"Stalled · last batch {age / 60:.0f} min ago - check `./shopmetric.sh status`",
            icon="🔴",
            color="red",
        )

    revenue, orders = num(k, "revenue"), num(k, "orders")
    revenue_prev, orders_prev = num(k, "revenue_prev"), num(k, "orders_prev")
    aov = revenue / orders if orders else 0.0
    aov_prev = revenue_prev / orders_prev if orders_prev else 0.0
    fraud = reasons[reasons["quarantine_reason"] == "SUSPECTED_FRAUD"] if not reasons.empty else reasons
    blocked_usd = float(fraud["amount_usd"].sum()) if not fraud.empty else 0.0
    quarantined = int(reasons["records"].sum()) if not reasons.empty else 0
    q_rate = quarantined / (orders + quarantined) if orders + quarantined else 0.0
    p95 = (
        float(perf["e2e_p95_ms"].dropna().iloc[-1])
        if not perf.empty and perf["e2e_p95_ms"].notna().any()
        else None
    )
    spark = per_min["revenue"].astype(float).tolist() if not per_min.empty else None

    c = st.columns(6)
    c[0].metric(
        "Revenue",
        money(revenue),
        pct_delta(revenue, revenue_prev),
        border=True,
        chart_data=spark,
        chart_type="area",
        help=f"Clean orders, last {window} min vs previous {window} min",
    )
    c[1].metric("Orders", f"{orders:,.0f}", pct_delta(orders, orders_prev), border=True)
    c[2].metric("Avg order value", money(aov), pct_delta(aov, aov_prev), border=True)
    c[3].metric(
        "Fraud blocked",
        money(blocked_usd),
        f"{int(fraud['records'].sum()) if not fraud.empty else 0} orders",
        delta_color="off",
        delta_arrow="off",
        border=True,
    )
    c[4].metric(
        "Quarantine rate",
        f"{q_rate:.1%}",
        f"{quarantined:,} records",
        delta_color="off",
        delta_arrow="off",
        border=True,
        help="Share of messages kept out of analytics (data quality, duplicates, fraud)",
    )
    c[5].metric(
        "End-to-end p95",
        f"{p95 / 1000:.2f}s" if p95 else "–",
        border=True,
        help="Kafka produce -> committed in the warehouse (latest minute)",
    )


# ------------------------------------------------------------------------- revenue tab
@st.fragment(run_every=RUN_EVERY)
def revenue_tab() -> None:
    per_min = run(Q.revenue_per_minute(window))
    if per_min.empty:
        empty_state()
        return
    per_min["minute"] = utc_naive(per_min["minute"])
    base = alt.Chart(per_min).encode(
        x=alt.X("minute:T", title="UTC", axis=alt.Axis(format="%H:%M", tickCount="minute", labelOverlap=True))
    )
    area = base.mark_area(color=TEAL, opacity=0.25, line={"color": TEAL}).encode(
        y=alt.Y("revenue:Q", title="Revenue / min (USD)", axis=alt.Axis(format="$,.0f")),
        tooltip=[
            alt.Tooltip("minute:T", format="%H:%M"),
            alt.Tooltip("revenue:Q", format="$,.2f"),
            "orders:Q",
        ],
    )
    st.altair_chart(area.properties(height=260, title="Revenue per minute"), width="stretch")

    left, mid, right = st.columns(3)
    with left:
        df = run(Q.breakdown("category", window))
        st.altair_chart(bar(df, "revenue", "label", title="By category"), width="stretch")
    with mid:
        df = run(Q.breakdown("billing_country", window))
        st.altair_chart(bar(df, "revenue", "label", BLUE, "By billing country"), width="stretch")
    with right:
        df = run(Q.breakdown("payment_method", window))
        st.altair_chart(bar(df, "orders", "label", GREY, "Orders by payment method", ",.0f"), width="stretch")

    st.subheader("Latest clean orders", divider="gray")
    st.dataframe(
        run(Q.recent_orders()),
        hide_index=True,
        column_config={
            "event_ts": st.column_config.DatetimeColumn("Time (UTC)", format="HH:mm:ss"),
            "amount_usd": st.column_config.NumberColumn("Amount", format="dollar"),
            "fraud_score": st.column_config.ProgressColumn(
                "Fraud score", min_value=0.0, max_value=1.0, format="%.3f"
            ),
        },
    )


# --------------------------------------------------------------------------- fraud tab
@st.fragment(run_every=RUN_EVERY)
def fraud_tab() -> None:
    perf = run(Q.pipeline_per_minute(window))
    reasons = run(Q.quarantine_by_reason(window))
    if perf.empty and reasons.empty:
        empty_state()
        return
    tp, fp, fn = (float(perf[c].sum()) if not perf.empty else 0.0 for c in ("tp", "fp", "fn"))
    inj, caught = (
        float(perf[c].sum()) if not perf.empty else 0.0 for c in ("chaos_injected", "chaos_caught")
    )
    c = st.columns(4)
    c[0].metric(
        "Live precision",
        f"{tp / (tp + fp):.1%}" if tp + fp else "–",
        border=True,
        help="Of the orders we blocked, how many were fraud (simulated ground truth)",
    )
    c[1].metric(
        "Live recall",
        f"{tp / (tp + fn):.1%}" if tp + fn else "–",
        border=True,
        help="Of the fraud that happened, how much we blocked",
    )
    c[2].metric(
        "Bad records caught",
        f"{caught / inj:.1%}" if inj else "–",
        f"{caught:,.0f} / {inj:,.0f}",
        delta_color="off",
        delta_arrow="off",
        border=True,
        help="Injected malformed/invalid/duplicate messages quarantined",
    )
    card = model_card()
    c[3].metric(
        "Model AUC-PR (test)",
        f"{card['test_metrics']['auc_pr']:.3f}" if card else "rules",
        card["version"] if card else "no model trained yet",
        delta_color="off",
        delta_arrow="off",
        border=True,
    )

    left, right = st.columns([3, 2])
    with left:
        fpm = run(Q.fraud_per_minute(window))
        if fpm.empty:
            st.success("No suspected fraud in this window.", icon="✅")
        else:
            fpm["minute"] = utc_naive(fpm["minute"])
            fpm["hhmm"] = fpm["minute"].dt.strftime("%H:%M")
            chart = (
                alt.Chart(fpm, title="Fraud alerts per minute")
                .mark_bar(color=RED, cornerRadiusEnd=2)
                .encode(
                    x=alt.X(
                        "hhmm:O", title="UTC", sort=None, axis=alt.Axis(labelAngle=0, labelOverlap="greedy")
                    ),
                    y=alt.Y("alerts:Q", title="alerts"),
                    tooltip=[
                        alt.Tooltip("minute:T", format="%H:%M"),
                        "alerts:Q",
                        alt.Tooltip("blocked_usd:Q", format="$,.2f"),
                    ],
                )
            )
            st.altair_chart(chart.properties(height=240), width="stretch")
    with right:
        if not reasons.empty:
            reasons["color"] = reasons["quarantine_reason"].map(REASON_COLORS).fillna(GREY)
            chart = (
                alt.Chart(reasons, title="Quarantined by reason")
                .mark_bar(cornerRadiusEnd=3)
                .encode(
                    x=alt.X("records:Q", title=None),
                    y=alt.Y("quarantine_reason:N", sort="-x", title=None, axis=alt.Axis(labelLimit=220)),
                    color=alt.Color("color:N", scale=None),
                    tooltip=["quarantine_reason", "records", alt.Tooltip("amount_usd", format="$,.2f")],
                )
            )
            st.altair_chart(chart.properties(height=240), width="stretch")

    scores = run(Q.score_distribution(window))
    if not scores.empty:
        threshold = card["threshold"] if card else 0.6
        hist = (
            alt.Chart(scores, title="Fraud score distribution (log scale)")
            .mark_bar(opacity=0.8)
            .encode(
                x=alt.X("fraud_score:Q", bin=alt.Bin(maxbins=40), title="fraud score"),
                y=alt.Y("count():Q", scale=alt.Scale(type="symlog"), title="orders"),
                color=alt.Color(
                    "routed:N", scale=alt.Scale(domain=["clean", "blocked"], range=[TEAL, RED]), title=None
                ),
            )
        )
        rule = (
            alt.Chart(pd.DataFrame({"t": [threshold]}))
            .mark_rule(color=AMBER, strokeDash=[4, 4])
            .encode(x="t:Q")
        )
        st.altair_chart((hist + rule).properties(height=220), width="stretch")

    st.subheader("Latest fraud alerts", divider="red")
    st.dataframe(
        run(Q.recent_alerts()),
        hide_index=True,
        column_config={
            "quarantined_at": st.column_config.DatetimeColumn("Time (UTC)", format="HH:mm:ss"),
            "amount_usd": st.column_config.NumberColumn("Amount", format="dollar"),
            "orders_10m": st.column_config.NumberColumn("Orders (10 min)", format="%d"),
            "fraud_score": st.column_config.ProgressColumn(
                "Score", min_value=0.0, max_value=1.0, format="%.3f"
            ),
            "ground_truth": st.column_config.TextColumn(
                "Ground truth", help="Simulated label, for evaluation only"
            ),
        },
    )


# ------------------------------------------------------------------------ pipeline tab
@st.fragment(run_every=RUN_EVERY)
def pipeline_tab() -> None:
    perf = run(Q.pipeline_per_minute(window))
    if perf.empty:
        empty_state()
        return
    perf["minute"] = utc_naive(perf["minute"])
    perf["hhmm"] = perf["minute"].dt.strftime("%H:%M")
    perf["events_per_s"] = perf["events"].astype(float) / 60
    left, right = st.columns(2)
    with left:
        chart = (
            alt.Chart(perf, title="Throughput (events/s, per minute)")
            .mark_bar(color=BLUE, cornerRadiusEnd=2)
            .encode(
                x=alt.X("hhmm:O", title="UTC", sort=None, axis=alt.Axis(labelAngle=0, labelOverlap="greedy")),
                y=alt.Y("events_per_s:Q", title="events/s"),
                tooltip=[
                    alt.Tooltip("minute:T", format="%H:%M"),
                    alt.Tooltip("events_per_s:Q", format=".1f"),
                    "batches:Q",
                ],
            )
        )
        st.altair_chart(chart.properties(height=250), width="stretch")
    with right:
        lat = perf.melt(
            id_vars="minute",
            value_vars=["ingest_p50_ms", "ingest_p95_ms", "e2e_p50_ms", "e2e_p95_ms"],
            var_name="metric",
            value_name="ms",
        ).dropna()
        chart = (
            alt.Chart(lat, title="Latency (ms)")
            .mark_line(point=True)
            .encode(
                x=alt.X(
                    "minute:T",
                    title="UTC",
                    axis=alt.Axis(format="%H:%M", tickCount="minute", labelOverlap=True),
                ),
                y=alt.Y("ms:Q", title="ms"),
                color=alt.Color("metric:N", title=None, scale=alt.Scale(range=[TEAL, BLUE, AMBER, RED])),
                tooltip=[
                    alt.Tooltip("minute:T", format="%H:%M"),
                    "metric:N",
                    alt.Tooltip("ms:Q", format=",.0f"),
                ],
            )
        )
        st.altair_chart(chart.properties(height=250), width="stretch")
    st.caption(
        "**ingest** = Kafka produce → scored in Spark · **e2e** = Kafka produce → committed in the "
        f"{S.warehouse} warehouse"
    )

    st.subheader("Latest micro-batches", divider="blue")
    st.dataframe(
        run(Q.last_batches()),
        hide_index=True,
        column_config={"batch_ts": st.column_config.DatetimeColumn("Time (UTC)", format="HH:mm:ss")},
    )

    card = model_card()
    with st.expander("Model card" + (f" · {card['version']}" if card else ""), expanded=False):
        if not card:
            st.write(
                "No trained model yet - the stream is using the rules baseline. Run `./shopmetric.sh train`."
            )
        else:
            m, b = card["test_metrics"], card["rules_baseline_test_metrics"]
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "model": card["version"],
                            **{k: m[k] for k in ("auc_pr", "auc_roc", "precision", "recall", "f1")},
                        },
                        {
                            "model": "rules baseline",
                            **{k: b[k] for k in ("auc_pr", "auc_roc", "precision", "recall", "f1")},
                        },
                    ]
                ),
                hide_index=True,
            )
            fi = pd.DataFrame(card["feature_importances"].items(), columns=["feature", "importance"])
            st.altair_chart(
                bar(fi, "importance", "feature", BLUE, "Feature importance", ".2f"), width="stretch"
            )
            st.caption(
                f"{card['split']} · threshold {card['threshold']} · trained {card['trained_at'][:19]} UTC"
            )


# ----------------------------------------------------------------------------- AI tab
def ai_tab() -> None:
    st.markdown(
        "Ask a question in plain English. Claude writes SQL, guardrails validate it, and it runs "
        "read-only against the warehouse."
    )
    if not S.anthropic_api_key:
        st.info(
            "Set `ANTHROPIC_API_KEY` in `.env` and run `./shopmetric.sh up` again to enable the copilot.",
            icon="🔑",
        )
        return
    picked = st.pills("Try one", copilot.EXAMPLES, selection_mode="single", key="example")
    with st.form("ask", clear_on_submit=False):
        question = st.text_input("Your question", value=picked or "", placeholder=copilot.EXAMPLES[0])
        submitted = st.form_submit_button("Ask", type="primary")
    if submitted and question.strip():
        with st.spinner("Thinking…"):
            st.session_state["answer"] = copilot.answer(
                question.strip(), warehouse(), S.anthropic_api_key, S.copilot_model
            )
    ans: copilot.CopilotAnswer | None = st.session_state.get("answer")
    if not ans:
        return
    if ans.error:
        st.error(ans.error)
    if ans.summary:
        st.success(ans.summary, icon="💡")
    if ans.sql:
        st.code(ans.sql, language="sql")
    if ans.data is not None and not ans.data.empty:
        df = ans.data
        st.dataframe(df, hide_index=True)
        numeric = df.select_dtypes("number").columns
        if len(df.columns) >= 2 and len(numeric) >= 1 and df.columns[0] not in numeric and len(df) > 1:
            x = df.columns[0]
            kind = "T" if pd.api.types.is_datetime64_any_dtype(df[x]) else "N"
            mark = alt.Chart(df).mark_line(point=True) if kind == "T" else alt.Chart(df).mark_bar(color=TEAL)
            st.altair_chart(
                mark.encode(
                    x=alt.X(f"{x}:{kind}", sort=None), y=alt.Y(f"{numeric[0]}:Q"), tooltip=list(df.columns)
                ).properties(height=280),
                width="stretch",
            )


# ------------------------------------------------------------------------------ layout
st.title("Sales Command Center")
header()
t_rev, t_fraud, t_pipe, t_ai = st.tabs(
    ["📈 Revenue", "🛡️ Fraud & data quality", "⚙️ Pipeline health", "🤖 Ask the data"]
)
with t_rev:
    revenue_tab()
with t_fraud:
    fraud_tab()
with t_pipe:
    pipeline_tab()
with t_ai:
    ai_tab()
