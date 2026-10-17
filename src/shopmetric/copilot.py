"""Ask-the-data copilot: natural-language question -> guarded SQL -> answer (Claude API).

Defense in depth, because LLM output is untrusted input:
  1. Prompt: schema-grounded, SELECT-only instructions.
  2. Parser guardrails (sqlglot): exactly one read-only statement, allow-listed tables only,
     no system catalogs / dangerous functions, row cap enforced by rewriting LIMIT.
  3. Database: the query runs as a read-only role with a statement timeout.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import pandas as pd

from .warehouse import TABLES, Warehouse

ALLOWED_TABLES = set(TABLES)
MAX_ROWS = 500
BLOCKED_FUNCTIONS = {
    "pg_sleep",
    "pg_read_file",
    "pg_read_binary_file",
    "pg_ls_dir",
    "dblink",
    "lo_import",
    "lo_export",
    "pg_terminate_backend",
    "pg_cancel_backend",
    "set_config",
    "system$",
}

COLUMN_NOTES = {
    "fact_orders": "clean, validated orders (one row per order). Revenue = sum(amount_usd).",
    "quarantine_log": "records kept OUT of analytics: data-quality failures, duplicates and SUSPECTED_FRAUD.",
    "pipeline_metrics": "one row per Spark micro-batch: volumes, latency (ms), live model confusion counts.",
}

EXAMPLES = [
    "What was revenue by category in the last hour?",
    "Which 5 countries had the most suspected fraud today, and how much money was blocked?",
    "Show orders per minute for the last 30 minutes",
    "What is the average order value by payment method today?",
    "What is the p95 end-to-end latency per minute over the last 15 minutes?",
]


def schema_doc() -> str:
    parts = []
    for name, spec in TABLES.items():
        cols = ", ".join(f"{c} {t.lower()}" for c, t in spec["columns"])
        parts.append(f"- {name}: {COLUMN_NOTES[name]}\n  columns: {cols}")
    return "\n".join(parts)


SYSTEM_PROMPT = f"""You are the analytics copilot for ShopMetric, a real-time e-commerce sales command center.
Translate the user's question into ONE PostgreSQL SELECT query over these tables:

{schema_doc()}

Rules:
- Read-only: a single SELECT (CTEs allowed). Never modify data.
- Only use the tables and columns listed. Timestamps are UTC (timestamptz).
- "today" = event_ts >= date_trunc('day', now()). "last N minutes" = >= now() - interval 'N minutes'.
- Money is amount_usd (US dollars). Revenue/orders/AOV come from fact_orders only.
- Fraud questions use quarantine_log where quarantine_reason = 'SUSPECTED_FRAUD'.
- Alias every computed column in snake_case. Round money with round(x::numeric(18,2), 2) (never a bare
  ::numeric). Order results meaningfully
  and LIMIT 100 unless the user asks for more.
- If the question cannot be answered from these tables, return:
  SELECT 'cannot answer: <short reason>' AS message
Return only the SQL inside <sql></sql> tags."""


class GuardrailError(ValueError):
    """Generated SQL rejected before it reaches the database."""


def validate_sql(sql: str, max_rows: int = MAX_ROWS) -> str:
    import sqlglot
    from sqlglot import exp

    try:
        statements = [s for s in sqlglot.parse(sql, read="postgres") if s is not None]
    except sqlglot.errors.ParseError as exc:
        raise GuardrailError(f"could not parse SQL: {exc}") from exc
    if len(statements) != 1:
        raise GuardrailError("exactly one SQL statement is allowed")
    tree = statements[0]

    set_op = getattr(exp, "SetOperation", exp.Union)
    if not isinstance(tree, (exp.Select, set_op)):
        raise GuardrailError(f"only SELECT queries are allowed (got {tree.key.upper()})")

    forbidden = [
        getattr(exp, n)
        for n in (
            "Insert",
            "Update",
            "Delete",
            "Drop",
            "Create",
            "Alter",
            "Merge",
            "Command",
            "Into",
            "Grant",
            "Copy",
            "Set",
            "Transaction",
            "TruncateTable",
            "Pragma",
        )
        if hasattr(exp, n)
    ]
    for node in tree.walk():
        if isinstance(node, tuple(forbidden)):
            raise GuardrailError(f"{node.key.upper()} is not allowed")
        if isinstance(node, (exp.Anonymous, exp.Func)):
            fname = (node.name if isinstance(node, exp.Anonymous) else node.sql_name()).lower()
            if fname in BLOCKED_FUNCTIONS or fname.startswith(("pg_", "lo_", "system$")):
                raise GuardrailError(f"function {fname}() is not allowed")

    ctes = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    for table in tree.find_all(exp.Table):
        name = table.name.lower()
        if table.db and table.db.lower() not in ("public",):
            raise GuardrailError(f"schema-qualified table {table.db}.{name} is not allowed")
        if name not in ALLOWED_TABLES | ctes:
            raise GuardrailError(
                f"table {name!r} is not allowed (allowed: {', '.join(sorted(ALLOWED_TABLES))})"
            )

    if isinstance(tree, exp.Select):
        limit = tree.args.get("limit")
        current = None
        if limit is not None:
            try:
                current = int(limit.expression.this)
            except (AttributeError, TypeError, ValueError):
                current = None
        if current is None or current > max_rows:
            tree = tree.limit(max_rows)
    else:
        tree = exp.select("*").from_(tree.subquery("q")).limit(max_rows)
    return tree.sql(dialect="postgres")


def _client(api_key: str):
    from anthropic import Anthropic

    return Anthropic(api_key=api_key)


def _text(message) -> str:
    return "".join(getattr(block, "text", "") for block in message.content)


def generate_sql(question: str, api_key: str, model: str) -> str:
    msg = _client(api_key).messages.create(
        model=model,
        max_tokens=1000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": question}],
    )
    text = _text(msg)
    match = re.search(r"<sql>(.*?)</sql>", text, re.S | re.I)
    return (match.group(1) if match else text).strip().rstrip(";").strip()


def summarize(question: str, df: pd.DataFrame, api_key: str, model: str) -> str:
    preview = df.head(30).to_csv(index=False)
    msg = _client(api_key).messages.create(
        model=model,
        max_tokens=300,
        system="You are a concise sales analyst. Answer in 1-2 sentences using only the data provided. "
        "Format money as $1,234. If the data is empty, say so.",
        messages=[{"role": "user", "content": f"Question: {question}\n\nQuery result (CSV):\n{preview}"}],
    )
    return _text(msg).strip()


@dataclass
class CopilotAnswer:
    question: str
    sql: str | None = None
    data: pd.DataFrame | None = None
    summary: str | None = None
    error: str | None = None


def answer(question: str, warehouse: Warehouse, api_key: str, model: str) -> CopilotAnswer:
    result = CopilotAnswer(question)
    try:
        raw_sql = generate_sql(question, api_key, model)
        result.sql = validate_sql(raw_sql)
        result.data = warehouse.query(result.sql)
        result.summary = summarize(question, result.data, api_key, model)
    except GuardrailError as exc:
        result.error = f"Blocked by guardrails: {exc}"
    except Exception as exc:  # noqa: BLE001 - surface API/DB errors in the UI
        result.error = f"{exc.__class__.__name__}: {exc}"
    return result
