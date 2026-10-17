"""Turn what the pipeline actually measured into resume-ready numbers.

    python -m shopmetric.report                # whole run
    python -m shopmetric.report --minutes 30   # last 30 minutes (e.g. after a load test)

Every figure comes from the warehouse tables the pipeline writes; nothing is hard-coded.
Quote these numbers (and the conditions they were measured under) instead of guessing.
"""

from __future__ import annotations

import argparse
import sys

import pandas as pd

from .config import get_settings
from .warehouse import get_warehouse


def _window(minutes: int | None, col: str) -> str:
    return f"where {col} >= now() - interval '{int(minutes)} minutes'" if minutes else ""


def build_report(minutes: int | None = None) -> str:
    s = get_settings()
    wh = get_warehouse(s, read_only=True)
    m = wh.query(f"select * from pipeline_metrics {_window(minutes, 'batch_ts')} order by batch_id")
    if m.empty:
        return "No pipeline metrics yet - is the stream running? (./shopmetric.sh status)"
    reasons = wh.query(
        f"select quarantine_reason, count(*) as n from quarantine_log {_window(minutes, 'quarantined_at')} "
        "group by 1 order by 2 desc"
    )
    wh.close()

    m["batch_ts"] = pd.to_datetime(m["batch_ts"], utc=True)
    span_s = max((m["batch_ts"].max() - m["batch_ts"].min()).total_seconds(), 1.0)
    rows = int(m["input_rows"].sum())
    per_min = m.set_index("batch_ts")["input_rows"].resample("1min").sum()
    rate = rows / span_s
    tp, fp, fn = (int(m[c].sum()) for c in ("tp", "fp", "fn"))
    precision = tp / (tp + fp) if tp + fp else float("nan")
    recall = tp / (tp + fn) if tp + fn else float("nan")
    injected, caught = int(m["chaos_injected"].sum()), int(m["chaos_caught"].sum())

    def q(col: str, p: float) -> float:
        return float(m[col].dropna().quantile(p)) if m[col].notna().any() else float("nan")

    lines = [
        "# ShopMetric - measured run report",
        f"window            {m['batch_ts'].min():%Y-%m-%d %H:%M} -> {m['batch_ts'].max():%H:%M} UTC "
        f"({span_s / 60:.1f} min, {len(m)} micro-batches, model {m['model_version'].iloc[-1]})",
        f"events processed  {rows:,}  (avg {rate:.1f}/s, peak 1-min {per_min.max() / 60:.1f}/s)",
        f"daily equivalent  {rate * 86400:,.0f} events/day at the average rate",
        f"ingest latency    median {q('ingest_p50_ms', 0.5):,.0f} ms, p95 {q('ingest_p95_ms', 0.95):,.0f} ms"
        "   (produce -> scored in Spark)",
        f"end-to-end        median {q('e2e_p50_ms', 0.5):,.0f} ms, p95 {q('e2e_p95_ms', 0.95):,.0f} ms"
        f"   (produce -> committed in {s.warehouse})",
        f"micro-batch time  median {q('batch_duration_ms', 0.5):,.0f} ms",
        f"data quality      {caught:,}/{injected:,} injected faulty records quarantined "
        f"({(caught / injected * 100) if injected else 0:.1f}%)",
        f"fraud model       live precision {precision:.1%}, recall {recall:.1%} "
        f"(tp={tp}, fp={fp}, fn={fn}; simulated ground truth)",
        "quarantine        " + ", ".join(f"{r.quarantine_reason}={int(r.n)}" for r in reasons.itertuples()),
        "",
        "Resume-ready phrasing (edit to taste, keep the conditions):",
        f"- Processed {rows:,} streaming events at ~{rate:.0f}/s (~{rate * 86400 / 1000:,.0f}K/day) with "
        f"p95 end-to-end latency of {q('e2e_p95_ms', 0.95) / 1000:.1f}s on a local Docker stack.",
        f"- Quarantined {caught / injected:.0%} of {injected:,} injected malformed/invalid records with reason codes; "
        f"fraud model reached {precision:.0%} precision / {recall:.0%} recall on live simulated traffic."
        if injected
        else "- (run longer to measure data-quality catch rate)",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Measured pipeline report")
    ap.add_argument("--minutes", type=int, default=None)
    args = ap.parse_args(argv)
    print(build_report(args.minutes))
    return 0


if __name__ == "__main__":
    sys.exit(main())
