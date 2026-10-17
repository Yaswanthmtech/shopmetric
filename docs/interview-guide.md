# ShopMetric — demo script and interview guide

## The 60-second pitch

"ShopMetric is a real-time sales and fraud platform. Checkout events stream through Kafka into Spark
Structured Streaming. Every event is validated against a versioned data contract; bad records are
quarantined to S3 with a reason code instead of being dropped. For valid orders I keep a small
per-customer state in RocksDB to compute 10-minute velocity features, score each order with a Spark
MLlib gradient-boosted model, and route it: suspected fraud goes to an alerts topic and the
quarantine, clean orders go to Snowflake through idempotent MERGEs, so replays never duplicate data.
A Streamlit command center shows revenue, fraud and pipeline health live, and an LLM copilot answers
questions in SQL behind guardrails. The whole thing starts with one command, and CI runs a full
Docker end-to-end test."

## 3-minute live demo

1. `./shopmetric.sh up` already running → open the dashboard; point at the green **Live** badge and the end-to-end p95 tile.
2. **Revenue** tab: revenue per minute and breakdowns are coming from the warehouse, refreshed every 5 s.
3. `./shopmetric.sh attack card_testing 3` → **Fraud** tab: alerts per minute spike; the alert table shows `orders_10m` climbing — "each order looks normal; the burst is the signal, and that comes from the stateful feature."
4. **Pipeline health**: throughput, latency percentiles, micro-batch table, model card with the rules-baseline comparison.
5. Kafka UI → `orders.fraud_alerts` messages. Optional: **Ask the data** → "Which countries had the most suspected fraud today?"
6. `./shopmetric.sh train` → a minute later the model version in the header changes without restarting the stream.

## Questions you should be ready for

**Why is the Kafka key `customer_id`?**
Kafka only guarantees order within a partition. Keying by customer puts all of a customer's events on
one partition, in order, which is what makes per-customer stateful features (velocity, dedup) correct.
Trade-off: a very hot customer becomes a hot partition.

**Is the pipeline exactly-once?**
Spark's Kafka source plus checkpoints gives exactly-once *processing*; `foreachBatch` sinks are
at-least-once because a failed batch is replayed. I make the sinks idempotent: warehouse writes are
MERGE/`ON CONFLICT` on natural keys (order_id, Kafka topic:partition:offset, batch_id), and the lake
writes one directory per batch with overwrite. The end result is effectively-once. The e2e test
replays the whole stream on a fresh checkpoint and asserts the warehouse does not change. The Kafka
alerts are at-least-once; consumers de-duplicate on `order_id`.

**Why `applyInPandasWithState` and not a windowed aggregation?**
A window aggregation emits per-window results; I need a feature *on each order* reflecting the
customer's previous 10 minutes, plus de-duplication. A per-key state machine does both. State is
JSON in RocksDB (the pandas state API does not support array-typed state), pruned to the window and
expired after 30 minutes idle with a processing-time timeout so state does not grow forever.

**How do you avoid training/serving skew?**
The trainer runs the exact same parsing, data-quality and stateless-feature code as the stream. The
only feature computed differently is velocity (window function in batch, state in streaming), and a
unit test replays events in random micro-batches through the streaming function and asserts the
values equal the batch window exactly.

**Why a time-based split?**
Fraud patterns drift and features depend on history; a random split leaks the future into training
and inflates metrics. I train on the oldest 70%, tune the threshold on the next 15%, and report on
the newest 15% that the model never saw.

**Class imbalance?**
Fraud is ~2.5% of orders. I use instance weights on the positive class (capped at 25) rather than
resampling, choose the decision threshold on validation by F1, and report precision/recall and
AUC-PR — accuracy and ROC-AUC are misleading at this imbalance.

**How do you deploy a new model safely?**
Each training run writes a versioned folder with `metadata.json`. It is promoted (an atomic rename of
`LATEST.json`) only if it beats the rules baseline on the test slice. The stream checks the pointer
every 30 s and swaps models between micro-batches; if loading fails it keeps serving the old one.
Live precision/recall per batch is tracked in `pipeline_metrics`, so a bad model shows up on the
dashboard.

**Tell me about a bug you found in your own ML system.**
My first model looked great offline but, in the live stream, false positives grew minute by minute.
The velocity feature depended on platform traffic: I had simulated training history at a much lower
order rate than the live producer, so live customers looked "fast". I fixed the data (history is now
generated at the live rate, and the simulated customer base grows with traffic so per-customer order
rates stay realistic) and retrained. The lesson: monitor feature distributions and live precision,
not just offline AUC — which is why per-batch precision/recall is on the dashboard.

**Late or out-of-order events?**
Velocity uses event time, and the window check is `[ts − 10 min, ts]`, so a slightly late event is
still counted correctly against state that has not been pruned. Very late events (beyond the window)
get partial features. In production I would add an event-time watermark and route very late events
to the quarantine for backfill.

**Where are the costs, and how did you reduce them?**
Snowflake compute is the expensive part, so the full raw stream and the quarantine go to S3 as zstd
parquet with lifecycle rules (Standard-IA after 30 days, Glacier IR after 90), and only clean,
analytics-ready rows reach Snowflake. The warehouse is XS with 60 s auto-suspend and a resource
monitor. The bronze query uses a 30 s trigger so it writes fewer, larger files.

**How would you scale to 100x?**
More Kafka partitions and a Spark cluster (Databricks / EMR / Kubernetes) instead of local mode;
replace the `toPandas` collection in `foreachBatch` with the Spark-Snowflake connector or Snowpipe
Streaming; increase `maxOffsetsPerTrigger`; move checkpoints to S3; and alert on consumer lag.

**How is the LLM copilot made safe?**
LLM output is treated as untrusted input. The SQL is parsed with sqlglot: exactly one statement, it
must be a SELECT, no DDL/DML/COPY/SET, no system catalogs or `pg_*` functions, only the three
allow-listed tables, and the row count is capped by rewriting `LIMIT`. It then runs as a read-only
database role with a statement timeout. Tests cover the injection cases.

**Schema evolution?**
`schema_version` is part of the contract. Unknown versions are quarantined
(`UNSUPPORTED_SCHEMA_VERSION`), not half-parsed. A backward-compatible change (new optional field)
bumps the minor version and is added to the Spark schema; CI fails if the Pydantic model and the
Spark schema disagree. In production I would put the schema in a schema registry.

## Honest limitations (saying these out loud is a strength)

- Single Kafka broker and Spark in local mode — a dev stack, not a cluster.
- Data and labels are simulated; real fraud labels arrive days later (chargebacks), so live precision/recall would be computed on a delay.
- `foreachBatch` collects each micro-batch to the driver for the warehouse write — fine at thousands of events per second, not at millions.
- Processing-time state timeouts make Spark run a small no-data batch every trigger; that is the price of expiring idle state.
- Fixed FX rates; a real system would join a daily rates table.
