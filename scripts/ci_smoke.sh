#!/usr/bin/env bash
# End-to-end smoke test of the real Docker stack (run by CI, also handy locally).
# Brings everything up with a small model, lets data flow, injects fraud, asserts results, tears down.
set -euo pipefail
cd "$(dirname "$0")/.."

[[ -f .env ]] || cp .env.example .env
export TRAIN_ARGS="--orders 40000 --max-iter 20"

cleanup() {
  docker compose logs --tail 60 spark-stream || true
  ./shopmetric.sh down || true
}
trap cleanup EXIT

./shopmetric.sh up
./shopmetric.sh attack card_testing 2 &
sleep 120

psql_q() { docker compose exec -T postgres psql -U shopmetric -d shopmetric -tAc "$1"; }
facts=$(psql_q "select count(*) from fact_orders")
batches=$(psql_q "select count(*) from pipeline_metrics")
quarantined=$(psql_q "select count(*) from quarantine_log")
model=$(psql_q "select model_version from pipeline_metrics order by batch_id desc limit 1")
echo "fact_orders=$facts pipeline_metrics=$batches quarantine_log=$quarantined model=$model"

(( facts > 100 ))   || { echo "too few clean orders"; exit 1; }
(( batches > 10 ))  || { echo "too few micro-batches"; exit 1; }
[[ "$model" == gbt-* ]] || { echo "trained model was not loaded"; exit 1; }

./shopmetric.sh report
echo "SMOKE TEST PASSED"
