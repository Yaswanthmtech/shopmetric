#!/usr/bin/env bash
# ShopMetric one-command orchestration.
#
#   ./shopmetric.sh up                 build + start everything (trains the model on first run)
#   ./shopmetric.sh down               stop (keeps data)
#   ./shopmetric.sh reset [--all]      stop and wipe Kafka/lake/warehouse/checkpoints (--all: models too)
#   ./shopmetric.sh status             containers + latest micro-batches
#   ./shopmetric.sh logs [service]     follow logs (default: spark-stream)
#   ./shopmetric.sh train [args]       retrain; promoted models are picked up by the stream
#   ./shopmetric.sh attack [scenario] [count]   inject live fraud (card_testing | account_takeover | synthetic_identity)
#   ./shopmetric.sh bench [rate] [minutes]      load test, then print the measured report
#   ./shopmetric.sh report [minutes]   resume-ready numbers measured from the warehouse
#   ./shopmetric.sh topics             Kafka topics, message counts and sample fraud alerts
#   ./shopmetric.sh lake               what the pipeline has written to the data lake
#   ./shopmetric.sh mode laptop|full   switch settings (laptop = fits an 8 GB RAM machine)
set -euo pipefail
cd "$(dirname "$0")"

BOLD=$'\e[1m'; GREEN=$'\e[32m'; YELLOW=$'\e[33m'; RED=$'\e[31m'; RESET=$'\e[0m'
info()  { echo "${BOLD}▸${RESET} $*"; }
ok()    { echo "${GREEN}✔${RESET} $*"; }
warn()  { echo "${YELLOW}!${RESET} $*"; }
die()   { echo "${RED}✘ $*${RESET}" >&2; exit 1; }

DC=(docker compose)
ALL_PROFILES=(--profile train --profile local --profile s3local --profile ui)

env_get() {  # value of KEY in .env (empty if missing)
  [[ -f .env ]] || return 0
  grep -E "^${1}=" .env | tail -1 | cut -d= -f2- || true
}
has_profile() { [[ ",$(env_get COMPOSE_PROFILES)," == *",$1,"* ]]; }
stack_running() { [[ -n "$("${DC[@]}" ps -q 2>/dev/null)" ]]; }

docker_mem_mb() {
  docker info --format '{{.MemTotal}}' 2>/dev/null | awk '{printf "%d", $1/1024/1024}'
}
gb() { awk -v mb="$1" 'BEGIN { printf "%.1f", mb / 1024 }'; }

preflight() {
  command -v docker >/dev/null || die "Docker is not installed (see docs/SETUP.md)."
  docker compose version >/dev/null 2>&1 || die "Docker Compose v2 is required ('docker compose', not 'docker-compose')."
  docker info >/dev/null 2>&1 || die "Docker is not running. Start Docker Desktop, or in WSL: sudo systemctl start docker"
  local mem_mb
  mem_mb=$(docker_mem_mb)
  mem_mb=${mem_mb:-0}
  if [[ ! -f .env ]]; then
    if (( mem_mb > 0 && mem_mb < 7000 )); then
      cp .env.laptop .env
      ok "created .env in LAPTOP mode (Docker has $(gb "$mem_mb") GB RAM)"
    else
      cp .env.example .env
      ok "created .env in FULL mode"
    fi
  fi
  mkdir -p models/fraud secrets
  if (( mem_mb > 0 )); then
    if has_profile s3local && (( mem_mb < 7000 )); then
      warn "Docker has $(gb "$mem_mb") GB RAM but .env is in FULL mode (needs ~5 GB+). Switch with: ./shopmetric.sh mode laptop"
    elif (( mem_mb < 3400 )); then
      warn "Docker has only $(gb "$mem_mb") GB RAM; laptop mode needs ~3.5 GB (docs/SETUP.md, 'Give WSL memory')."
    fi
  fi
  # Ports are only free-checked when the stack is not already running (re-running 'up' is fine).
  if ! stack_running; then
    local port busy=() ports=(8501 4040 9094)
    has_profile local && ports+=(5433)
    has_profile s3local && ports+=(8333)
    has_profile ui && ports+=(8080)
    for port in "${ports[@]}"; do
      if (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then busy+=("$port"); fi
    done
    if (( ${#busy[@]} )); then
      die "port(s) ${busy[*]} already in use by another program. Stop it, or change the left-hand port under 'ports:' in docker-compose.yml."
    fi
  fi
}

wait_healthy() {  # wait_healthy <service> <timeout_s>   (service must define a healthcheck)
  local svc=$1 timeout=${2:-180} start=$SECONDS cid status
  while (( SECONDS - start < timeout )); do
    cid=$("${DC[@]}" ps -q "$svc" 2>/dev/null || true)
    if [[ -n "$cid" ]]; then
      status=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$cid")
      [[ "$status" == "healthy" ]] && return 0
    fi
    sleep 2
  done
  die "$svc did not become healthy within ${timeout}s - try: ./shopmetric.sh logs $svc"
}

cmd_up() {
  local t0=$SECONDS
  preflight
  if [[ "$(env_get LOW_MEMORY)" == "true" ]]; then info "mode: LAPTOP (low-memory settings from .env.laptop)"; fi
  info "building images (first run downloads Spark/Kafka deps; later runs are cached)"
  "${DC[@]}" build --quiet
  if [[ ! -f models/fraud/LATEST.json ]]; then
    info "no fraud model yet - training one on simulated history (~3-6 min, first run only)"
    local targs=()
    read -r -a targs <<< "${TRAIN_ARGS:-}"
    "${DC[@]}" run --rm trainer python -m shopmetric.train_model ${targs[@]+"${targs[@]}"}
  else
    ok "fraud model found: $(sed 's/.*"version": *"\([^"]*\)".*/\1/' models/fraud/LATEST.json)"
  fi
  info "starting Kafka and the warehouse, then init -> spark-stream, producer, dashboard"
  "${DC[@]}" up -d
  wait_healthy dashboard 300
  ok "ShopMetric is up in $(( SECONDS - t0 ))s"
  echo
  echo "  ${BOLD}Dashboard${RESET}    http://localhost:8501"
  echo "  ${BOLD}Spark UI${RESET}     http://localhost:4040"
  if has_profile ui; then echo "  ${BOLD}Kafka UI${RESET}     http://localhost:8080"; else echo "  ${BOLD}Kafka${RESET}        ./shopmetric.sh topics"; fi
  if has_profile local; then echo "  ${BOLD}Postgres${RESET}     localhost:5433  (shopmetric / shopmetric)"; fi
  if has_profile s3local; then echo "  ${BOLD}S3 (lake)${RESET}    http://localhost:8333  bucket shopmetric-lake"; else echo "  ${BOLD}Lake${RESET}         ./shopmetric.sh lake"; fi
  cat <<EOF

  Try:  ./shopmetric.sh attack card_testing 3    then watch the Fraud tab
        ./shopmetric.sh report                    measured numbers for your resume
EOF
}

cmd_down() {
  local t0=$SECONDS
  "${DC[@]}" "${ALL_PROFILES[@]}" down --remove-orphans
  ok "stopped in $(( SECONDS - t0 ))s (data kept; ./shopmetric.sh reset to wipe)"
}

cmd_reset() {
  local t0=$SECONDS
  read -r -p "This deletes Kafka topics, lake, warehouse data and checkpoints. Continue? [y/N] " ans
  [[ "${ans:-}" =~ ^[Yy]$ ]] || { warn "aborted"; return; }
  "${DC[@]}" "${ALL_PROFILES[@]}" down -v --remove-orphans
  if [[ "${1:-}" == "--all" ]]; then
    # model files were written by a container (root-owned on Linux/WSL), so delete them from one
    "${DC[@]}" run --rm --no-deps -T trainer sh -c 'rm -rf /models/fraud/versions /models/fraud/LATEST.json'
    ok "models removed"
  fi
  if [[ "$(env_get WAREHOUSE)" == "snowflake" ]]; then
    warn "cloud mode: Snowflake tables are not touched by 'down -v'. Truncate with:"
    echo "    docker compose run --rm init python -m shopmetric.init_infra --truncate"
  fi
  ok "reset in $(( SECONDS - t0 ))s"
}

cmd_status() {
  "${DC[@]}" ps --format 'table {{.Service}}\t{{.State}}\t{{.Status}}\t{{.Ports}}'
  echo
  info "memory per container"
  docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}' 2>/dev/null || true
  echo
  info "latest micro-batches"
  "${DC[@]}" logs --no-log-prefix --tail 400 spark-stream 2>/dev/null | grep "shopmetric.sinks" | tail -5 || true
}

cmd_logs()   { "${DC[@]}" logs -f --tail 100 "${1:-spark-stream}"; }

cmd_train() {
  preflight
  local paused=0 rc=0
  if [[ "$(env_get LOW_MEMORY)" == "true" && -n "$("${DC[@]}" ps -q spark-stream 2>/dev/null)" ]]; then
    info "laptop mode: pausing the stream while the model trains (Kafka keeps buffering new orders)"
    "${DC[@]}" stop spark-stream >/dev/null
    paused=1
  fi
  "${DC[@]}" run --rm trainer python -m shopmetric.train_model "$@" || rc=$?
  if (( paused )); then
    "${DC[@]}" start spark-stream >/dev/null
    ok "stream resumed: it loads the latest model and catches up on the orders Kafka buffered"
  else
    ok "if promoted, the running stream picks up the new model within MODEL_RELOAD_S seconds"
  fi
  return "$rc"
}

cmd_attack() {
  local scenario=${1:-card_testing} count=${2:-3}
  info "injecting ${count} x ${scenario} (orders arrive in real time over the next few minutes)"
  "${DC[@]}" run --rm -T --no-deps producer python -m shopmetric.producer --scenario "$scenario" --count "$count"
}

cmd_bench() {
  local default_rate=200
  if [[ "$(env_get LOW_MEMORY)" == "true" ]]; then default_rate=50; fi
  local rate=${1:-$default_rate} minutes=${2:-3}
  info "load test: ${rate} orders/s for ${minutes} min (steady producer paused)"
  "${DC[@]}" stop producer >/dev/null
  "${DC[@]}" run --rm -T --no-deps producer python -m shopmetric.producer --rate "$rate" \
      --duration-s "$(( minutes * 60 ))" || true
  sleep 10   # let the last micro-batches land
  "${DC[@]}" start producer >/dev/null
  cmd_report "$(( minutes + 1 ))"
}

cmd_report() {
  if [[ -n "${1:-}" ]]; then
    "${DC[@]}" run --rm -T --no-deps init python -m shopmetric.report --minutes "$1"
  else
    "${DC[@]}" run --rm -T --no-deps init python -m shopmetric.report
  fi
}

cmd_topics() {
  local orders alerts
  orders=$(env_get TOPIC_ORDERS); orders=${orders:-orders.raw}
  alerts=$(env_get TOPIC_ALERTS); alerts=${alerts:-orders.fraud_alerts}
  # CLI tools run inside the broker container with a small heap of their own
  local k=("${DC[@]}" exec -T -e KAFKA_HEAP_OPTS=-Xmx128m kafka) bin=/opt/kafka/bin
  info "topics"
  "${k[@]}" "$bin/kafka-topics.sh" --bootstrap-server localhost:9092 --describe | grep -v "Partition: " || true
  info "messages per partition (topic:partition:latest-offset)"
  "${k[@]}" "$bin/kafka-get-offsets.sh" --bootstrap-server localhost:9092 --topic "$orders"
  "${k[@]}" "$bin/kafka-get-offsets.sh" --bootstrap-server localhost:9092 --topic "$alerts"
  info "sample fraud alerts from ${alerts}"
  "${k[@]}" "$bin/kafka-console-consumer.sh" --bootstrap-server localhost:9092 --topic "$alerts" \
      --from-beginning --max-messages 3 --timeout-ms 10000 2>/dev/null || echo "  (no alerts yet)"
}

cmd_lake() {
  local uri
  uri=$(env_get LAKE_URI)
  if [[ "$uri" == file://* ]]; then
    # shellcheck disable=SC2016  # the script runs inside the container
    "${DC[@]}" exec -T spark-stream sh -c '
      cd /lake 2>/dev/null && [ -n "$(ls -A)" ] || { echo "  the lake is still empty"; exit 0; }
      echo "size:"; du -sh ./* 2>/dev/null
      echo; echo "folders (Hive-style partitions, same layout as on S3):"
      find . -mindepth 1 -maxdepth 5 -type d ! -path "*_spark_metadata*" | sort | tail -25
      echo; echo "parquet files: $(find . -name "*.parquet" | wc -l)"'
  else
    info "the lake is ${uri} (object storage)"
    if has_profile s3local; then echo "  local S3 endpoint: http://localhost:8333 (keys in .env)"; fi
  fi
}

cmd_mode() {
  local mode=${1:-} src key=""
  case "$mode" in
    laptop) src=.env.laptop ;;
    full)   src=.env.example ;;
    *)      die "usage: ./shopmetric.sh mode laptop|full" ;;
  esac
  if [[ -f .env ]]; then
    key=$(env_get ANTHROPIC_API_KEY)
    cp .env .env.bak
  fi
  cp "$src" .env
  if [[ -n "$key" ]]; then
    sed -i.tmp "s|^ANTHROPIC_API_KEY=.*|ANTHROPIC_API_KEY=${key}|" .env && rm -f .env.tmp
  fi
  ok "switched to ${mode} mode (previous settings saved in .env.bak; your API key was kept)"
  if stack_running; then
    warn "the stack is running with the old settings. Start fresh in ${mode} mode with:"
    echo "    ./shopmetric.sh reset && ./shopmetric.sh up"
  fi
}

usage() { awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0"; }

case "${1:-help}" in
  up)      shift; cmd_up "$@" ;;
  down)    shift; cmd_down "$@" ;;
  reset)   shift; cmd_reset "$@" ;;
  status)  shift; cmd_status "$@" ;;
  logs)    shift; cmd_logs "$@" ;;
  train)   shift; cmd_train "$@" ;;
  attack)  shift; cmd_attack "$@" ;;
  bench)   shift; cmd_bench "$@" ;;
  report)  shift; cmd_report "$@" ;;
  topics)  shift; cmd_topics "$@" ;;
  lake)    shift; cmd_lake "$@" ;;
  mode)    shift; cmd_mode "$@" ;;
  *)       usage ;;
esac
