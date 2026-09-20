#!/usr/bin/env bash
# Start / stop / inspect the local single-node KRaft broker.
#
#   scripts/kafka_local.sh start    # format on first run, then boot
#   scripts/kafka_local.sh stop
#   scripts/kafka_local.sh status
#   scripts/kafka_local.sh reset     # wipe all data and re-format
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG="$ROOT/infra/kafka/kraft.properties"
DATA_DIR="$ROOT/.kafka-data"
LOG_FILE="$ROOT/.kafka-data/broker.log"
KAFKA_BIN="${KAFKA_BIN:-/opt/homebrew/opt/kafka/bin}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9094}"

# Homebrew's JDK prints this to stdout and corrupts command output otherwise.
unset _JAVA_OPTIONS || true

start() {
  if status >/dev/null 2>&1; then
    echo "Kafka already running on $BOOTSTRAP"
    return 0
  fi
  mkdir -p "$DATA_DIR"
  # meta.properties only exists once the log dir has been formatted.
  if [[ ! -f "$DATA_DIR/meta.properties" ]]; then
    echo "Formatting $DATA_DIR ..."
    "$KAFKA_BIN/kafka-storage" format \
      --standalone \
      -t "$("$KAFKA_BIN/kafka-storage" random-uuid)" \
      -c "$CONFIG" >/dev/null
  fi
  echo "Starting Kafka (log: $LOG_FILE) ..."
  # Detach every descriptor: if the broker keeps the caller's stdout open, a piped
  # invocation of this script never sees EOF and appears to hang.
  ( cd "$ROOT" && nohup "$KAFKA_BIN/kafka-server-start" "$CONFIG" \
      >"$LOG_FILE" 2>&1 </dev/null & echo $! > "$DATA_DIR/broker.pid" )

  for _ in $(seq 1 60); do
    # A socket probe, not kafka-broker-api-versions: that starts a JVM per poll and
    # takes longer than the broker needs to boot.
    if nc -z localhost "${BOOTSTRAP##*:}" 2>/dev/null; then
      echo "Kafka up on $BOOTSTRAP"
      return 0
    fi
    sleep 1
  done
  echo "Kafka did not come up; tail of $LOG_FILE:" >&2
  tail -30 "$LOG_FILE" >&2
  return 1
}

stop() {
  "$KAFKA_BIN/kafka-server-stop" >/dev/null 2>&1 || true
  # kafka-server-stop matches on the class name and can miss; fall back to the pid.
  if [[ -f "$DATA_DIR/broker.pid" ]]; then
    kill "$(cat "$DATA_DIR/broker.pid")" 2>/dev/null || true
    rm -f "$DATA_DIR/broker.pid"
  fi
  echo "Kafka stopped"
}

status() {
  nc -z localhost "${BOOTSTRAP##*:}" 2>/dev/null
}

case "${1:-start}" in
  start) start ;;
  stop) stop ;;
  restart) stop; sleep 2; start ;;
  status) if status; then echo "running"; else echo "not running"; exit 1; fi ;;
  reset) stop; sleep 2; rm -rf "$DATA_DIR"; start ;;
  logs) tail -f "$LOG_FILE" ;;
  *) echo "usage: $0 {start|stop|restart|status|reset|logs}" >&2; exit 2 ;;
esac
