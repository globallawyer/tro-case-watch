#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/deploy/gcp/compose.yml"
LOCK_FILE="/tmp/tro-nightly-low-priority.lock"
START_DATE="${START_DATE:-2025-01-01}"
COURTLISTENER_ALERT_LIMIT="${COURTLISTENER_ALERT_LIMIT:-2000}"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] nightly low-priority sync already running"
  exit 0
fi

cd "$ROOT_DIR"

run_sync() {
  local label="$1"
  shift
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] start $label"
  docker compose -f "$COMPOSE_FILE" exec -T app node src/server.js "$@"
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] done $label"
}

run_sync "recent" --sync-only recent
sleep 20
run_sync "catalog" --sync-only worldtro
sleep 15
run_sync "courtlistener-alerts" --sync-only courtlistener-alerts --force --limit "$COURTLISTENER_ALERT_LIMIT" --start-date "$START_DATE"
