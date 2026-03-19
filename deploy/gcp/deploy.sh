#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
APP_DATA_DIR="${APP_DATA_DIR:-/var/lib/tro-case-watch/data}"
LEGACY_DATA_DIR="$ROOT_DIR/data"
FORCE_SEED_RESTORE="${FORCE_SEED_RESTORE:-0}"
SEED_ARCHIVE_PATH="${SEED_ARCHIVE_PATH:-$ROOT_DIR/seed/tro-watch.sqlite.gz}"

cd "$ROOT_DIR"

if [ ! -d "$APP_DATA_DIR" ]; then
  sudo mkdir -p "$APP_DATA_DIR"
fi
sudo chown -R "$USER":"$USER" "$APP_DATA_DIR"

if [ -f "$LEGACY_DATA_DIR/tro-watch.sqlite" ] && [ ! -f "$APP_DATA_DIR/tro-watch.sqlite" ]; then
  echo "Migrating existing SQLite data to persistent directory: $APP_DATA_DIR"
  cp -a "$LEGACY_DATA_DIR/." "$APP_DATA_DIR/"
fi

if [ "$FORCE_SEED_RESTORE" = "1" ]; then
  if [ ! -f "$SEED_ARCHIVE_PATH" ]; then
    echo "Seed archive not found: $SEED_ARCHIVE_PATH" >&2
    exit 1
  fi

  echo "Force restoring app database from seed archive: $SEED_ARCHIVE_PATH"
  rm -f "$APP_DATA_DIR/tro-watch.sqlite" "$APP_DATA_DIR/tro-watch.sqlite-wal" "$APP_DATA_DIR/tro-watch.sqlite-shm" "$APP_DATA_DIR/tro-watch.sqlite-journal"
  gunzip -c "$SEED_ARCHIVE_PATH" > "$APP_DATA_DIR/tro-watch.sqlite"
fi

export APP_DATA_DIR
echo "Using persistent app data directory: $APP_DATA_DIR"

docker compose -f deploy/gcp/compose.yml up -d --build
docker compose -f deploy/gcp/compose.yml ps
