#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
APP_DATA_DIR="${APP_DATA_DIR:-/var/lib/tro-case-watch/data}"
LEGACY_DATA_DIR="$ROOT_DIR/data"

cd "$ROOT_DIR"

if [ ! -d "$APP_DATA_DIR" ]; then
  sudo mkdir -p "$APP_DATA_DIR"
fi
sudo chown -R "$USER":"$USER" "$APP_DATA_DIR"

if [ -f "$LEGACY_DATA_DIR/tro-watch.sqlite" ] && [ ! -f "$APP_DATA_DIR/tro-watch.sqlite" ]; then
  echo "Migrating existing SQLite data to persistent directory: $APP_DATA_DIR"
  cp -a "$LEGACY_DATA_DIR/." "$APP_DATA_DIR/"
fi

export APP_DATA_DIR
echo "Using persistent app data directory: $APP_DATA_DIR"

docker compose -f deploy/gcp/compose.yml up -d --build
docker compose -f deploy/gcp/compose.yml ps
