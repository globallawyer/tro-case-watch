#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SERVE_DIR="$ROOT_DIR/data/contact-queue"
PORT="${1:-8787}"

if [[ ! -d "$SERVE_DIR" ]]; then
  echo "Directory not found: $SERVE_DIR"
  exit 1
fi

detect_ip() {
  local ip=""
  ip="$(ipconfig getifaddr en0 2>/dev/null || true)"
  if [[ -z "$ip" ]]; then
    ip="$(ipconfig getifaddr en1 2>/dev/null || true)"
  fi
  if [[ -z "$ip" ]]; then
    ip="$(ifconfig | awk '/inet / && $2 != "127.0.0.1" { print $2; exit }')"
  fi
  echo "$ip"
}

HOST_IP="$(detect_ip)"

echo "Serving: $SERVE_DIR"
echo "Port: $PORT"
if [[ -n "$HOST_IP" ]]; then
  echo "Open on iPhone (same Wi-Fi): http://$HOST_IP:$PORT/"
else
  echo "Could not determine LAN IP automatically. Use your Mac's local IP address."
fi
echo
echo "Press Control-C to stop."

cd "$SERVE_DIR"
python3 -m http.server "$PORT"
