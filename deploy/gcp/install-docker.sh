#!/usr/bin/env bash
set -euo pipefail

if ! command -v apt-get >/dev/null 2>&1; then
  echo "This script currently supports Ubuntu or Debian images with apt-get."
  exit 1
fi

sudo apt-get update
sudo apt-get install -y ca-certificates curl git
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker "$USER"

echo "Docker installed. Log out and back in once so the docker group takes effect."
