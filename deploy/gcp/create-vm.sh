#!/usr/bin/env bash
set -euo pipefail

: "${PROJECT_ID:?Set PROJECT_ID}"
: "${ZONE:?Set ZONE, for example us-central1-a}"

VM_NAME="${VM_NAME:-trotracker-prod}"
REGION="${REGION:-${ZONE%-*}}"
STATIC_IP_NAME="${STATIC_IP_NAME:-trotracker-ip}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-standard-2}"
BOOT_DISK_SIZE="${BOOT_DISK_SIZE:-50GB}"
NETWORK_TAG="${NETWORK_TAG:-trotracker-web}"
IMAGE_FAMILY="${IMAGE_FAMILY:-ubuntu-2204-lts-amd64}"
IMAGE_PROJECT="${IMAGE_PROJECT:-ubuntu-os-cloud}"

gcloud config set project "$PROJECT_ID" >/dev/null

if ! gcloud compute addresses describe "$STATIC_IP_NAME" --region "$REGION" >/dev/null 2>&1; then
  gcloud compute addresses create "$STATIC_IP_NAME" --region "$REGION"
fi

STATIC_IP="$(gcloud compute addresses describe "$STATIC_IP_NAME" --region "$REGION" --format='value(address)')"

if ! gcloud compute firewall-rules describe trotracker-allow-web >/dev/null 2>&1; then
  gcloud compute firewall-rules create trotracker-allow-web \
    --allow=tcp:22,tcp:80,tcp:443 \
    --target-tags="$NETWORK_TAG"
fi

gcloud compute instances create "$VM_NAME" \
  --zone="$ZONE" \
  --machine-type="$MACHINE_TYPE" \
  --boot-disk-size="$BOOT_DISK_SIZE" \
  --image-family="$IMAGE_FAMILY" \
  --image-project="$IMAGE_PROJECT" \
  --tags="$NETWORK_TAG" \
  --address="$STATIC_IP"

echo
echo "VM created."
echo "Static IP: $STATIC_IP"
echo "SSH: gcloud compute ssh $VM_NAME --zone=$ZONE"
