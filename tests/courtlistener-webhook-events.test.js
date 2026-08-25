import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { Store } from "../src/db.js";

function createStore() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "tro-webhook-events-"));
  return {
    directory,
    store: new Store(path.join(directory, "test.sqlite"))
  };
}

function cleanup(directory, store) {
  store.db.close();
  fs.rmSync(directory, { recursive: true, force: true });
}

test("CourtListener webhook inbox deduplicates and completes durable events", () => {
  const { directory, store } = createStore();
  try {
    const receivedAt = "2026-08-25T08:00:00.000Z";
    const first = store.enqueueCourtListenerWebhookEvent({
      idempotencyKey: "event-1",
      eventType: 1,
      payload: { webhook: { event_type: 1 }, payload: { results: [{ id: 11 }] } },
      receivedAt,
      maxPending: 5
    });
    const duplicate = store.enqueueCourtListenerWebhookEvent({
      idempotencyKey: "event-1",
      eventType: 1,
      payload: { ignored: true },
      receivedAt,
      maxPending: 5
    });

    assert.equal(first.queued, true);
    assert.equal(duplicate.duplicate, true);
    assert.equal(duplicate.id, first.id);

    const before = store.getCourtListenerWebhookEventStats({ now: receivedAt });
    assert.equal(before.pendingCount, 1);
    assert.equal(before.readyCount, 1);
    assert.equal(before.lastRealReceivedAt, receivedAt);

    const claimed = store.claimCourtListenerWebhookEvent({ now: receivedAt });
    assert.equal(claimed.id, first.id);
    assert.equal(claimed.attempts, 1);
    assert.equal(claimed.payload.payload.results[0].id, 11);
    assert.equal(store.completeCourtListenerWebhookEvent(claimed.id, { processed: 1 }), true);

    const after = store.getCourtListenerWebhookEventStats({ now: receivedAt });
    assert.equal(after.pendingCount, 0);
    assert.equal(after.processingCount, 0);
    assert.equal(after.completedCount, 1);
  } finally {
    cleanup(directory, store);
  }
});

test("CourtListener webhook inbox bounds backlog and separates probes", () => {
  const { directory, store } = createStore();
  try {
    const first = store.enqueueCourtListenerWebhookEvent({
      idempotencyKey: "probe-1",
      payload: {},
      isProbe: true,
      receivedAt: "2026-08-25T08:00:00.000Z",
      maxPending: 1
    });
    const full = store.enqueueCourtListenerWebhookEvent({
      idempotencyKey: "event-2",
      payload: {},
      receivedAt: "2026-08-25T08:01:00.000Z",
      maxPending: 1
    });

    assert.equal(first.queued, true);
    assert.equal(full.status, "queue-full");
    const stats = store.getCourtListenerWebhookEventStats({ now: "2026-08-25T08:02:00.000Z" });
    assert.equal(stats.lastProbeReceivedAt, "2026-08-25T08:00:00.000Z");
    assert.equal(stats.lastRealReceivedAt, null);
  } finally {
    cleanup(directory, store);
  }
});

test("CourtListener webhook inbox recovers expired workers and caps retries", () => {
  const { directory, store } = createStore();
  try {
    store.enqueueCourtListenerWebhookEvent({
      idempotencyKey: "event-retry",
      payload: {},
      receivedAt: "2026-08-25T08:00:00.000Z"
    });
    const firstClaim = store.claimCourtListenerWebhookEvent({
      now: "2026-08-25T08:00:00.000Z",
      leaseTtlMs: 60_000
    });
    assert.ok(firstClaim);

    const recovered = store.claimCourtListenerWebhookEvent({
      now: "2026-08-25T08:02:00.000Z",
      leaseTtlMs: 60_000
    });
    assert.equal(recovered.id, firstClaim.id);
    assert.equal(recovered.attempts, 2);

    const failed = store.retryCourtListenerWebhookEvent(recovered.id, new Error("permanent"), {
      maxAttempts: 2,
      retryAfterMs: 10_000
    });
    assert.equal(failed.failed, true);
    const stats = store.getCourtListenerWebhookEventStats({ now: "2026-08-25T08:03:00.000Z" });
    assert.equal(stats.failedCount, 1);
    assert.equal(stats.pendingCount, 0);
  } finally {
    cleanup(directory, store);
  }
});
