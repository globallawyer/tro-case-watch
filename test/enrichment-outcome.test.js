import test from "node:test";
import assert from "node:assert/strict";
import { didEnrichmentComplete, enrichmentFailure, enrichmentResultFailure, nextEnrichmentProviderState, planEnrichmentRetry } from "../src/enrichment-outcome.js";

test("empty or skipped provider results never report successful enrichment", () => {
  for (const result of [undefined, null, {}, { casesUpserted: 1 }, { enriched: false, reason: "not-found" }, { skipped: true }]) {
    assert.equal(didEnrichmentComplete(result), false);
  }
});

test("transport errors without HTTP status and verification pages remain retryable", () => {
  const failures = [
    new TypeError("fetch failed", { cause: { code: "ECONNRESET" } }),
    Object.assign(new Error("timed out"), { name: "AbortError" }),
    Object.assign(new Error("no rows"), { code: "EMPTY_DISCOVERY" }),
    Object.assign(new Error("verification required"), { code: "SOURCE_CHALLENGE" })
  ].map(error => enrichmentFailure(error, "source"));
  assert.equal(failures[0].code, "ECONNRESET");
  assert.ok(failures.every(failure => failure.retryable));
  assert.ok(failures[3].retryAfterMs >= 3600_000);
  assert.equal(enrichmentFailure({ status: 401 }, "source").retryable, false);
  assert.equal(enrichmentFailure({ status: 429 }, "source").retryable, true);
});

test("empty and metadata-only results cannot complete a docket job", () => {
  for (const result of [{}, { enriched: false, reason: "empty" }, { enriched: false, reason: "metadata-only", casesUpserted: 1 }]) {
    const failure = enrichmentResultFailure(result, "source");
    assert.equal(didEnrichmentComplete(result), false);
    assert.equal(failure.retryable, true);
  }
  assert.equal(enrichmentResultFailure({ enriched: false, reason: "not-applicable" }, "source"), null);
  assert.equal(enrichmentResultFailure({ enriched: false, reason: "identity-mismatch" }, "source").retryable, false);
  assert.equal(enrichmentResultFailure({ enriched: false, reason: "metadata-only", contentAvailable: false }, "recentfilings").retryable, false);
});

test("partial success retries only failed providers and preserves requested aliases", () => {
  const failure = enrichmentFailure(new TypeError("fetch failed"), "worldtro");
  const plan = planEnrichmentRetry({ providers: ["signal-feed", "worldtro", "lookup"] }, {
    requestedProviders: ["signal-feed", "worldtro", "lookup:courtlistener"],
    completedProviders: ["signal-feed"],
    failedProviders: [failure, enrichmentFailure({ retryable: false, error: "not found" }, "lookup:courtlistener")]
  });
  assert.deepEqual(plan.providers, ["worldtro"]);
  assert.equal(plan.retryAttempts, 1);
  assert.equal(plan.unresolved.length, 1);
});

test("provider cooldowns do not consume failed-attempt budget and are persisted across jobs", () => {
  const failure = enrichmentFailure({ status: 429, error: "limited", retryAfterMs: 7200_000 }, "source");
  const circuit = nextEnrichmentProviderState({}, failure, { now: 0 });
  assert.equal(circuit.nextAttemptAt, "1970-01-01T02:00:00.000Z");
  const deferred = enrichmentFailure({ code: "PROVIDER_BACKOFF", deferred: true, retryable: true, retryAfterMs: 7200_000 }, "source");
  assert.deepEqual(nextEnrichmentProviderState(circuit, deferred), circuit);
  const plan = planEnrichmentRetry({ providers: ["source"], retryAttempts: 5 }, { failedProviders: [deferred] });
  assert.equal(plan.retryAttempts, 5);
  assert.equal(plan.exhausted, false);
  assert.equal(plan.requeueAfterMs, 7200_000);
  assert.equal(nextEnrichmentProviderState(circuit, null, { now: 1 }).failures, 0);
});

test("six actual failures stop immediate retries without losing unresolved outcomes", () => {
  const plan = planEnrichmentRetry({ providers: ["source"], retryAttempts: 5 }, {
    failedProviders: [enrichmentFailure(new Error("fetch failed"), "source")]
  });
  assert.equal(plan.exhausted, true);
  assert.equal(plan.requeueAfterMs, 0);
  assert.equal(plan.unresolved.length, 1);
});

test("memory pauses retry untouched providers without spending failure budget", () => {
  const plan = planEnrichmentRetry({ providers: ["done", "pending"], retryAttempts: 5 }, {
    completedProviders: ["done"], stoppedReason: "memory-pressure"
  });
  assert.deepEqual(plan.providers, ["pending"]);
  assert.equal(plan.retryAttempts, 5);
  assert.equal(plan.exhausted, false);
});

test("one case's empty docket or missing page cannot pause a healthy source globally", () => {
  for (const failure of [enrichmentResultFailure({ enriched: false, reason: "empty" }, "source"),
    enrichmentFailure({ status: 404, retryable: true }, "source")]) {
    assert.deepEqual(nextEnrichmentProviderState({}, failure), {});
  }
});

test("successful provider results are retained even without newly inserted rows", () => {
  assert.equal(didEnrichmentComplete({ enriched: true, entries: 125 }), true);
  assert.equal(didEnrichmentComplete({ entries: 125 }), true);
});
