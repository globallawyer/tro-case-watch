import test from "node:test";
import assert from "node:assert/strict";
import { syncTaskOutcome } from "../src/sync-task-outcome.js";
import { CatalogClient } from "../src/providers/catalog.js";

test("a completed loop with failed cases is not reported as successful synchronization", () => {
  assert.equal(syncTaskOutcome({ candidateCount: 2, failedCases: 2, syncedCases: 0 }), "failed");
  assert.equal(syncTaskOutcome({ failedCases: 1, syncedCases: 1 }), "partial");
  assert.equal(syncTaskOutcome({ outcome: "unresolved" }), "failed");
  assert.equal(syncTaskOutcome({ syncedCases: 2, failedCases: 0 }), "succeeded");
  assert.equal(syncTaskOutcome({ skipped: true }), "succeeded");
  assert.equal(syncTaskOutcome({ itemsFetched: 15, hydratedItems: 0, hydrationFailures: { "no-entries": 15 } }), "failed");
  assert.equal(syncTaskOutcome({ itemsFetched: 15, hydratedItems: 2, hydrationFailures: { "no-entries": 13 } }), "partial");
});

test("a challenged or empty primary listing never masquerades as a successful empty catalog", async () => {
  const client = new CatalogClient({ enabled: true, discoveryPages: ["/"] });
  client.fetchText = async () => '<title>安全验证</title><input name="slider_token">';
  await assert.rejects(client.discoverCases(), { code: "SOURCE_CHALLENGE" });
  client.fetchText = async () => "<html>Cases</html>";
  await assert.rejects(client.discoverCases(), { code: "EMPTY_DISCOVERY" });
});

test("primary source reading is size bounded and honors a server retry interval", async () => {
  const fetch = globalThis.fetch;
  const client = new CatalogClient({ enabled: true });
  try {
    globalThis.fetch = async () => new Response(new Uint8Array(4 * 1024 * 1024 + 1));
    await assert.rejects(client.fetchText("https://example.test/large"), { code: "RESPONSE_TOO_LARGE" });
    client.lastRequestAt = 0;
    globalThis.fetch = async () => new Response("limited", { status: 429, headers: { "retry-after": "3600" } });
    await assert.rejects(client.fetchText("https://example.test/limited"), { status: 429, retryAfterMs: 3600000 });
  } finally { globalThis.fetch = fetch; }
});

test("a challenged primary lookup never continues probing fallback URLs", async () => {
  const client = new CatalogClient({ enabled: true });
  client.fetchJson = async () => { throw Object.assign(new Error("verification"), { code: "SOURCE_CHALLENGE" }); };
  await assert.rejects(client.lookupCasePath({ stateCode: "IL", year: "2026", serial: "12755" }), { code: "SOURCE_CHALLENGE" });
});
