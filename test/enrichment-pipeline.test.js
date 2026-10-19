import test from "node:test";
import assert from "node:assert/strict";
import { createCaseEnrichmentRefresher } from "../src/case-enrichment.js";
import { SignalFeedClient } from "../src/providers/signal-feed.js";
import { CaseSyncService } from "../src/sync.js";

function fixture(syncService) {
  const checkpoints = new Map();
  const store = {
    getCase: id => ({ id, docket_number: "1:26-cv-04482", court_id: "ilnd" }),
    getCheckpoint: key => checkpoints.get(key),
    saveCheckpoint: (key, value) => checkpoints.set(key, value),
    refreshCaseDocketSummary() {}
  };
  return createCaseEnrichmentRefresher({ store, syncService, wait: async () => {} });
}

test("configured source order is honored and failed sources never prevent a rich fallback", async () => {
  const calls = [];
  const refresh = fixture({
    async enrichCaseWithPriorityFeed() { calls.push("worldtro"); return { enriched: true, entries: 56 }; },
    async enrichCaseWithSignalFeed() { calls.push("signal-feed"); throw new TypeError("fetch failed"); },
    async enrichCaseWithLawFirmLookup() { calls.push("61tro"); return { enriched: true, docketEntriesUpserted: 56 }; }
  });
  const result = await refresh(1, { providers: ["worldtro", "signal-feed", "61tro"] });
  assert.deepEqual(calls, ["worldtro", "signal-feed", "61tro"]);
  assert.deepEqual(result.completedProviders, ["worldtro", "61tro"]);
  assert.equal(result.failedProviders[0].provider, "signal-feed");
  assert.equal(result.failedProviders[0].retryable, true);
});

test("a source outage opens one persistent backoff shared by different cases", async () => {
  let calls = 0;
  const refresh = fixture({ async enrichCaseWithSignalFeed() {
    calls++;
    throw Object.assign(new Error("source unavailable"), { code: "EMPTY_DISCOVERY" });
  } });
  await refresh(1, { providers: ["signal-feed"], respectProviderBackoff: true });
  const second = await refresh(2, { providers: ["signal-feed"], respectProviderBackoff: true });
  assert.equal(calls, 1);
  assert.equal(second.failedProviders[0].code, "PROVIDER_BACKOFF");
  assert.equal(second.failedProviders[0].deferred, true);
});

test("attempted lookup with an error is not reported as completed", async () => {
  const refresh = fixture({ async importLookup() {
    return { sourceResults: { courtlistener: { attempted: true, error: "connection reset", code: "ECONNRESET" } } };
  } });
  const result = await refresh(1, { providers: ["lookup:courtlistener"] });
  assert.deepEqual(result.completedProviders, []);
  assert.equal(result.failedProviders[0].code, "ECONNRESET");
});

test("an empty public search is indeterminate, not a successful no-match", async () => {
  const client = new SignalFeedClient({ enabled: true, publicCasesUrl: "https://example.test/en/cases/" });
  client.requestText = async () => "<html><h1>Cases</h1></html>";
  await assert.rejects(client.lookupByDocket("1:26-cv-04482"), { code: "EMPTY_DISCOVERY" });
});

test("known source pages temporarily unavailable remain retryable without malformed language paths", async () => {
  const client = new SignalFeedClient({ enabled: true, publicCasesUrl: "https://example.test/en/cases/" });
  const urls = client.buildPublicDetailCandidates("https://example.test/en/cases/known");
  assert.ok(urls.every(url => !url.includes("/en/en/")));
  client.fetchPublicDetail = async () => { throw Object.assign(new Error("not found"), { status: 404 }); };
  await assert.rejects(client.fetchKnownCase({ detailUrl: urls[0] }), { code: "KNOWN_CASE_UNAVAILABLE", retryable: true });
});

test("public HTML reading enforces a decompressed size limit and honors Retry-After", async () => {
  const originalFetch = globalThis.fetch;
  const client = new SignalFeedClient({ enabled: true, publicCasesUrl: "https://example.test/en/cases/" });
  client.throttle = async () => {};
  try {
    globalThis.fetch = async () => new Response(new Uint8Array(4 * 1024 * 1024 + 1));
    await assert.rejects(client.requestText("https://example.test/large"), { code: "RESPONSE_TOO_LARGE" });
    globalThis.fetch = async () => new Response("limited", { status: 429, headers: { "retry-after": "3600" } });
    await assert.rejects(client.requestText("https://example.test/limited"), { status: 429, retryAfterMs: 3600_000 });
  } finally { globalThis.fetch = originalFetch; }
});

test("lawfirm service propagates transport failures rather than converting them to not-found", async () => {
  const service = new CaseSyncService({ config: { sync: {} }, store: {
    batchMutations: fn => fn(), getCase: () => ({ id: 1, docket_number: "1:26-cv-04482" })
  }, lawFirms: { async lookupByDocket() { throw Object.assign(new Error("fetch failed"), { code: "ECONNRESET" }); } } });
  service.canEnrichCaseWithLawFirmLookup = () => true;
  await assert.rejects(service.enrichCaseWithLawFirmLookup(1, { force: true }), { code: "ECONNRESET" });
});
