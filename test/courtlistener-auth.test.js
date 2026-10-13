import assert from "node:assert/strict";
import test from "node:test";

import { CourtListenerClient } from "../src/providers/courtlistener.js";

test("CourtListener retrieves all 125 docket entries across pages", async () => {
  const client = new CourtListenerClient({ baseUrl: "https://example.test/api/rest/v4", apiToken: "test", enableDocketSync: true });
  let calls = 0;
  client.fetchJson = async () => {
    calls += 1;
    return { results: Array.from({ length: calls === 1 ? 100 : 25 }, (_, i) => ({ id: (calls - 1) * 100 + i })),
      next: calls === 1 ? "https://example.test/api/rest/v4/docket-entries/?docket=1&cursor=next" : null };
  };
  assert.equal((await client.fetchDocketEntries(1)).length, 125);
  assert.equal(calls, 2);
});

test("CourtListener rejects cyclic and cross-origin pagination instead of accepting partial data", async () => {
  const client = new CourtListenerClient({ baseUrl: "https://example.test/api/rest/v4", apiToken: "test", enableDocketSync: true });
  const url = "https://example.test/api/rest/v4/docket-entries/?docket=1&page_size=100";
  client.fetchJson = async () => ({ results: [{ id: 1 }], next: url });
  await assert.rejects(client.fetchDocketEntries(1), /pagination incomplete/);
  client.fetchJson = async () => ({ results: [{ id: 1 }], next: "https://other.test/docket-entries/" });
  await assert.rejects(client.fetchDocketEntries(1), /Unexpected/);
});

test("CourtListener page budgets fail visibly instead of declaring a truncated docket complete", async () => {
  const client = new CourtListenerClient({ baseUrl: "https://example.test/api/rest/v4", apiToken: "test", enableDocketSync: true, docketEntriesMaxPages: 1 });
  client.fetchJson = async () => ({ results: [{ id: 1 }], next: "https://example.test/api/rest/v4/docket-entries/?cursor=next" });
  await assert.rejects(client.fetchDocketEntries(1), /pagination incomplete/);
});

test("public CourtListener searches do not send an API token", async () => {
  const originalFetch = globalThis.fetch;
  let authorization = null;
  globalThis.fetch = async (_url, options) => {
    authorization = options.headers.authorization || null;
    return new Response(JSON.stringify({ results: [] }), { status: 200 });
  };

  try {
    const client = new CourtListenerClient({
      baseUrl: "https://example.test/api/rest/v4",
      apiToken: "blocked-token",
      enableDocketSync: true,
      enableDocketAlerts: true
    });
    await client.search({ query: "test", startDate: "2026-01-01" });
    assert.equal(authorization, null);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("authenticated CourtListener docket requests send the API token", async () => {
  const originalFetch = globalThis.fetch;
  let authorization = null;
  globalThis.fetch = async (_url, options) => {
    authorization = options.headers.authorization || null;
    return new Response(JSON.stringify({ id: 1 }), { status: 200 });
  };

  try {
    const client = new CourtListenerClient({
      baseUrl: "https://example.test/api/rest/v4",
      apiToken: "working-token",
      enableDocketSync: true,
      enableDocketAlerts: true
    });
    await client.fetchDocket(1);
    assert.equal(authorization, "Token working-token");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("CourtListener requests abort at the configured deadline without unbounded retries", async () => {
  const originalFetch = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async (_url, options) => {
    calls += 1;
    return new Promise((_resolve, reject) => {
      options.signal.addEventListener("abort", () => {
        reject(new DOMException("Request timed out", "AbortError"));
      }, { once: true });
    });
  };

  try {
    const client = new CourtListenerClient({
      baseUrl: "https://example.test/api/rest/v4",
      apiToken: "",
      timeoutMs: 1000,
      maxRetries: 0
    });
    const startedAt = Date.now();
    await assert.rejects(
      client.search({ query: "test", startDate: "2026-01-01" }),
      (error) => error?.name === "AbortError"
    );
    assert.equal(calls, 1);
    assert.ok(Date.now() - startedAt < 2000);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
