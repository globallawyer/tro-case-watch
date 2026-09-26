import assert from "node:assert/strict";
import test from "node:test";

import { SignalFeedClient, __test as signalFeedTest } from "../src/providers/signal-feed.js";

function buildPublicRows(count) {
  return Array.from({ length: count }, (_, index) => {
    const id = String(index + 1).padStart(32, "0");
    const docket = `1:2026-cv-${String(index + 1).padStart(5, "0")}`;
    return `<tr><td><a href="/en/cases/${id}-${index + 1}">${docket}</a></td>` +
      `<td title="Plaintiff ${index + 1}"></td><td title="Northern District of Illinois"></td>` +
      `<td>Aug 19, 2026</td></tr>`;
  }).join("");
}

test("public discovery hydrates all 15 published cases by default", async () => {
  const originalFetch = globalThis.fetch;
  const listHtml = `<table>${buildPublicRows(15)}</table>`;
  let detailRequests = 0;

  globalThis.fetch = async (url) => {
    if (String(url) === "https://example.test/en/cases/") {
      return new Response(listHtml, { status: 200 });
    }
    detailRequests += 1;
    return new Response("<html></html>", { status: 200 });
  };

  try {
    const client = new SignalFeedClient({
      enabled: true,
      publicCasesUrl: "https://example.test/en/cases/",
      minIntervalMs: 1
    });
    const result = await client.fetchRecent();

    assert.equal(result.source, "public");
    assert.equal(result.items.length, 15);
    assert.equal(detailRequests, 15);
    assert.equal(client.getStatus().maxCasesPerRun, 100);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("authenticated discovery accepts the official 100-case ceiling", () => {
  const client = new SignalFeedClient({
    enabled: true,
    publicCasesUrl: "https://example.test/en/cases/",
    maxCasesPerRun: 500
  });

  assert.equal(client.getStatus().maxCasesPerRun, 100);
});

test("public discovery falls back to the localized home page", async () => {
  const originalFetch = globalThis.fetch;
  const homeHtml = `<table>${buildPublicRows(2)}</table>`;
  const requested = [];

  globalThis.fetch = async (url) => {
    requested.push(String(url));
    return new Response(String(url).endsWith("/en/") ? homeHtml : "<html></html>", { status: 200 });
  };

  try {
    const client = new SignalFeedClient({
      enabled: true,
      publicCasesUrl: "https://example.test/en/cases/",
      minIntervalMs: 1
    });
    const items = await client.fetchPublicRecent();

    assert.equal(items.length, 2);
    assert.deepEqual(requested, ["https://example.test/en/cases/", "https://example.test/en/"]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("public discovery rejects an HTTP 200 page with no case rows", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(
    "<html><body><p>No cases found</p></body></html>",
    { status: 200 }
  );

  try {
    const client = new SignalFeedClient({
      enabled: true,
      publicCasesUrl: "https://example.test/en/cases/",
      minIntervalMs: 1
    });

    await assert.rejects(
      () => client.fetchPublicRecent(),
      (error) => error?.code === "EMPTY_DISCOVERY"
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("recent hydration reports detail failures instead of silently hiding them", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url) => {
    if (String(url) === "https://example.test/en/cases/") {
      return new Response(`<table>${buildPublicRows(1)}</table>`, { status: 200 });
    }
    return new Response("rate limited", { status: 429 });
  };

  try {
    const client = new SignalFeedClient({
      enabled: true,
      publicCasesUrl: "https://example.test/en/cases/",
      minIntervalMs: 1
    });
    const result = await client.fetchRecent();

    assert.equal(result.items.length, 1);
    assert.equal(result.hydratedItems, 0);
    assert.deepEqual(result.hydrationFailures, { "http-429": 1 });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("known cases retry canonical public detail URL variants", async () => {
  const client = new SignalFeedClient({
    enabled: true,
    publicCasesUrl: "https://example.test/en/cases/"
  });
  const requested = [];
  client.fetchPublicDetail = async (url) => {
    requested.push(url);
    if (url.includes("/en/cases/")) {
      const error = new Error("not found");
      error.status = 404;
      throw error;
    }
    return { docketId: "abc", docketNumber: "2:26-cv-00651", entries: [{ entryId: "1" }] };
  };

  const item = await client.fetchKnownCase({
    docketId: "abc",
    docketNumber: "2:26-cv-00651",
    detailUrl: "https://example.test/en/cases/abc-case"
  });

  assert.equal(item.entries.length, 1);
  assert.ok(requested.some((url) => url.includes("/cases/") && !url.includes("/en/cases/")));
});

test("known cases do not retry URL variants after a transient failure", async () => {
  const client = new SignalFeedClient({
    enabled: true,
    publicCasesUrl: "https://example.test/en/cases/"
  });
  let requests = 0;
  client.fetchPublicDetail = async () => {
    requests += 1;
    const error = new Error("rate limited");
    error.status = 429;
    throw error;
  };

  await assert.rejects(() => client.fetchKnownCase({
    docketId: "abc",
    docketNumber: "2:26-cv-00651",
    detailUrl: "https://example.test/en/cases/abc-case"
  }), /rate limited/);
  assert.equal(requests, 1);
});

test("rendered TroHub detail pages preserve docket text and attachments", () => {
  const html = `
    <h1>1:2026-cv-26215 Zhongshan Jializhi Technology Co., Ltd. v. Schedule A Defendants</h1>
    <div>Filing Date</div><div>Sep 09, 2026</div>
    <div>Plaintiff</div><div>Zhongshan Jializhi Technology Co., Ltd.</div>
    <div>Court</div><div>Southern District of Florida</div>
    <span>Sep 10, 2026</span>
    <h3>ORDER ON PLAINTIFF'S EMERGENCY MOTION: Granting [4] Motion to Seal. The Clerk of Court is directed to SEAL the following filings.</h3>
    <div>Document</div>
    <span>Sep 09, 2026</span>
    <h3>COMPLAINT against The Individuals, Partnerships, and Unincorporated Associations Identified on Schedule "A".</h3>
    <div>Document Attachment 1: Civil Cover Sheet Attachment 2: Exhibit 1</div>
  `;

  const item = signalFeedTest.parsePublicDetail(html, "https://example.test/cases/detail");

  assert.equal(item.docketNumber, "1:26-cv-26215");
  assert.equal(item.courtName, "Southern District of Florida");
  assert.equal(item.dateFiled, "2026-09-09");
  assert.equal(item.entries.length, 2);
  assert.match(item.entries[0].title, /Granting \[4\] Motion to Seal/);
  assert.equal(item.entries[0].filedAt, "2026-09-10");
  assert.equal(item.entries[1].attachments.length, 1);
  assert.match(item.entries[1].attachments[0].title, /Civil Cover Sheet/);
});

test("Astro detail parsing accepts props before component metadata", () => {
  const props = JSON.stringify({
    troCase: {
      case_number: "2:2026-cv-00651",
      case_name: "Example v. Defendants",
      case_date: "2026-08-25",
      entries: [{ entry_id: "1", entry_date: "2026-08-26", title: "Full docket text" }]
    }
  }).replaceAll('"', "&quot;");
  const item = signalFeedTest.parsePublicDetail(
    `<astro-island props="${props}" component-url="/components/CaseDetail.astro"></astro-island>`,
    "https://example.test/cases/detail"
  );

  assert.equal(item.docketNumber, "2:26-cv-00651");
  assert.equal(item.entries[0].filedAt, "2026-08-26");
  assert.equal(item.entries[0].title, "Full docket text");
});
