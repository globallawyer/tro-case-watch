import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { Store } from "../src/db.js";
import { CaseSyncService } from "../src/sync.js";
import { PRIORITY_FEED_ENTRY_SOURCE } from "../src/priority-feed.js";
import { CourtFeedClient } from "../src/providers/courtfeed.js";
import { classifyCase } from "../src/queries.js";

function createStore() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "tro-docket-quality-"));
  return {
    directory,
    store: new Store(path.join(directory, "test.sqlite"))
  };
}

function targetCase(overrides = {}) {
  return {
    source_case_key: "courtlistener:pawd:2:26-cv-00651",
    primary_source: "courtlistener",
    source_case_id: "73194302",
    courtlistener_docket_id: 73194302,
    court_id: "pawd",
    court_name: "Western District of Pennsylvania",
    case_name: "Example Brand v. Schedule A Defendants",
    docket_number: "2:26-cv-00651",
    date_filed: "2026-04-15",
    cause: "15:1114 Trademark Infringement",
    nature_of_suit: "840 Trademark",
    status: "open",
    tags_marker: "|tro|schedule_a|seller_tro|",
    source_urls: [],
    plaintiffs: ["Example Brand"],
    defendants: ["Schedule A Defendants"],
    docket_count: 5,
    latest_docket_number: "5",
    raw: {},
    ...overrides
  };
}

function docketEntry(caseId, source, number, description = `Docket entry ${number}`) {
  return {
    case_id: caseId,
    source_entry_key: `${source}:${caseId}:${number}`,
    primary_source: source,
    source_entry_id: String(number),
    document_type: "Docket Entry",
    entry_number: String(number),
    document_number: String(number),
    filed_at: `2026-04-${String(Number(number) + 10).padStart(2, "0")}T12:00:00.000Z`,
    description,
    absolute_url: `https://example.test/${source}/${number}`,
    is_available: 1,
    raw: {}
  };
}

test("different explicit numbers remain separate despite identical motion text", () => {
  const { directory, store } = createStore();
  try {
    const saved = store.upsertCase(targetCase());
    for (const [source, number] of [["courtlistener", 10], ["signal-feed", 11]]) {
      store.upsertDocketEntry({ ...docketEntry(saved.id, source, number,
        "MOTION for extension of time by Example Brand regarding discovery deadlines and service [9]"), filed_at: "2026-05-01" });
    }
    assert.equal(store.getCase(saved.id).entries.length, 2);
  } finally { store.db.close(); fs.rmSync(directory, { recursive: true, force: true }); }
});

test("attachment-only document numbers do not inflate sequence coverage", () => {
  const { directory, store } = createStore();
  try {
    const saved = store.upsertCase(targetCase());
    store.upsertDocketEntry({ ...docketEntry(saved.id, "courtlistener", 6), entry_number: null,
      raw: { document_type: 2, attachment_number: 1 } });
    const coverage = store.getEntryCoverageForCaseIds([saved.id]).get(saved.id);
    assert.equal(coverage.numberedEntries, 0);
    assert.equal(coverage.lastNumber, 0);
  } finally { store.db.close(); fs.rmSync(directory, { recursive: true, force: true }); }
});

test("empty CourtListener responses never count as successful docket enrichment", async () => {
  const { directory, store } = createStore();
  try {
    const saved = store.upsertCase(targetCase({ last_docket_sync_at: "2026-01-01T00:00:00Z" }));
    const service = new CaseSyncService({ config: { sync: {} }, store,
      courtListener: {
        async fetchDocket() { return { docket_number: saved.docket_number }; },
        async fetchDocketEntries() { return []; },
        hasDocketEntriesAccess() { return true; },
        hasDocketAlertAccess() { return false; },
        absoluteUrl() { return null; }
      } });
    const result = await service.syncSingleCourtListenerDocket(saved);
    assert.equal(result.enriched, false);
    assert.equal(result.reason, "empty");
    assert.equal(store.getCase(saved.id).last_docket_sync_at, saved.last_docket_sync_at);
  } finally { store.db.close(); fs.rmSync(directory, { recursive: true, force: true }); }
});

test("same-day date and timestamp copies merge and sort by actual docket number", () => {
  const { directory, store } = createStore();
  try {
    const saved = store.upsertCase(targetCase());
    store.upsertDocketEntry({ ...docketEntry(saved.id, "courtlistener", 10), filed_at: "2026-05-01" });
    store.upsertDocketEntry({ ...docketEntry(saved.id, "signal-feed", 10), filed_at: "2026-05-01T12:00:00Z" });
    store.upsertDocketEntry({ ...docketEntry(saved.id, "courtlistener", 11), filed_at: "2026-05-01" });
    const entries = store.getCase(saved.id).entries;
    assert.deepEqual(entries.map(x => x.entry_number), ["11", "10"]);
  } finally { store.db.close(); fs.rmSync(directory, { recursive: true, force: true }); }
});

test("partial mirror refresh preserves previous filings from the same source", async () => {
  const { directory, store } = createStore();
  try {
    const saved = store.upsertCase(targetCase());
    store.upsertDocketEntry(docketEntry(saved.id, PRIORITY_FEED_ENTRY_SOURCE, 1));
    store.upsertDocketEntry(docketEntry(saved.id, PRIORITY_FEED_ENTRY_SOURCE, 2));
    const service = new CaseSyncService({ config: { sync: {} }, store,
      priorityFeed: { async enrichCase() { return { url: "https://example.test/case", entries: [{ row_number: "2", filed_at: "2026-04-12", description: "Docket entry 2" }] }; } } });
    await service.syncSinglePriorityFeedCase(store.getCase(saved.id));
    assert.deepEqual(new Set(store.getCase(saved.id).entries.map(x => x.entry_number)), new Set(["1", "2"]));
  } finally { store.db.close(); fs.rmSync(directory, { recursive: true, force: true }); }
});

test("rendered signal entries are scoped to their case even when titles and dates match", () => {
  const { directory, store } = createStore();
  try {
    const service = new CaseSyncService({ config: { sync: {} }, store });
    const result = service.ingestSignalFeedItems(["1:26-cv-26215", "1:26-cv-26216"].map((docketNumber) => ({
      docketNumber, docketId: docketNumber, courtId: "flsd", courtName: "Southern District of Florida",
      caseName: "Example Brand v. Schedule A Defendants", dateFiled: "2026-09-09",
      entries: [{ entryId: "rendered:2026-09-10:order:1", title: "ORDER granting motion to seal", filedAt: "2026-09-10" }]
    })));
    assert.equal(result.casesUpserted, 2);
    const rows = store.db.prepare("SELECT case_id,COUNT(*) AS n FROM docket_entries GROUP BY case_id").all();
    assert.equal(rows.length, 2);
    assert.ok(rows.every(x => x.n === 1));
  } finally { store.db.close(); fs.rmSync(directory, { recursive: true, force: true }); }
});

test("signal refresh uses a known detail URL and refuses a different court", async () => {
  const { directory, store } = createStore();
  try {
    const saved = store.upsertCase(targetCase({ raw: { signal_feed: { detailUrl: "https://example.test/known-case" } } }));
    const service = new CaseSyncService({ config: { sync: {} }, store,
      signalFeed: { enabled: true,
        async lookupByDocket() { throw new Error("must not search only recent cases"); },
        async fetchKnownCase(candidate) {
          assert.equal(candidate.detailUrl, "https://example.test/known-case");
          return { docketNumber: candidate.docketNumber, courtId: "ilnd", entries: [] };
        }
      } });
    assert.equal((await service.enrichCaseWithSignalFeed(saved.id, { force: true })).reason, "identity-mismatch");
  } finally { store.db.close(); fs.rmSync(directory, { recursive: true, force: true }); }
});

test("coverage counts canonical docket items instead of raw source rows", () => {
  const { directory, store } = createStore();
  try {
    const saved = store.upsertCase(targetCase());
    store.upsertDocketEntry(docketEntry(saved.id, "courtlistener", 1, "Official filing"));
    store.upsertDocketEntry(docketEntry(saved.id, PRIORITY_FEED_ENTRY_SOURCE, 1, "Mirror filing"));

    const coverage = store.getEntryCoverageForCaseIds([saved.id]).get(saved.id);
    assert.equal(coverage.rawEntries, 2);
    assert.equal(coverage.totalEntries, 1);
    assert.equal(store.getCase(saved.id).entries.length, 1);
    assert.equal(store.getCase(saved.id).entries[0].primary_source, "courtlistener");
  } finally {
    store.db.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test("TroHub full text replaces a title-only cross-source docket row", () => {
  const { directory, store } = createStore();
  try {
    const saved = store.upsertCase(targetCase({
      docket_number: "1:26-cv-26215",
      source_case_key: "courtlistener:flsd:1:26-cv-26215",
      court_id: "flsd",
      court_name: "Southern District of Florida"
    }));
    const officialTitleOnly = docketEntry(saved.id, "courtlistener", 9, "Order on Motion for Miscellaneous Relief");
    officialTitleOnly.filed_at = "2026-09-10";
    store.upsertDocketEntry(officialTitleOnly);
    store.upsertDocketEntry({
      case_id: saved.id,
      source_entry_key: "signal-feed:rendered:2026-09-10:order-on-motion-for-miscellaneous-relief:1",
      primary_source: "signal-feed",
      source_entry_id: "rendered:2026-09-10:order-on-motion-for-miscellaneous-relief:1",
      document_type: "Docket Entry",
      entry_number: null,
      document_number: null,
      filed_at: "2026-09-10",
      description: "Order on Motion for Miscellaneous Relief. Granting the motion and directing the clerk to enter the attached order.",
      absolute_url: "https://www.trohub.com/en/cases/example",
      is_available: 0,
      raw: {}
    });

    const detail = store.getCase(saved.id);
    assert.equal(detail.entries.length, 1);
    assert.match(detail.entries[0].description, /directing the clerk to enter the attached order/);
    assert.equal(detail.entries[0].primary_source, "signal-feed");
  } finally {
    store.db.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test("priority timeline refresh preserves complementary docket sources", async () => {
  const { directory, store } = createStore();
  try {
    const saved = store.upsertCase(targetCase());
    store.upsertDocketEntry(docketEntry(saved.id, "courtlistener", 3, "Later official filing"));

    const service = new CaseSyncService({
      config: {},
      store,
      priorityFeed: {
        async enrichCase() {
          return {
            url: "https://example.test/archive/case",
            title: "Archived timeline",
            entries: [
              { row_number: "2", filed_at: "2026-04-12T12:00:00.000Z", description: "Second filing" },
              { row_number: "1", filed_at: "2026-04-11T12:00:00.000Z", description: "First filing" }
            ]
          };
        }
      }
    });

    const result = await service.syncSinglePriorityFeedCase(store.getCase(saved.id));
    assert.equal(result.enriched, true);
    assert.deepEqual(
      store.getCase(saved.id).entries.map((entry) => Number(entry.document_number)).sort((a, b) => a - b),
      [1, 2, 3]
    );
    assert.equal(
      store.db.prepare("SELECT COUNT(*) AS count FROM docket_entries WHERE case_id = ? AND primary_source = 'courtlistener'").get(saved.id).count,
      1
    );
  } finally {
    store.db.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test("cases with an archived timeline remain eligible for gap completion", () => {
  const { directory, store } = createStore();
  try {
    const saved = store.upsertCase(targetCase({
      raw: {
        priorityFeed: {
          rowCount: 5,
          syncedAt: "2026-08-25T00:00:00.000Z"
        }
      }
    }));
    store.upsertDocketEntry(docketEntry(saved.id, PRIORITY_FEED_ENTRY_SOURCE, 1));

    const candidates = store.getCasesNeedingPacerMonitorSync(10, {
      startDate: "2026-01-01",
      recentWindowDays: 365,
      staleAfterHours: 1,
      blockedRetryAfterHours: 1,
      notFoundRetryAfterHours: 1
    });

    assert.ok(candidates.some((candidate) => candidate.id === saved.id));
  } finally {
    store.db.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test("polluted legacy tags do not spend completion budget on unrelated civil cases", () => {
  const { directory, store } = createStore();
  try {
    const saved = store.upsertCase(targetCase({
      source_case_key: "courtlistener:generic:3:26-cv-04483",
      courtlistener_docket_id: 70000001,
      court_id: "njd",
      court_name: "District of New Jersey",
      case_name: "Watts v. CBRE, Inc.",
      docket_number: "3:26-cv-04483",
      cause: "42:1983 Civil Rights Act",
      nature_of_suit: "440 Civil Rights: Other",
      source_urls: ["https://www.courtlistener.com/docket/70000001/example/"],
      raw: {
        priorityFeed: {
          rowCount: 0,
          missing: true,
          syncedAt: "2026-08-25T00:00:00.000Z"
        }
      }
    }));
    assert.ok(saved);

    const candidates = store.getCasesNeedingPacerMonitorSync(10, {
      startDate: "2026-01-01",
      recentWindowDays: 365,
      staleAfterHours: 1
    });
    assert.ok(!candidates.some((candidate) => candidate.id === saved.id));
  } finally {
    store.db.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test("search preset labels are hints rather than automatic watchlist evidence", () => {
  const unrelated = {
    caseName: "Watts v. CBRE, Inc.",
    cause: "42:1983 Civil Rights Act",
    party: ["Watts", "CBRE, Inc."],
    recap_documents: []
  };

  assert.deepEqual(classifyCase(unrelated, ["tro", "schedule_a"]), []);
});

test("official court feed discovery can rotate across all district courts", () => {
  const client = new CourtFeedClient({
    enabled: true,
    courts: ["all"],
    maxFeedsPerRun: 8
  });

  assert.equal(client.trackAllDistrictCourts, true);
  assert.equal(client.maxFeedsPerRun, 8);
  assert.ok(client.listFeeds().some((feed) => feed.id === "pawd"));
  assert.ok(client.listFeeds().length > 3);
});

test("crawler docket matching uses indexed lookup instead of hydrating the full case table", () => {
  const { directory, store } = createStore();
  try {
    const saved = store.upsertCase(targetCase());
    store.getHydratedCases = () => {
      throw new Error("full case hydration must not run during crawler matching");
    };

    assert.equal(
      store.findCaseByCourtAndDocket({
        courtId: "pawd",
        docketNumber: "2:26-cv-00651",
        startDate: "2026-01-01"
      })?.id,
      saved.id
    );
    assert.equal(store.findCaseByDocketNumber("2:26-cv-00651", "2026-01-01")?.id, saved.id);
    assert.deepEqual(store.listActiveTargetCourtIds("2026-01-01"), ["pawd"]);
  } finally {
    store.db.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test("discovery follow-up is persisted for the isolated enrichment worker", async () => {
  const { directory, store } = createStore();
  try {
    const saved = store.upsertCase(targetCase());
    const service = new CaseSyncService({
      config: {
        sync: {
          discoveryStartDate: "2026-01-01",
          discoveryEnrichmentProviders: ["courtlistener", "pacermonitor"],
          discoveryEnrichmentDelayMs: 60_000,
          webhookEnrichmentMaxPendingJobs: 10
        },
        courtFeeds: { crossSourceFollowUpMaxCasesPerRun: 0 }
      },
      store,
      courtFeeds: { maxLookupsPerRun: 1 }
    });
    service.importLookup = async () => {
      throw new Error("discovery must not enrich inline");
    };

    const result = await service.lookupDiscoveryCandidates(new Map([
      ["pawd|26cv00651", {
        docketNumber: "2:26-cv-00651",
        courtName: "Western District of Pennsylvania",
        caseName: "Example Brand v. Schedule A Defendants",
        priority: 0,
        filedAt: "2026-08-25T00:00:00.000Z"
      }]
    ]), 1);

    const queue = store.getCheckpoint("system:webhook-enrichment-queue");
    assert.equal(result.lookupsTriggered, 0);
    assert.equal(result.enrichmentsQueued, 1);
    assert.equal(queue.pending.length, 1);
    assert.equal(queue.pending[0].caseId, saved.id);
    assert.deepEqual(queue.pending[0].providers, ["courtlistener", "pacermonitor"]);
  } finally {
    store.db.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test("docket completion scheduler queues bounded candidates and honors cooldown", () => {
  const { directory, store } = createStore();
  try {
    const saved = store.upsertCase(targetCase({
      docket_count: 20,
      latest_docket_number: "20",
      last_filing_at: "2026-08-25T00:00:00.000Z"
    }));
    store.upsertDocketEntry(docketEntry(saved.id, "courtlistener", 1));
    const service = new CaseSyncService({
      config: {
        sync: {
          discoveryStartDate: "2026-01-01",
          docketCompletionSchedulerEnabled: true,
          docketCompletionMaxCasesPerRun: 1,
          docketCompletionRetryDelayMs: 6 * 60 * 60 * 1000,
          docketCompletionQueueDelayMs: 15_000,
          docketCompletionRecentWindowDays: 365,
          docketCompletionProviders: ["signal-feed", "courtlistener"],
          webhookEnrichmentMaxPendingJobs: 10
        }
      },
      store
    });

    const first = service.enqueueDocketCompletionCandidates();
    assert.equal(first.enrichmentsQueued, 1);
    assert.deepEqual(first.queuedCaseIds, [saved.id]);

    store.updateCheckpointAtomically("system:webhook-enrichment-queue", () => null);
    const second = service.enqueueDocketCompletionCandidates();
    assert.equal(second.enrichmentsQueued, 0);
    assert.ok(second.excludedCaseCount >= 1);
  } finally {
    store.db.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
});
