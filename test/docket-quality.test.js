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
