import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { Store } from "../src/db.js";

function createTempStore() {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "tro-watch-store-"));
  const dbPath = path.join(tempDir, "test.sqlite");
  const store = new Store(dbPath);
  return {
    store,
    cleanup() {
      try {
        store.db.close();
      } catch {
        // ignore cleanup failures in tests
      }
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  };
}

test("upsertCase keeps better case metadata while merging source raw data", () => {
  const { store, cleanup } = createTempStore();

  try {
    const saved = store.upsertCase({
      source_case_key: "case:ilnd:26-cv-00011",
      primary_source: "courtlistener",
      source_case_id: "72099599",
      court_id: "ilnd",
      court_name: "District Court, N.D. Illinois",
      case_name: "Bose Corporation v. The Partnerships Identified on Schedule A",
      docket_number: "1:26-cv-00011",
      plaintiffs: ["Bose Corporation"],
      raw: {
        courtlistener: {
          docketUrl: "https://www.courtlistener.com/docket/72099599/"
        }
      }
    });

    const merged = store.upsertCase({
      source_case_key: "case:ilnd:26-cv-00011",
      primary_source: "61tro",
      source_case_id: "https://61tro.com/view/id/ilnd-1%3A2026-cv-00011.html",
      court_id: "ilnd",
      court_name: "Northern District of Illinois",
      case_name: "XYZ Corporation v. The Partnerships Identified On Schedule A",
      docket_number: "26-cv-00011",
      raw: {
        law_firm_sites: {
          "61tro": {
            caseUrl: "https://61tro.com/view/id/ilnd-1%3A2026-cv-00011.html",
            entryCount: 67
          }
        }
      }
    });

    assert.ok(saved?.id);
    assert.equal(merged.case_name, "Bose Corporation v. The Partnerships Identified on Schedule A");
    assert.equal(merged.docket_number, "1:26-cv-00011");
    assert.equal(merged.court_name, "Northern District of Illinois");
    assert.equal(merged.primary_source, "courtlistener");
    assert.equal(merged.raw?.courtlistener?.docketUrl, "https://www.courtlistener.com/docket/72099599/");
    assert.equal(
      merged.raw?.law_firm_sites?.["61tro"]?.caseUrl,
      "https://61tro.com/view/id/ilnd-1%3A2026-cv-00011.html"
    );
  } finally {
    cleanup();
  }
});

test("upsertDocketEntry collapses duplicate source entries and keeps richer text", () => {
  const { store, cleanup } = createTempStore();

  try {
    const savedCase = store.upsertCase({
      source_case_key: "case:ilnd:26-cv-00011",
      primary_source: "courtlistener",
      source_case_id: "72099599",
      court_id: "ilnd",
      court_name: "Northern District of Illinois",
      case_name: "Bose Corporation v. The Partnerships Identified on Schedule A",
      docket_number: "1:26-cv-00011",
      date_filed: "2026-01-02"
    });

    store.upsertDocketEntry({
      case_id: savedCase.id,
      source_entry_key: "61tro:old-detail:entry-1",
      primary_source: "61tro",
      source_entry_id: "2026-04-03:minute-entry",
      filed_at: "2026-04-03",
      description: "MINUTE entry before the Honorable Mary M. Rowland",
      absolute_url: "https://61tro.com/detail/8739.html"
    });

    const mergedEntry = store.upsertDocketEntry({
      case_id: savedCase.id,
      source_entry_key: "61tro:view-id:entry-1",
      primary_source: "61tro",
      source_entry_id: "2026-04-03:minute-entry",
      filed_at: "2026-04-03",
      description: "MINUTE entry before the Honorable Mary M. Rowland\n附件：1:(Text of Proposed Order)",
      absolute_url: "https://61tro.com/view/id/ilnd-1%3A2026-cv-00011.html"
    });

    const count = store.db.prepare("SELECT COUNT(*) AS n FROM docket_entries WHERE case_id = ?").get(savedCase.id)?.n || 0;
    assert.equal(count, 1);
    assert.equal(mergedEntry.source_entry_key, "61tro:view-id:entry-1");
    assert.match(mergedEntry.description, /附件：1:\(Text of Proposed Order\)/);
  } finally {
    cleanup();
  }
});

test("dedupeStoredDocketEntries removes historical duplicates and recomputes case counts exactly", async () => {
  const { store, cleanup } = createTempStore();

  try {
    const savedCase = store.upsertCase({
      source_case_key: "case:ilnd:26-cv-00011",
      primary_source: "61tro",
      source_case_id: "https://61tro.com/view/id/ilnd-1%3A2026-cv-00011.html",
      court_id: "ilnd",
      court_name: "Northern District of Illinois",
      case_name: "Bose Corporation v. The Partnerships Identified on Schedule A",
      docket_number: "1:26-cv-00011",
      date_filed: "2026-01-02",
      docket_count: 4
    });

    store.upsertDocketEntry({
      case_id: savedCase.id,
      source_entry_key: "61tro:detail:1",
      primary_source: "61tro",
      source_entry_id: "2026-04-03:minute-entry",
      filed_at: "2026-04-03",
      description: "MINUTE entry before the Honorable Mary M. Rowland"
    });
    store.upsertDocketEntry({
      case_id: savedCase.id,
      source_entry_key: "61tro:detail:2",
      primary_source: "61tro",
      source_entry_id: "2026-04-04:notice-dismissal",
      filed_at: "2026-04-04",
      description: "NOTICE of Voluntary Dismissal by Bose Corporation"
    });

    store.db.prepare(`
      INSERT INTO docket_entries (
        case_id,
        source_entry_key,
        primary_source,
        source_entry_id,
        document_type,
        entry_number,
        document_number,
        filed_at,
        description,
        description_zh,
        absolute_url,
        is_available,
        page_count,
        pacer_doc_id,
        raw_json,
        last_synced_at,
        created_at,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      savedCase.id,
      "61tro:view:duplicate",
      "61tro",
      "2026-04-03:minute-entry",
      "Docket Entry",
      null,
      null,
      "2026-04-03",
      "MINUTE entry before the Honorable Mary M. Rowland\n附件：1:(Text of Proposed Order)",
      null,
      null,
      0,
      null,
      null,
      "{}",
      new Date().toISOString(),
      new Date().toISOString(),
      new Date().toISOString()
    );

    const result = await store.dedupeStoredDocketEntries({ caseId: savedCase.id });
    const count = store.db.prepare("SELECT COUNT(*) AS n FROM docket_entries WHERE case_id = ?").get(savedCase.id)?.n || 0;
    const refreshedCase = store.getCase(savedCase.id);

    assert.equal(result.groupsProcessed, 1);
    assert.equal(result.entriesDeleted, 1);
    assert.equal(count, 2);
    assert.equal(refreshedCase.docket_count, 2);
  } finally {
    cleanup();
  }
});

test("reapStaleSyncRuns skips SQLITE_BUSY rows without crashing the app", () => {
  const { store, cleanup } = createTempStore();

  try {
    const runId = store.claimSyncRun("system", "recent");
    assert.ok(runId);

    const staleStartedAt = new Date(Date.now() - 10 * 60 * 1000).toISOString();
    store.db.prepare("UPDATE sync_runs SET started_at = ? WHERE id = ?").run(staleStartedAt, runId);

    const originalFinishSyncRun = store.finishSyncRun.bind(store);
    store.finishSyncRun = () => {
      const error = new Error("database is locked");
      error.code = "ERR_SQLITE_ERROR";
      error.errcode = 5;
      throw error;
    };

    const reaped = store.reapStaleSyncRuns("system", {
      mode: "recent",
      heartbeatTimeoutMs: 1,
      maxRuntimeMs: 1,
      reasonPrefix: "recent watchdog auto-cleared stale run"
    });

    const savedRun = store.getSyncRun(runId);

    assert.deepEqual(reaped, []);
    assert.equal(savedRun?.status, "running");

    store.finishSyncRun = originalFinishSyncRun;
  } finally {
    cleanup();
  }
});
