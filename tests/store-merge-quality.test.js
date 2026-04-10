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

test("getCase collapses equivalent cross-source docket entries into one richer timeline item", () => {
  const { store, cleanup } = createTempStore();

  try {
    const savedCase = store.upsertCase({
      source_case_key: "case:ilnd:26-cv-00011",
      primary_source: "courtlistener",
      source_case_id: "72099599",
      courtlistener_docket_id: 72099599,
      court_id: "ilnd",
      court_name: "Northern District of Illinois",
      case_name: "Bose Corporation v. The Partnerships Identified on Schedule A",
      docket_number: "1:26-cv-00011",
      date_filed: "2026-01-02"
    });

    store.upsertDocketEntry({
      case_id: savedCase.id,
      source_entry_key: "courtlistener:140",
      primary_source: "courtlistener",
      source_entry_id: "2208776613",
      filed_at: "2026-04-02",
      entry_number: "62",
      document_number: "62",
      description: "MOTION by Defendants Fableter, SunEaseleetwo, SunnyBrewonthree, asdgh, hsdakldh, muyuanbaihuotwo for extension of time",
      absolute_url: "https://www.courtlistener.com/docket/72099599/62/"
    });

    store.upsertDocketEntry({
      case_id: savedCase.id,
      source_entry_key: "61tro:motion-extend-time",
      primary_source: "61tro",
      source_entry_id: "61tro-motion-extend-time",
      filed_at: "2026-04-02",
      description: "MOTION by Defendants Fableter, SunEaseleetwo, SunnyBrewonthree, asdgh, hsdakldh, muyuanbaihuotwo for extension of time 翻译 附件： 1:(Text of Proposed Order)",
      absolute_url: "https://61tro.com/view/id/ilnd-1%3A2026-cv-00011.html"
    });

    const detail = store.getCase(savedCase.id);

    assert.equal(detail.entries.length, 1);
    assert.match(detail.entries[0].description, /Text of Proposed Order/);
    assert.equal(detail.entries[0].primary_source, "courtlistener");
    assert.deepEqual(detail.entries[0].raw?.merged_sources, ["courtlistener", "61tro"]);
  } finally {
    cleanup();
  }
});

test("restoreMissingFromBackup restores missing cases and docket entries by source keys", async () => {
  const backupDir = fs.mkdtempSync(path.join(os.tmpdir(), "tro-watch-backup-"));
  const liveDir = fs.mkdtempSync(path.join(os.tmpdir(), "tro-watch-live-"));
  const backupDbPath = path.join(backupDir, "backup.sqlite");
  const liveDbPath = path.join(liveDir, "live.sqlite");
  const backupStore = new Store(backupDbPath);
  const liveStore = new Store(liveDbPath);

  try {
    const existingCase = backupStore.upsertCase({
      source_case_key: "case:ilnd:26-cv-00011",
      primary_source: "courtlistener",
      source_case_id: "72099599",
      court_id: "ilnd",
      court_name: "Northern District of Illinois",
      case_name: "Bose Corporation v. The Partnerships Identified on Schedule A",
      docket_number: "1:26-cv-00011",
      date_filed: "2026-01-02"
    });
    backupStore.upsertDocketEntry({
      case_id: existingCase.id,
      source_entry_key: "courtlistener:62",
      primary_source: "courtlistener",
      source_entry_id: "2208776613",
      filed_at: "2026-04-02",
      description: "MOTION by Defendants for extension of time"
    });

    const missingCase = backupStore.upsertCase({
      source_case_key: "case:ilnd:26-cv-00430",
      primary_source: "courtlistener",
      source_case_id: "73000430",
      court_id: "ilnd",
      court_name: "Northern District of Illinois",
      case_name: "Example Brand LLC v. Schedule A Defendants",
      docket_number: "1:26-cv-00430",
      date_filed: "2026-02-12"
    });
    backupStore.upsertDocketEntry({
      case_id: missingCase.id,
      source_entry_key: "courtlistener:430:1",
      primary_source: "courtlistener",
      source_entry_id: "430-entry-1",
      filed_at: "2026-02-12",
      description: "COMPLAINT filed"
    });
    backupStore.upsertDocketEntry({
      case_id: missingCase.id,
      source_entry_key: "courtlistener:430:2",
      primary_source: "courtlistener",
      source_entry_id: "430-entry-2",
      filed_at: "2026-02-13",
      description: "MOTION for temporary restraining order"
    });

    liveStore.upsertCase({
      source_case_key: existingCase.source_case_key,
      primary_source: existingCase.primary_source,
      source_case_id: existingCase.source_case_id,
      court_id: existingCase.court_id,
      court_name: existingCase.court_name,
      case_name: existingCase.case_name,
      docket_number: existingCase.docket_number,
      date_filed: existingCase.date_filed
    });
    liveStore.upsertDocketEntry({
      case_id: 1,
      source_entry_key: "courtlistener:62",
      primary_source: "courtlistener",
      source_entry_id: "2208776613",
      filed_at: "2026-04-02",
      description: "MOTION by Defendants for extension of time"
    });

    const dryRun = await liveStore.restoreMissingFromBackup({
      sourceDbPath: backupDbPath,
      dryRun: true
    });
    assert.equal(dryRun.missingCaseCount, 1);
    assert.equal(dryRun.selectedCaseCount, 1);
    assert.equal(dryRun.missingEntryCount, 2);

    const restored = await liveStore.restoreMissingFromBackup({
      sourceDbPath: backupDbPath
    });
    assert.equal(restored.restoredCases, 1);
    assert.equal(restored.restoredEntries, 2);

    const restoredCase = liveStore.db
      .prepare("SELECT * FROM cases WHERE source_case_key = ? LIMIT 1")
      .get("case:ilnd:26-cv-00430");
    const restoredEntries = liveStore.db
      .prepare("SELECT COUNT(*) AS n FROM docket_entries WHERE case_id = ?")
      .get(restoredCase.id)?.n || 0;

    assert.equal(restoredCase.docket_number, "1:26-cv-00430");
    assert.equal(restoredEntries, 2);
  } finally {
    try {
      backupStore.db.close();
    } catch {
      // ignore cleanup failures in tests
    }
    try {
      liveStore.db.close();
    } catch {
      // ignore cleanup failures in tests
    }
    fs.rmSync(backupDir, { recursive: true, force: true });
    fs.rmSync(liveDir, { recursive: true, force: true });
  }
});

test("restoreMissingFromBackup can limit recovery to currently retained missing cases", async () => {
  const backupDir = fs.mkdtempSync(path.join(os.tmpdir(), "tro-watch-retained-backup-"));
  const liveDir = fs.mkdtempSync(path.join(os.tmpdir(), "tro-watch-retained-live-"));
  const backupDbPath = path.join(backupDir, "backup.sqlite");
  const liveDbPath = path.join(liveDir, "live.sqlite");
  const backupStore = new Store(backupDbPath);
  const liveStore = new Store(liveDbPath);
  const timestamp = new Date().toISOString();

  try {
    const retainedCase = backupStore.upsertCase({
      source_case_key: "case:ilnd:26-cv-00430",
      primary_source: "courtlistener",
      source_case_id: "73000430",
      court_id: "ilnd",
      court_name: "Northern District of Illinois",
      case_name: "Example Brand LLC v. The Partnerships Identified on Schedule A",
      docket_number: "1:26-cv-00430",
      date_filed: "2026-02-12",
      tags_marker: "|tro|schedule_a|seller_tro|",
      is_watchlist: 1,
      is_tro: 1,
      is_schedule_a: 1,
      is_seller_watch: 1
    });
    backupStore.upsertDocketEntry({
      case_id: retainedCase.id,
      source_entry_key: "courtlistener:430:1",
      primary_source: "courtlistener",
      source_entry_id: "430-entry-1",
      filed_at: "2026-02-12",
      description: "COMPLAINT filed"
    });

    backupStore.db.prepare(`
      INSERT INTO cases (
        source_case_key,
        primary_source,
        source_case_id,
        court_id,
        court_name,
        case_name,
        docket_number,
        date_filed,
        tags_marker,
        is_watchlist,
        is_tro,
        is_schedule_a,
        is_seller_watch,
        priority_feed_row_count,
        raw_json,
        created_at,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      "case:nysd:26-cv-10191",
      "courtlistener",
      "7310191",
      "nysd",
      "Southern District of New York",
      "Ordinary Contract Dispute LLC v. Example Inc.",
      "1:26-cv-10191",
      "2026-02-14",
      "",
      0,
      0,
      0,
      0,
      0,
      "{}",
      timestamp,
      timestamp
    );
    const filteredCase = backupStore.db
      .prepare("SELECT id FROM cases WHERE source_case_key = ? LIMIT 1")
      .get("case:nysd:26-cv-10191");
    backupStore.upsertDocketEntry({
      case_id: filteredCase.id,
      source_entry_key: "courtlistener:10191:1",
      primary_source: "courtlistener",
      source_entry_id: "10191-entry-1",
      filed_at: "2026-02-14",
      description: "COMPLAINT filed"
    });

    const dryRun = await liveStore.restoreMissingFromBackup({
      sourceDbPath: backupDbPath,
      dryRun: true,
      retainedOnly: true
    });
    assert.equal(dryRun.missingCaseCount, 2);
    assert.equal(dryRun.retainedOnly, true);
    assert.equal(dryRun.retainedByCurrentRules, 1);
    assert.equal(dryRun.selectedCaseCount, 1);
    assert.equal(dryRun.missingEntryCount, 1);

    const restored = await liveStore.restoreMissingFromBackup({
      sourceDbPath: backupDbPath,
      retainedOnly: true
    });
    assert.equal(restored.restoredCases, 1);
    assert.equal(restored.restoredEntries, 1);

    const retainedRestored = liveStore.db
      .prepare("SELECT COUNT(*) AS n FROM cases WHERE source_case_key = ?")
      .get("case:ilnd:26-cv-00430")?.n || 0;
    const filteredRestored = liveStore.db
      .prepare("SELECT COUNT(*) AS n FROM cases WHERE source_case_key = ?")
      .get("case:nysd:26-cv-10191")?.n || 0;

    assert.equal(retainedRestored, 1);
    assert.equal(filteredRestored, 0);
  } finally {
    try {
      backupStore.db.close();
    } catch {
      // ignore cleanup failures in tests
    }
    try {
      liveStore.db.close();
    } catch {
      // ignore cleanup failures in tests
    }
    fs.rmSync(backupDir, { recursive: true, force: true });
    fs.rmSync(liveDir, { recursive: true, force: true });
  }
});

test("inspectMissingFromBackup summarizes reasons and flags for missing cases", () => {
  const backupDir = fs.mkdtempSync(path.join(os.tmpdir(), "tro-watch-inspect-backup-"));
  const liveDir = fs.mkdtempSync(path.join(os.tmpdir(), "tro-watch-inspect-live-"));
  const backupDbPath = path.join(backupDir, "backup.sqlite");
  const liveDbPath = path.join(liveDir, "live.sqlite");
  const backupStore = new Store(backupDbPath);
  const liveStore = new Store(liveDbPath);
  const timestamp = new Date().toISOString();

  try {
    backupStore.db.prepare(`
      INSERT INTO cases (
        source_case_key,
        primary_source,
        source_case_id,
        court_id,
        court_name,
        case_name,
        docket_number,
        date_filed,
        tags_marker,
        is_watchlist,
        is_tro,
        is_schedule_a,
        is_seller_watch,
        priority_feed_row_count,
        raw_json,
        created_at,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      "case:nysd:26-cv-10191",
      "courtlistener",
      "7310191",
      "nysd",
      "Southern District of New York",
      "Ordinary Contract Dispute LLC v. Example Inc.",
      "1:26-cv-10191",
      "2026-02-14",
      "",
      0,
      0,
      0,
      0,
      0,
      "{}",
      timestamp,
      timestamp
    );

    const result = liveStore.inspectMissingFromBackup({
      sourceDbPath: backupDbPath
    });

    assert.equal(result.missingCaseCount, 1);
    assert.equal(result.inspectedCaseCount, 1);
    assert.equal(result.retainedByCurrentRules, 0);
    assert.equal(result.reasonCounts["general-civil"], 1);
    assert.equal(result.primarySourceCounts.courtlistener, 1);
    assert.equal(result.watchlistFlagCount, 0);
  } finally {
    try {
      backupStore.db.close();
    } catch {
      // ignore cleanup failures in tests
    }
    try {
      liveStore.db.close();
    } catch {
      // ignore cleanup failures in tests
    }
    fs.rmSync(backupDir, { recursive: true, force: true });
    fs.rmSync(liveDir, { recursive: true, force: true });
  }
});

test("inspectMissingFromBackup can focus on retained missing cases only", () => {
  const backupDir = fs.mkdtempSync(path.join(os.tmpdir(), "tro-watch-inspect-retained-backup-"));
  const liveDir = fs.mkdtempSync(path.join(os.tmpdir(), "tro-watch-inspect-retained-live-"));
  const backupDbPath = path.join(backupDir, "backup.sqlite");
  const liveDbPath = path.join(liveDir, "live.sqlite");
  const backupStore = new Store(backupDbPath);
  const liveStore = new Store(liveDbPath);
  const timestamp = new Date().toISOString();

  try {
    backupStore.db.prepare(`
      INSERT INTO cases (
        source_case_key,
        primary_source,
        source_case_id,
        court_id,
        court_name,
        case_name,
        docket_number,
        date_filed,
        tags_marker,
        is_watchlist,
        is_tro,
        is_schedule_a,
        is_seller_watch,
        priority_feed_row_count,
        raw_json,
        created_at,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      "case:ilnd:26-cv-00430",
      "courtlistener",
      "73000430",
      "ilnd",
      "Northern District of Illinois",
      "Example Brand LLC v. The Partnerships Identified on Schedule A",
      "1:26-cv-00430",
      "2026-02-12",
      "|tro|schedule_a|seller_tro|",
      1,
      1,
      1,
      1,
      0,
      "{}",
      timestamp,
      timestamp
    );
    backupStore.db.prepare(`
      INSERT INTO cases (
        source_case_key,
        primary_source,
        source_case_id,
        court_id,
        court_name,
        case_name,
        docket_number,
        date_filed,
        tags_marker,
        is_watchlist,
        is_tro,
        is_schedule_a,
        is_seller_watch,
        priority_feed_row_count,
        raw_json,
        created_at,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      "case:nysd:26-cv-10191",
      "courtlistener",
      "7310191",
      "nysd",
      "Southern District of New York",
      "Ordinary Contract Dispute LLC v. Example Inc.",
      "1:26-cv-10191",
      "2026-02-14",
      "",
      0,
      0,
      0,
      0,
      0,
      "{}",
      timestamp,
      timestamp
    );

    const result = liveStore.inspectMissingFromBackup({
      sourceDbPath: backupDbPath,
      retainedOnly: true
    });

    assert.equal(result.missingCaseCount, 2);
    assert.equal(result.inspectedCaseCount, 1);
    assert.equal(result.retainedOnly, true);
    assert.equal(result.retainedByCurrentRules, 1);
    assert.equal(result.primarySourceCounts.courtlistener, 1);
    assert.equal(result.reasonCounts["general-civil"] || 0, 0);
    assert.equal(result.watchlistFlagCount, 1);
  } finally {
    try {
      backupStore.db.close();
    } catch {
      // ignore cleanup failures in tests
    }
    try {
      liveStore.db.close();
    } catch {
      // ignore cleanup failures in tests
    }
    fs.rmSync(backupDir, { recursive: true, force: true });
    fs.rmSync(liveDir, { recursive: true, force: true });
  }
});

test("getCase keeps distinct same-day cross-source docket entries separate", () => {
  const { store, cleanup } = createTempStore();

  try {
    const savedCase = store.upsertCase({
      source_case_key: "case:ilnd:26-cv-00011",
      primary_source: "courtlistener",
      source_case_id: "72099599",
      courtlistener_docket_id: 72099599,
      court_id: "ilnd",
      court_name: "Northern District of Illinois",
      case_name: "Bose Corporation v. The Partnerships Identified on Schedule A",
      docket_number: "1:26-cv-00011",
      date_filed: "2026-01-02"
    });

    store.upsertDocketEntry({
      case_id: savedCase.id,
      source_entry_key: "courtlistener:minute-dismiss-52",
      primary_source: "courtlistener",
      source_entry_id: "cl-minute-52",
      filed_at: "2026-03-26",
      description: "MINUTE entry before the Honorable Mary M. Rowland: Plaintiff's response to Defendant's motion [52] to dismiss is due by 4/23/26; reply due by 5/14/26."
    });

    store.upsertDocketEntry({
      case_id: savedCase.id,
      source_entry_key: "61tro:minute-extension-54",
      primary_source: "61tro",
      source_entry_id: "61tro-minute-54",
      filed_at: "2026-03-26",
      description: "MINUTE entry before the Honorable Mary M. Rowland: The Court grants Defendant JusiOriginal's unopposed motion for extension of time [54]. Defendant JusiOriginal to answer or otherwise plead by 4/9/26."
    });

    const detail = store.getCase(savedCase.id);

    assert.equal(detail.entries.length, 2);
  } finally {
    cleanup();
  }
});

test("rebuildCaseFastPathColumns restores case search and category flags from stored case rows", () => {
  const { store, cleanup } = createTempStore();

  try {
    const savedCase = store.upsertCase({
      source_case_key: "case:ilnd:25-cv-10191",
      primary_source: "courtlistener",
      source_case_id: "72099599",
      court_id: "ilnd",
      court_name: "Northern District of Illinois",
      case_name: "Acme Corporation v. The Partnerships Identified on Schedule A",
      docket_number: "1:25-cv-10191",
      date_filed: "2025-11-02",
      tags_marker: "|tro|schedule_a|seller_tro|",
      plaintiffs: ["Acme Corporation"],
      recent_activity_summary: "Temporary restraining order entered",
      raw: {
        priorityFeed: {
          lawFirm: "Sriplaw"
        }
      }
    });

    store.db.exec(`
      UPDATE cases
      SET search_text = NULL,
          priority_activity_at = NULL,
          is_watchlist = NULL,
          is_tro = NULL,
          is_schedule_a = NULL,
          is_seller_watch = NULL,
          priority_feed_row_count = NULL
    `);

    const rebuild = store.rebuildCaseFastPathColumns();
    const payload = store.listCases({
      startDate: "2025-01-01",
      category: "seller_watch",
      page: 1,
      pageSize: 25,
      search: "sriplaw",
      court: ""
    });
    const row = store.db.prepare(`
      SELECT search_text, priority_activity_at, is_watchlist, is_tro, is_schedule_a, is_seller_watch
      FROM cases
      WHERE id = ?
    `).get(savedCase.id);

    assert.equal(rebuild.updatedCases, 1);
    assert.equal(typeof rebuild.rebuiltFts, "boolean");
    assert.equal(payload.total, 1);
    assert.equal(payload.items[0].id, savedCase.id);
    assert.match(String(row.search_text || ""), /sriplaw/);
    assert.equal(row.is_watchlist, 1);
    assert.equal(row.is_tro, 1);
    assert.equal(row.is_schedule_a, 1);
    assert.equal(row.is_seller_watch, 1);
    assert.ok(row.priority_activity_at);
  } finally {
    cleanup();
  }
});

test("cleanupWindowEmailArtifacts deletes legacy 3-hour report checkpoints and sync runs", () => {
  const { store, cleanup } = createTempStore();

  try {
    store.saveCheckpoint("window-email-report:Asia/Shanghai:3:foo:test@example.com", { sent: true });
    store.db.prepare(`
      INSERT INTO sync_runs (
        provider,
        mode,
        status,
        started_at,
        finished_at,
        stats_json,
        error_text
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(
      "window-email",
      "report-3h",
      "succeeded",
      new Date().toISOString(),
      new Date().toISOString(),
      "{}",
      null
    );

    const result = store.cleanupWindowEmailArtifacts();
    const remainingCheckpoint = store.getCheckpoint("window-email-report:Asia/Shanghai:3:foo:test@example.com");
    const remainingRuns = Number(
      store.db.prepare(`SELECT COUNT(*) AS n FROM sync_runs WHERE provider = 'window-email'`).get()?.n || 0
    );

    assert.equal(result.checkpointsDeleted, 1);
    assert.equal(result.syncRunsDeleted, 1);
    assert.equal(remainingCheckpoint, null);
    assert.equal(remainingRuns, 0);
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
