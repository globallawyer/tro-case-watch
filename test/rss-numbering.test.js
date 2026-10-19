import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Store } from "../src/db.js";
import { getDocketSequenceValue } from "../src/docket-sequence.js";
import { getDocketContentStatus } from "../src/docket-content.js";

function withStore(run) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "tro-rss-numbering-"));
  const store = new Store(path.join(dir, "test.sqlite"));
  try {
    const saved = store.upsertCase({ source_case_key: "rss-numbering", source_case_id: "rss-numbering", primary_source: "worldtro", court_id: "flsd",
      docket_number: "1:26-cv-27217", case_name: "Example Brand v. Schedule A Defendants", tags_marker: "|tro|schedule_a|" });
    run(store, saved.id);
  } finally { store.db.close(); fs.rmSync(dir, { recursive: true, force: true }); }
}

function insert(store, id, key, number, description, date = null, source = "worldtro") {
  return store.upsertDocketEntry({ case_id: id, source_entry_key: key, primary_source: source,
    entry_number: number, document_number: number, description, filed_at: date, raw: { row_number: number } });
}

test("mirror RSS notices use an explicit document reference, never a table position", () => {
  const entry = { primary_source: "worldtro", entry_number: "5", description: "Form AO 120/121（法院 RSS 更新，第 3 条）" };
  assert.equal(getDocketSequenceValue(entry), "3");
  assert.equal(getDocketContentStatus(entry), "title-only");
  const marker = { ...entry, description: "[court-rss:abcd] RSS update: [Order]" };
  assert.equal(getDocketSequenceValue(marker), "");
  assert.equal(getDocketSequenceValue({ ...marker, raw: { documentNumber: "17" } }), "17");
  assert.equal(getDocketSequenceValue({ ...entry, description: "ORDER: Defendant must respond by Friday." }), "5");
});

test("ingestion preserves RSS original row evidence but cannot inflate official coverage", () => withStore((store, id) => {
  const saved = insert(store, id, "rss", "99", "[court-rss:abcd] RSS update: [Order]");
  assert.equal(saved.entry_number, null);
  assert.equal(saved.raw.source_row_number, "99");
  const coverage = store.getEntryCoverageForCaseIds([id]).get(id);
  assert.equal(coverage.numberedEntries, 0);
  assert.equal(coverage.descriptiveEntries, 0);
  assert.equal(coverage.titleOnlyEntries, 1);
}));

test("legacy wrong mirror numbers merge with their real filing without rewriting raw storage", () => withStore((store, id) => {
  const notice = insert(store, id, "notice", "5", "Form AO 120/121（法院 RSS 更新，第 3 条）");
  // Simulate the old ingestion bug in the archived database.
  store.db.prepare("UPDATE docket_entries SET entry_number='5', document_number='5' WHERE id=?").run(notice.id);
  insert(store, id, "real", "3", "FORM AO 120 SENT TO DIRECTOR OF U.S. PATENT AND TRADEMARK (ksr)", "2026-10-15", "pacermonitor");
  const entries = store.getCase(id).entries;
  assert.equal(entries.length, 1);
  assert.equal(getDocketSequenceValue(entries[0]), "3");
  assert.equal(entries[0].filed_at, "2026-10-15");
  assert.match(entries[0].description, /SENT TO DIRECTOR/);
  assert.equal(store.db.prepare("SELECT entry_number FROM docket_entries WHERE id=?").get(notice.id).entry_number, "5");
  const coverage = store.getEntryCoverageForCaseIds([id]).get(id);
  assert.equal(coverage.totalEntries, 1);
  assert.equal(coverage.numberedEntries, 1);
  assert.equal(coverage.lastNumber, 3);
  assert.equal(coverage.descriptiveEntries, 1);
}));

test("one official number merges across source date disagreements without replacing rich text", () => withStore((store, id) => {
  insert(store, id, "title", "2", "Notice", "2026-10-16");
  insert(store, id, "body", "2", "NOTICE: All parties must file their corporate disclosure statements within seven days.", "2026-10-15", "pacermonitor");
  const entries = store.getCase(id).entries;
  assert.equal(entries.length, 1);
  assert.match(entries[0].description, /corporate disclosure/);
  assert.equal(entries[0].filed_at, "2026-10-15");
  assert.equal(store.getEntryCoverageForCaseIds([id]).get(id).totalEntries, 1);
}));

test("unknown filing dates do not become semantic matches merely from the ingestion day", () => withStore((store, id) => {
  for (const [source, key] of [["worldtro", "unknown"], ["pacermonitor", "known"]]) {
    insert(store, id, key, null, "NOTICE: The parties must provide a statement of their positions.", source === "worldtro" ? null : "2026-10-15", source);
  }
  store.db.prepare("UPDATE docket_entries SET created_at='2026-10-15T10:00:00Z'").run();
  assert.equal(store.getCase(id).entries.length, 2);
}));

test("legacy unnumbered RSS notices sort after real filings, not at their old fake number", () => withStore((store, id) => {
  const notice = insert(store, id, "notice", "99", "[court-rss:abcd] RSS update: [Order]");
  store.db.prepare("UPDATE docket_entries SET entry_number='99', document_number='99' WHERE id=?").run(notice.id);
  insert(store, id, "filing", "1", "COMPLAINT filed by Example Brand. Filing fee paid.", "2026-10-15", "pacermonitor");
  assert.deepEqual(store.getCase(id).entries.map(getDocketSequenceValue), ["1", ""]);
  assert.equal(store.getEntryCoverageForCaseIds([id]).get(id).lastNumber, 1);
}));

test("mirror row insertion cannot overwrite a different RSS notification", () => withStore((store, id) => {
  const save = (key, row, marker) => store.upsertDocketEntry({ case_id: id, source_entry_key: key,
    source_entry_id: row, primary_source: "worldtro", entry_number: row, document_number: row,
    description: `[court-rss:${marker}] RSS update: [Order]`, raw: { row_number: row } });
  const old = save("old-a", "1", "event-a");
  store.db.prepare("UPDATE docket_entries SET source_entry_id='1' WHERE id=?").run(old.id);
  save("new-b", "1", "event-b");
  save("moved-a", "2", "event-a");
  const rows = store.db.prepare("SELECT source_entry_id,description FROM docket_entries WHERE case_id=? ORDER BY id").all(id);
  assert.equal(rows.length, 2);
  assert.equal(rows[0].source_entry_id, "rss:event-a");
  assert.match(rows[0].description, /event-a/);
  assert.match(rows[1].description, /event-b/);
}));
