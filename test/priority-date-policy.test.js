import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Store } from "../src/db.js";
import { getDocketSequenceValue } from "../src/docket-sequence.js";
import { getDocketPresentationPolicy, isPrimaryDocketEntry } from "../src/docket-presentation.js";
import { createCaseEnrichmentRefresher } from "../src/case-enrichment.js";
import { auditPriorityPolicy } from "../scripts/audit-2026-priority-policy.mjs";

function withStore(run) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "tro-priority-date-"));
  const store = new Store(path.join(dir, "test.sqlite"));
  const row = store.upsertCase({ source_case_key: "priority-case", source_case_id: "priority-case", primary_source: "worldtro", court_id: "ilnd",
    court_name: "Northern District of Illinois", docket_number: "1:26-cv-04482", date_filed: "2026-04-21",
    case_name: "Example Brand v. Schedule A Defendants", tags_marker: "|tro|schedule_a|seller_tro|" });
  const save = (key, source, number, text, extra = {}) => store.upsertDocketEntry({ case_id: row.id,
    source_entry_key: key, primary_source: source, source_entry_id: key, entry_number: number, document_number: number,
    description: text, filed_at: "2026-06-12", raw: {}, ...extra });
  try { run({ store, row, save }); } finally { store.db.close(); fs.rmSync(dir, { recursive: true, force: true }); }
}

test("primary source overrides longer auxiliary text and date without deleting independent entries", () => withStore(({ store, row, save }) => {
  save("aux", "courtlistener", "1", "ORDER: A long third-party version must not override primary wording.", { filed_at: "2026-06-14", description_zh: "OLD TRANSLATION" });
  save("primary", "worldtro", "1", "ORDER: Motion granted.", { filed_at: "2026-06-10" });
  save("extra", "signal-feed", "2", "NOTICE: Independent event available only from an auxiliary source.", { filed_at: "2026-06-13" });
  const entries = store.getCase(row.id).entries;
  assert.equal(entries.length, 2);
  assert.equal(entries[0].primary_source, "signal-feed");
  assert.equal(entries[1].description, "ORDER: Motion granted.");
  assert.equal(entries[1].filed_at, "2026-06-10");
  assert.equal(entries[1].description_zh, null);
  assert.equal(entries[1].primary_source, "worldtro");
  assert.equal(store.db.prepare("SELECT COUNT(*) AS n FROM docket_entries").get().n, 3);
}));

test("new auxiliary refreshes cannot displace already archived primary wording", () => withStore(({ store, row, save }) => {
  save("primary", "worldtro", "1", "ORDER: Primary wording.", { last_synced_at: "2026-06-12T00:00:00Z" });
  save("aux", "61tro", "1", "ORDER: Longer, newer auxiliary wording must remain secondary.", { last_synced_at: "2026-10-19T00:00:00Z" });
  assert.equal(store.getCase(row.id).entries[0].description, "ORDER: Primary wording.");
}));

test("primary correction replaces longer old wording, an old date and stale translation", () => withStore(({ store, row, save }) => {
  save("primary", "worldtro", "1", "ORDER: Long original wording to be corrected.", { description_zh: "OLD TRANSLATION", filed_at: "2026-06-14" });
  save("primary", "worldtro", "1", "ORDER: Corrected.", { filed_at: "2026-06-12" });
  const entry = store.getCase(row.id).entries[0];
  assert.equal(entry.description, "ORDER: Corrected.");
  assert.equal(entry.filed_at, "2026-06-12");
  assert.equal(entry.description_zh, null);
}));

test("the latest primary revision also corrects document metadata rather than preferring longer values", () => withStore(({ store, row, save }) => {
  save("primary-old", "worldtro", "1", "ORDER: Original longer wording.", { filed_at: "2026-06-14", last_synced_at: "2026-06-14T00:00:00Z", page_count: 40 });
  save("primary-new", "worldtro", "1", "ORDER: Corrected.", { filed_at: "2026-06-10", last_synced_at: "2026-10-19T00:00:00Z", page_count: 2 });
  assert.equal(store.getCase(row.id).entries[0].description, "ORDER: Corrected.");
  assert.equal(store.getCase(row.id).entries[0].page_count, 2);
  save("primary-new", "worldtro", "1", "ORDER: Updated.", { page_count: 1, document_type: "Order" });
  assert.equal(store.getCase(row.id).entries[0].page_count, 1);
}));

test("missing primary content can be supplied without losing the primary date", () => withStore(({ store, row, save }) => {
  save("aux", "signal-feed", "1", "COMPLAINT: Detailed auxiliary text.");
  save("primary", "worldtro", "1", "", { filed_at: "2026-06-10" });
  const entry = store.getCase(row.id).entries[0];
  assert.match(entry.description, /Detailed auxiliary/);
  assert.equal(entry.filed_at, "2026-06-10");
}));

test("a primary title is retained as the heading while a matched auxiliary body fills its missing explanation", () => withStore(({ store, row, save }) => {
  save("aux-body", "signal-feed", "1", "COMPLAINT: Plaintiff alleges infringement of the registered marks in Exhibit A.");
  save("primary-title", "worldtro", "1", "Complaint");
  const entry = store.getCase(row.id).entries[0];
  assert.equal(entry.primary_source, "worldtro");
  assert.equal(entry.document_type, "Complaint");
  assert.match(entry.description, /alleges infringement/);
  assert.equal(entry.raw.primary_title, "Complaint");
  assert.equal(entry.raw.body_supplement_source, "signal-feed");
}));

test("unique unnumbered primary text replaces its auxiliary variant without trusting table position", () => withStore(({ store, row, save }) => {
  const text = "MOTION by Example Brand for preliminary injunction against Store Alpha and Store Beta identified in Exhibit A";
  save("aux", "courtlistener", "30", `${text}, with supporting exhibits and counsel declaration.`);
  save("primary", "worldtro", "999", text, { raw: { row_number: 999 } });
  save("missing", "signal-feed", "31", "ORDER: A separate filing not available from the primary source.");
  const entries = store.getCase(row.id).entries;
  assert.equal(entries.length, 2);
  const primary = entries.find(isPrimaryDocketEntry);
  assert.equal(primary.description, text);
  assert.equal(getDocketSequenceValue(primary), "");
  assert.equal(getDocketPresentationPolicy(entries).hideNumbers, true);
}));

test("ambiguous same-day filings never merge merely through a mirror table position", () => withStore(({ store, row, save }) => {
  save("one", "signal-feed", "1", "ORDER: Briefing is due July 19 for Defendant Alpha.");
  save("two", "signal-feed", "2", "ORDER: Briefing is due August 20 for Defendant Beta.");
  save("primary", "worldtro", "1", "ORDER: Distinct enforcement proceeding against Defendant Gamma.", { raw: { row_number: 1 } });
  assert.equal(store.getCase(row.id).entries.length, 3);
}));

test("RSS notifications under a mirror source name are not actual mirror content", () => withStore(({ store, row, save }) => {
  save("rss", "worldtro", "99", "[court-rss:notice] Preliminary Injunction", { raw: { row_number: 99, documentNumber: "1" } });
  save("real", "signal-feed", "1", "INJUNCTION: The Court enjoins sales of products identified in Exhibit A.");
  const entry = store.getCase(row.id).entries[0];
  assert.equal(isPrimaryDocketEntry(entry), false);
  assert.match(entry.description, /enjoins sales/);
}));

test("preview resolves all primary counterparts before applying its display limit", () => withStore(({ store, row, save }) => {
  save("primary", "worldtro", "1", "ORDER: Primary wording and an older date.", { filed_at: "2026-01-01" });
  for (let n = 2; n < 30; n++) save(`extra:${n}`, "signal-feed", String(n), `NOTICE: Independent filing ${n}.`);
  save("aux", "courtlistener", "1", "ORDER: Auxiliary wording and a newer date.", { filed_at: "2026-10-19" });
  const entry = store.getCase(row.id, { recentEntriesLimit: 1 }).entries[0];
  assert.notEqual(getDocketSequenceValue(entry), "1");
  assert.equal(entry.filed_at, "2026-06-12");
}));

test("chronology never substitutes ingestion time for an unknown filing date", () => withStore(({ store, row, save }) => {
  save("newest", "signal-feed", "1", "ORDER: Recent filing.", { filed_at: "2026-10-19" });
  save("older", "worldtro", "999", "NOTICE: Older primary filing.", { filed_at: "2026-01-01" });
  save("undated", "61tro", "1000", "NOTICE: Filing with no published date.", { filed_at: null });
  assert.deepEqual(store.getCase(row.id).entries.map(entry => entry.source_entry_key), ["newest", "older", "undated"]);
}));

test("number display hides gaps and date conflicts, not legitimate unnumbered notices", () => {
  const entry = (number, date) => ({ entry_number: String(number), filed_at: date, raw: {}, primary_source: "signal-feed" });
  assert.equal(getDocketPresentationPolicy([entry(1, "2026-01-01"), entry(3, "2026-02-01")]).hideNumbers, true);
  assert.equal(getDocketPresentationPolicy([entry(1, "2026-02-01"), entry(2, "2026-01-01")]).hideNumbers, true);
  assert.equal(getDocketPresentationPolicy([entry(1, "2026-02-01T12:00:00Z"), entry(2, "2026-02-01T00:00:00Z")]).hideNumbers, true);
  assert.equal(getDocketPresentationPolicy([entry(1, "2026-02-01"), entry(2, null)]).hideNumbers, true);
  assert.equal(getDocketPresentationPolicy([entry(1, "2026-01-01"), entry(2, "2026-02-01"), { filed_at: "2026-02-01", raw: {} }]).hideNumbers, false);
});

test("old queued provider lists also try the primary mirror first", async () => {
  const calls = [];
  const refresh = createCaseEnrichmentRefresher({ store: { getCase: id => ({ id }), refreshCaseDocketSummary() {} }, syncService: {
    async enrichCaseWithSignalFeed() { calls.push("signal"); return { enriched: true }; },
    async enrichCaseWithPriorityFeed() { calls.push("primary"); return { enriched: true }; }
  } });
  await refresh(1, { providers: ["signal-feed", "worldtro"] });
  assert.deepEqual(calls, ["primary", "signal"]);
});

test("full-year audit checks all inventory records without changing source rows", () => withStore(({ store, row, save }) => {
  save("aux", "courtlistener", "1", "ORDER: An auxiliary version.");
  save("primary", "worldtro", "1", "ORDER: Primary version.");
  store.upsertCase({ source_case_key: "missing-date", source_case_id: "missing-date", primary_source: "61tro",
    court_id: "ilnd", docket_number: "1:26-cv-09999", case_name: "Undated Brand v. Schedule A Defendants", tags_marker: "|tro|schedule_a|seller_tro|" });
  store.upsertCase({ source_case_key: "old", source_case_id: "old", primary_source: "61tro",
    court_id: "ilnd", docket_number: "1:25-cv-00001", date_filed: "2025-01-01", case_name: "Old Brand v. Schedule A Defendants", tags_marker: "|tro|schedule_a|seller_tro|" });
  const before = store.db.prepare("SELECT COUNT(*) AS n FROM docket_entries").get().n;
  const dbPath = store.db.prepare("PRAGMA database_list").get().file;
  const report = auditPriorityPolicy(dbPath);
  assert.equal(report.caseRecords, 2);
  assert.equal(report.logicalCases, 2);
  assert.equal(report.primaryDisplayedEntries, 1);
  assert.equal(report.emptyCases, 1);
  assert.equal(report.passed, true);
  assert.equal(store.db.prepare("SELECT COUNT(*) AS n FROM docket_entries").get().n, before);
  assert.equal(store.getCase(row.id).entries.length, 1);
}));
