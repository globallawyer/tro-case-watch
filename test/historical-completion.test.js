import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Store } from "../src/db.js";
import { CaseSyncService } from "../src/sync.js";
import { SignalFeedClient } from "../src/providers/signal-feed.js";
import { getDocketSequenceValue } from "../src/docket-sequence.js";
import { getDocketContentStatus } from "../src/docket-content.js";

const caseRecord = (overrides = {}) => ({ source_case_key: "historical", source_case_id: "historical",
  primary_source: "courtlistener", court_id: "ilnd", court_name: "Northern District of Illinois",
  case_name: "Example Brand v. Schedule A Defendants", docket_number: "1:26-cv-04482",
  date_filed: "2026-04-21", tags_marker: "|tro|schedule_a|seller_tro|", docket_count: 12, latest_docket_number: "12", ...overrides });
async function withStore(run) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "tro-historical-"));
  const store = new Store(path.join(dir, "test.sqlite"));
  try { await run(store); } finally { store.db.close(); fs.rmSync(dir, { recursive: true, force: true }); }
}
const entry = (id, key, source, number, text, extra = {}) => ({ case_id: id, source_entry_key: key,
  source_entry_id: key, primary_source: source, entry_number: number, document_number: number,
  description: text, filed_at: "2026-06-12", raw: {}, ...extra });

test("historical public lookup searches by full-year docket instead of recent discovery", async () => {
  const client = new SignalFeedClient({ enabled: true, publicCasesUrl: "https://example.test/en/cases/" });
  let searchUrl;
  client.requestText = async url => {
    searchUrl = new URL(url);
    return '<tr><td><a href="/en/cases/00000000000000000000000000000001-case">1:2026-cv-04482</a></td></tr>';
  };
  client.fetchPublicRecent = async () => { throw new Error("Latest-only lookup is forbidden"); };
  client.fetchPublicDetail = async detailUrl => ({ docketNumber: "1:26-cv-04482", courtId: "ilnd", detailUrl, entries: [{ title: "ORDER: Extension granted.", documentNumber: 56 }] });
  const found = await client.lookupByDocket("1:26-cv-04482", { courtId: "ilnd" });
  assert.equal(searchUrl.searchParams.get("q"), "2026-cv-04482");
  assert.equal(found.entries[0].documentNumber, 56);
  assert.equal(await client.lookupByDocket("1:26-cv-04482", { courtId: "nysd" }), null);
});

test("historical lookup does not follow a foreign-origin case link", async () => {
  const client = new SignalFeedClient({ enabled: true, publicCasesUrl: "https://example.test/en/cases/" });
  client.requestText = async () => '<tr><td><a href="https://foreign.test/cases/x">1:2026-cv-04482</a></td></tr>';
  client.fetchPublicDetail = async () => { throw new Error("Foreign URL followed"); };
  assert.equal(await client.lookupByDocket("1:26-cv-04482"), null);
});

test("mirror row 34 can align to real docket 35 only through a unique text match", () => withStore(store => {
  const saved = store.upsertCase(caseRecord());
  const text = "PRELIMINARY INJUNCTION signed by Judge Example; notice mailed to all parties.";
  store.upsertDocketEntry(entry(saved.id, "mirror", "worldtro", "34", text, { filed_at: null, raw: { row_number: "34" } }));
  store.upsertDocketEntry(entry(saved.id, "real", "signal-feed", "35", text));
  const entries = store.getCase(saved.id).entries;
  assert.equal(entries.length, 1);
  assert.equal(getDocketSequenceValue(entries[0]), "35");
  assert.equal(entries[0].filed_at, "2026-06-12");
  const coverage = store.getEntryCoverageForCaseIds([saved.id]).get(saved.id);
  assert.equal(coverage.totalEntries, entries.length);
  assert.equal(coverage.lastNumber, 35);
}));

test("conflicting mirror row numbers cannot overwrite a different numbered court order", () => withStore(store => {
  const saved = store.upsertCase(caseRecord());
  store.upsertDocketEntry(entry(saved.id, "mirror", "worldtro", "34", "PRELIMINARY INJUNCTION signed by Judge Example.", { raw: { row_number: "34" } }));
  store.upsertDocketEntry(entry(saved.id, "official", "signal-feed", "34", "MINUTE entry: briefing schedule is set for July 19."));
  const entries = store.getCase(saved.id).entries;
  assert.equal(entries.length, 2);
  assert.match(entries.find(e => getDocketSequenceValue(e) === "34").description, /briefing schedule/);
  assert.equal(entries.filter(e => getDocketSequenceValue(e) === "").length, 1);
}));

test("unverified mirror row insertion preserves both distinct descriptions and archive continuity", () => withStore(store => {
  const saved = store.upsertCase(caseRecord());
  const save = (key, row, text) => store.upsertDocketEntry(entry(saved.id, key, "worldtro", row, text, { source_entry_id: row, raw: { row_number: row } }));
  save("old", "1", "ORDER: First hearing will be held July 19.");
  save("new", "1", "ORDER: Defendant must answer by August 20.");
  save("shifted", "2", "ORDER: First hearing will be held July 19.");
  assert.equal(store.db.prepare("SELECT COUNT(*) AS n FROM docket_entries WHERE case_id=?").get(saved.id).n, 2);
}));

test("compound CourtListener short labels remain title-only and cannot displace actual text", () => withStore(store => {
  const saved = store.upsertCase(caseRecord());
  const label = "Order on Motion for Preliminary Injunction AND Order on Motion to Unseal Document";
  const short = entry(saved.id, "label", "courtlistener", "35", label, { raw: { short_description: label, description: "" } });
  assert.equal(getDocketContentStatus(short), "title-only");
  store.upsertDocketEntry(short);
  store.upsertDocketEntry(entry(saved.id, "text", "signal-feed", "35", "INJUNCTION: Defendants may not sell the products listed in Exhibit A."));
  const entries = store.getCase(saved.id).entries;
  assert.equal(entries.length, 1);
  assert.match(entries[0].description, /may not sell/);
  assert.equal(store.getEntryCoverageForCaseIds([saved.id]).get(saved.id).descriptiveEntries, 1);
}));

test("completed recent candidates cannot permanently hide an older incomplete case", () => withStore(store => {
  const old = store.upsertCase(caseRecord());
  store.db.exec("BEGIN");
  for (let i = 0; i < 360; i++) {
    const row = store.upsertCase(caseRecord({ source_case_key: `recent:${i}`, source_case_id: `recent:${i}`, docket_number: `1:26-cv-${10000+i}`,
      date_filed: "2026-10-19", latest_docket_filed_at: "2026-10-19" }));
    for (let n = 1; n <= 12; n++) store.upsertDocketEntry(entry(row.id, `complete:${i}:${n}`, "signal-feed", String(n), `ORDER ${n}: Parties must file their joint statement before November 20.`, { filed_at: "2026-10-19" }));
  }
  store.db.exec("COMMIT");
  assert.equal(store.getCasesNeedingSupplementalDocketSync(1, { startDate: "2026-01-01", evaluationTime: "2026-10-19T00:00:00Z" }).length, 0);
  assert.equal(store.getCasesNeedingSupplementalDocketSync(1, { startDate: "2026-01-01", evaluationTime: "2026-10-19T00:10:00Z" })[0].id, old.id);
}));

test("empty mirror fetch retains successful history and does not advance docket freshness", async () => withStore(async store => {
  const saved = store.upsertCase(caseRecord({ last_docket_sync_at: "2026-06-01T00:00:00Z", raw: { priorityFeed: { syncedAt: "2026-06-01T00:00:00Z", rowCount: 39 } } }));
  const service = new CaseSyncService({ config: { sync: {} }, store, priorityFeed: { async enrichCase() { return { entries: [] }; } } });
  await service.syncSinglePriorityFeedCase(store.getCase(saved.id));
  const row = store.getCase(saved.id);
  assert.equal(row.raw.priorityFeed.syncedAt, "2026-06-01T00:00:00Z");
  assert.equal(row.raw.priorityFeed.rowCount, 39);
  assert.ok(row.raw.priorityFeed.checkedAt);
  assert.equal(row.last_docket_sync_at, "2026-06-01T00:00:00Z");
}));
