import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Store } from "../src/db.js";
import { parseDocketDate, normalizeDocketFiledAt } from "../src/docket-date.js";
import { PacerMonitorAdapter, verifyPacerMonitorIdentity } from "../src/providers/pacermonitor.js";
import { getDocketSequenceValue } from "../src/docket-sequence.js";
import { getDocketContentStatus } from "../src/docket-content.js";
import { CaseSyncService } from "../src/sync.js";

test("docket dates accept explicit source formats without fabricating a missing year", () => {
  assert.equal(parseDocketDate("2026-10-15"), "2026-10-15");
  assert.equal(parseDocketDate("9/5/2026"), "2026-09-05");
  assert.equal(parseDocketDate("Thursday, October 15, 2026"), "2026-10-15");
  assert.equal(parseDocketDate("Sep. 5, 2026"), "2026-09-05");
  assert.equal(parseDocketDate("Thu 10/15 4:41 AM"), null);
});

test("invalid calendar dates and impossible years do not enter the filing timeline", () => {
  for (const value of ["3926-09-05", "02/30/2026", "2026-13-01", "2026-00-04"]) assert.equal(parseDocketDate(value), null);
  assert.equal(normalizeDocketFiledAt("2026-10-15T10:20:00.000Z"), "2026-10-15T10:20:00.000Z");
  assert.equal(parseDocketDate("02/29/2024"), "2024-02-29");
});

function row(dateCell, text = "COMPLAINT filed by Example Brand. Filing fee paid.") {
  return `<tr class="docketItemRow"><td class="docketItemNum">1</td><td class="docketItemDate">${dateCell}</td><td class="docketItemText">${text}</td></tr>`;
}

test("PACERMonitor reads explicit dates from its date cell and metadata", () => {
  const client = new PacerMonitorAdapter({ enabled: true });
  for (const cell of ["10/15/2026", '<span title="Thursday, October 15, 2026">Thu 10/15</span>', '<time datetime="2026-10-15T04:41:00-04:00">Thu 10/15</time>']) {
    assert.equal(client.parseCasePage(row(cell), "https://www.pacermonitor.com/public/case/1/test", {}).entries[0].filed_at, "2026-10-15");
  }
});

test("PACERMonitor does not confuse a response deadline with the filing date", () => {
  const client = new PacerMonitorAdapter({ enabled: true });
  const entries = client.parseCasePage(row("Thu 10/15", "ORDER: Defendant must respond by 10/20/2026."), "https://www.pacermonitor.com/public/case/1/test", {}).entries;
  assert.equal(entries[0].filed_at, null);
});

test("PACERMonitor date-group headers supply the actual year across New Year", () => {
  const client = new PacerMonitorAdapter({ enabled: true });
  const html = '<tr class="docketHeaderRow"><td>Friday, January 2, 2026</td></tr>' + row("Fri 01/02") +
    '<tr class="docketHeaderRow"><td>Wednesday, December 31, 2025</td></tr>' + row("Wed 12/31");
  const entries = client.parseCasePage(html, "https://www.pacermonitor.com/public/case/1/test", {}).entries;
  assert.deepEqual(entries.map(e => e.filed_at), ["2026-01-02", "2025-12-31"]);
});

test("PACERMonitor refuses a different court or office rather than trusting the first search hit", () => {
  const client = new PacerMonitorAdapter({ enabled: true });
  const html = '<title>Example Brand v. Schedule A (1:26-cv-27217), Florida Southern District Court</title>' + row("10/15/2026");
  const target = { docket_number: "1:26-cv-27217", court_name: "Southern District of Florida" };
  assert.equal(client.parseCasePage(html, "https://www.pacermonitor.com/public/case/1/test", target).entries.length, 1);
  for (const bad of [{ ...target, docket_number: "2:26-cv-27217" }, { ...target, court_name: "Northern District of Florida" }]) {
    assert.equal(client.parseCasePage(html, "https://www.pacermonitor.com/public/case/1/test", bad).state, "identity-mismatch");
  }
});

test("state-only PACERMonitor identities require an exact plaintiff and district containment", () => {
  const html = '<title>ALBION BRAND FOUNDRY LTD v. Schedule A (1:26-cv-27217), Florida Southern District Court</title>';
  const target = { docket_number: "26-cv-27217", court_id: "fl", court_name: "Florida", case_name: "ALBION BRAND FOUNDRY LTD" };
  assert.equal(verifyPacerMonitorIdentity(html, target).courtId, "flsd");
  assert.equal(verifyPacerMonitorIdentity(html, { ...target, case_name: "BLANCHE BRIDAL PTY. LTD." }), null);
  assert.equal(verifyPacerMonitorIdentity(html, { ...target, court_name: "Pennsylvania" }), null);
});

test("RSS internal sequence identifiers never become published docket numbers", () => {
  const entry = { primary_source: "courtfeed", entry_number: "71", raw: { sequenceNumber: "71", documentNumber: "" } };
  assert.equal(getDocketSequenceValue(entry), "");
  assert.equal(getDocketSequenceValue({ ...entry, entry_number: "2", raw: { sequenceNumber: "71", documentNumber: "2" } }), "2");
});

test("legacy RSS markers and generic notices remain title-only", () => {
  for (const description of ["[court-rss:abc123] Temporary Restraining Order", "Summons Issued", "Notice of Motion for Default Judgment", "[Complaint] 附件: Exhibit A", "CIVIL Cover Sheet Attachments: 1.pdf"]) {
    assert.equal(getDocketContentStatus({ description }), "title-only");
  }
});

test("PACERMonitor orphan attachments never masquerade as filings or leak across date groups", () => {
  const client = new PacerMonitorAdapter({ enabled: true });
  const orphan = '<tr class="docketItemAttachService"><td class="docketItemTextAttach">Exhibit A</td></tr>';
  const html = row("10/15/2026") + '<tr class="docketHeaderRow"><td>October 14, 2026</td></tr>' + orphan;
  const entries = client.parseCasePage(html, "https://www.pacermonitor.com/public/case/1/test", {}).entries;
  assert.equal(entries.length, 1);
  assert.doesNotMatch(entries[0].description, /Exhibit/);
});

test("storage and coverage agree with display quality and reject invalid filing dates", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "tro-source-date-"));
  const store = new Store(path.join(dir, "test.sqlite"));
  try {
    const saved = store.upsertCase({ source_case_key: "quality:1", source_case_id: "quality:1", primary_source: "courtlistener", court_id: "ilnd", court_name: "Northern District of Illinois", docket_number: "1:26-cv-12687", case_name: "Example Brand v. Schedule A Defendants", date_filed: "2026-10-15", latest_docket_filed_at: "3926-09-05", tags_marker: "|tro|schedule_a|", raw: {} });
    assert.equal(store.getCase(saved.id).latest_docket_filed_at, null);
    store.upsertDocketEntry({ case_id: saved.id, source_entry_key: "rss:internal", primary_source: "courtfeed", entry_number: "71", description: "[court-rss:abc] Temporary Restraining Order", filed_at: "3926-09-05", raw: { sequenceNumber: "71", documentNumber: "" } });
    const entry = store.getCase(saved.id).entries[0];
    assert.equal(entry.filed_at, null);
    assert.equal(entry.raw.rejected_filed_at, "3926-09-05");
    const coverage = store.getEntryCoverageForCaseIds([saved.id]).get(saved.id);
    assert.equal(coverage.descriptiveEntries, 0);
    assert.equal(coverage.numberedEntries, 0);
    assert.equal(coverage.hasContentGap, true);
    store.upsertDocketEntry({ case_id: saved.id, source_entry_key: "rss:internal", primary_source: "courtfeed", entry_number: "71", description: "[Temporary Restraining Order]", filed_at: "2026-10-15", raw: { sequenceNumber: "71", documentNumber: "" } });
    assert.equal(store.getCase(saved.id).entries[0].filed_at, "2026-10-15");
  } finally { store.db.close(); fs.rmSync(dir, { recursive: true, force: true }); }
});

test("PACERMonitor failures respect cooldown without advancing successful docket freshness", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "tro-pm-freshness-"));
  const store = new Store(path.join(dir, "test.sqlite"));
  try {
    const saved = store.upsertCase({ source_case_key: "quality:pm", source_case_id: "quality:pm", primary_source: "courtlistener", court_id: "ilnd", court_name: "Northern District of Illinois", docket_number: "1:26-cv-12687", case_name: "Example Brand v. Schedule A Defendants", date_filed: "2026-10-15", tags_marker: "|tro|schedule_a|", raw: { pacermonitor: { syncedAt: "2026-10-01T00:00:00Z", rowCount: 5 } } });
    let attempts = 0;
    const service = new CaseSyncService({ config: { sync: {}, pacerMonitor: { staleAfterHours: 24, blockedRetryAfterHours: 12, notFoundRetryAfterHours: 6 } }, store,
      pacerMonitor: { enabled: true, async enrichCase() { attempts++; return { state: "challenge", entries: [], syncedAt: new Date().toISOString() }; } } });
    await service.syncSinglePacerMonitorCase(store.getCase(saved.id));
    const raw = store.getCase(saved.id).raw.pacermonitor;
    assert.equal(raw.syncedAt, "2026-10-01T00:00:00Z");
    assert.equal(raw.rowCount, 5);
    assert.ok(raw.checkedAt);
    assert.equal((await service.enrichCaseWithPacerMonitor(saved.id)).reason, "cooldown");
    assert.equal(attempts, 1);
  } finally { store.db.close(); fs.rmSync(dir, { recursive: true, force: true }); }
});

test("case overview URLs cannot collapse different filings or unnumbered court notices", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "tro-case-url-dedup-"));
  const store = new Store(path.join(dir, "test.sqlite"));
  try {
    const saved = store.upsertCase({ source_case_key: "quality:url", source_case_id: "quality:url", primary_source: "courtlistener", court_id: "ilnd", court_name: "Northern District of Illinois", docket_number: "1:26-cv-12687", case_name: "Example Brand v. Schedule A Defendants", date_filed: "2026-10-15", tags_marker: "|tro|schedule_a|", raw: {} });
    for (const [key, number, description] of [["one", "1", "Complaint"], ["two", "2", "Complaint"], ["assignment", null, "CASE ASSIGNED to Judge Example"], ["consent", null, "CLERK'S NOTICE: All parties must consent to magistrate jurisdiction."]]) {
      store.upsertDocketEntry({ case_id: saved.id, source_entry_key: key, primary_source: "pacermonitor", entry_number: number, description, filed_at: "2026-10-15", absolute_url: "https://www.pacermonitor.com/public/case/1/test", raw: {} });
    }
    assert.equal(store.db.prepare("SELECT COUNT(*) AS n FROM docket_entries").get().n, 4);
    assert.equal(store.getCase(saved.id).entries.length, 4);
  } finally { store.db.close(); fs.rmSync(dir, { recursive: true, force: true }); }
});
