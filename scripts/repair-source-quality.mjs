import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { DatabaseSync } from "node:sqlite";

const root = process.cwd();
const { Store } = await import(pathToFileURL(path.join(root, "src/db.js")));
const { normalizeDocketFiledAt } = await import(pathToFileURL(path.join(root, "src/docket-date.js")));
const { PacerMonitorAdapter } = await import(pathToFileURL(path.join(root, "src/providers/pacermonitor.js")));
const { getIntegerDocketSequence } = await import(pathToFileURL(path.join(root, "src/docket-sequence.js")));
const apply = process.argv.includes("--apply");
const snapshotPath = process.argv.find((arg, i) => i > 1 && arg.endsWith(".json"));
const snapshot = snapshotPath ? JSON.parse(fs.readFileSync(snapshotPath, "utf8")) : { cases: [] };
if (!Array.isArray(snapshot.cases) || snapshot.cases.length > 4 || (snapshotPath && (!Number.isFinite(Date.parse(snapshot.fetchedAt)) || Math.abs(Date.now() - Date.parse(snapshot.fetchedAt)) > 86400000))) throw new Error("Expected bounded fresh source snapshot");
const dbPath = process.env.DB_PATH || "/app/data/tro-watch.sqlite";
const store = apply ? new Store(dbPath) : Object.assign(Object.create(Store.prototype), { db: new DatabaseSync(dbPath, { readOnly: true }), caseDetailCache: new Map(), caseCacheVersion: 0 });
const db = store.db;
const result = { apply, invalidDates: [], quarantinedEntries: [], restoredDates: [], skippedSnapshots: [] };
const affected = new Set();
const now = new Date().toISOString();
try {
  if (apply) {
    db.exec(`CREATE TABLE IF NOT EXISTS source_quality_repair_journal (
      repair_key TEXT PRIMARY KEY, record_type TEXT NOT NULL, record_id INTEGER NOT NULL,
      reason TEXT NOT NULL, snapshot_json TEXT NOT NULL, created_at TEXT NOT NULL);
      BEGIN IMMEDIATE;`);
  }
  const archive = (key, kind, row, reason) => {
    if (apply) db.prepare("INSERT OR IGNORE INTO source_quality_repair_journal VALUES (?,?,?,?,?,?)")
      .run(key, kind, row.id, reason, JSON.stringify(row), now);
  };
  const archiveCase = id => {
    const row = db.prepare("SELECT * FROM cases WHERE id=?").get(id);
    archive(`source-quality:case:${id}`, "case", row, "source-quality metadata repair");
    affected.add(id);
    return row;
  };
  const invalid = db.prepare("SELECT * FROM docket_entries WHERE CAST(substr(filed_at,1,4) AS INTEGER)>2027 LIMIT 21").all();
  if (invalid.length > 20) throw new Error("Invalid date repair exceeds audited safety bound");
  for (const entry of invalid) {
    archiveCase(entry.case_id);
    archive(`invalid-date:${entry.id}`, "docket_entry", entry, "impossible filing year");
    result.invalidDates.push({ id: entry.id, previous: entry.filed_at });
    if (apply) {
      const raw = JSON.parse(entry.raw_json || "{}");
      raw.rejected_filed_at = entry.filed_at;
      db.prepare("UPDATE docket_entries SET filed_at=NULL,raw_json=?,updated_at=? WHERE id=? AND filed_at=?")
        .run(JSON.stringify(raw), now, entry.id, entry.filed_at);
    }
  }

  const wrongCase = db.prepare("SELECT * FROM cases WHERE id=5267284").get();
  const foreign = db.prepare("SELECT * FROM docket_entries WHERE case_id=5267284 AND primary_source='pacermonitor' AND absolute_url LIKE '%/public/case/66055570/%'").all();
  if (foreign.length && (!/^BLANCHE BRIDAL/i.test(wrongCase?.case_name || "") || foreign.length > 20)) throw new Error("Quarantine identity differs from audited mismatch");
  if (foreign.length) {
    archiveCase(wrongCase.id);
    for (const entry of foreign) {
      archive(`foreign-pm:${entry.id}`, "docket_entry", entry, "Abdelwahed habeas entries linked to BLANCHE BRIDAL");
      result.quarantinedEntries.push(entry.id);
      if (apply) db.prepare("DELETE FROM docket_entries WHERE id=? AND case_id=? AND primary_source='pacermonitor'").run(entry.id, wrongCase.id);
    }
    if (apply) {
      const raw = JSON.parse(wrongCase.raw_json || "{}");
      raw.pacermonitor = { ...(raw.pacermonitor || {}), rejectedCaseUrl: raw.pacermonitor?.caseUrl || foreign[0].absolute_url,
        caseUrl: null, title: null, metaDescription: null, state: "identity-mismatch", syncedAt: null, checkedAt: now, rowCount: 0, lastAttemptEntryCount: 0 };
      const urls = JSON.parse(wrongCase.source_urls_json || "[]").filter(url => !url.includes("/public/case/66055570/"));
      db.prepare("UPDATE cases SET raw_json=?,source_urls_json=? WHERE id=?").run(JSON.stringify(raw), JSON.stringify(urls), wrongCase.id);
    }
  }

  const adapter = new PacerMonitorAdapter({ enabled: true });
  for (const item of snapshot.cases) {
    const url = new URL(item.url);
    if (url.protocol !== "https:" || url.hostname !== "www.pacermonitor.com" || !url.pathname.startsWith("/public/case/")) throw new Error("Untrusted source origin");
    const row = db.prepare("SELECT * FROM cases WHERE id=?").get(Number(item.id));
    if (!row) throw new Error("Snapshot target missing");
    const parsed = adapter.parseCasePage(item.html, item.url, row);
    if (!parsed.identity || !parsed.entries.length) { result.skippedSnapshots.push({ id: item.id, reason: parsed.state || "empty" }); continue; }
    archiveCase(row.id);
    for (const entry of db.prepare("SELECT * FROM docket_entries WHERE case_id=? AND primary_source='pacermonitor'").all(row.id)) {
      if (normalizeDocketFiledAt(entry.filed_at)) continue;
      const number = getIntegerDocketSequence(entry);
      const matches = parsed.entries.filter(candidate => number ? Number(candidate.row_number) === number : candidate.description === entry.description);
      if (matches.length !== 1 || !normalizeDocketFiledAt(matches[0].filed_at)) continue;
      archive(`restore-date:${entry.id}`, "docket_entry", entry, "explicit source date header");
      result.restoredDates.push({ id: entry.id, filedAt: matches[0].filed_at });
      if (apply) db.prepare("UPDATE docket_entries SET filed_at=?,updated_at=? WHERE id=?").run(matches[0].filed_at, now, entry.id);
    }
    if (apply) {
      const existing = store.getCase(row.id);
      const identity = parsed.identity;
      store.upsertCase({ ...existing, court_id: identity.courtId || existing.court_id, court_name: identity.courtName,
        case_name: identity.caseName, docket_number: identity.docketNumber });
    }
  }
  if (apply) {
    store.invalidateCaseViews();
    for (const id of affected) {
      const row = db.prepare("SELECT * FROM cases WHERE id=?").get(id);
      const detail = store.getCase(id);
      const entries = detail?.entries || [];
      const validDates = entries.map(e => normalizeDocketFiledAt(e.filed_at)).filter(Boolean).sort();
      const latestDate = validDates.at(-1) || normalizeDocketFiledAt(row.date_filed);
      const lastNumber = Math.max(0, ...entries.map(e => getIntegerDocketSequence(e) || 0));
      const quarantineCase = id === 5267284 && foreign.length > 0;
      db.prepare("UPDATE cases SET latest_docket_filed_at=?,latest_docket_number=?,docket_count=?,priority_activity_at=? WHERE id=?")
        .run(quarantineCase || !normalizeDocketFiledAt(row.latest_docket_filed_at) ? latestDate : row.latest_docket_filed_at,
          quarantineCase ? (lastNumber ? String(lastNumber) : null) : row.latest_docket_number,
          quarantineCase ? Math.max(entries.length, lastNumber) : row.docket_count,
          [row.updated_at, latestDate, row.date_filed].filter(Boolean).sort().at(-1) || null, id);
      if (quarantineCase) {
        db.prepare("UPDATE cases SET recent_activity_summary=?,recent_activity_summary_zh=NULL WHERE id=?")
          .run(entries[0]?.description || null, id);
        store.invalidateCaseViews();
        store.upsertCase(store.getCase(id));
      }
    }
    db.exec("COMMIT;");
  }
  console.log(JSON.stringify(result));
} catch (error) {
  if (apply) { try { db.exec("ROLLBACK;"); } catch {} }
  throw error;
} finally { db.close(); }
