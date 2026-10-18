import { DatabaseSync } from "node:sqlite";
import path from "node:path";
import { pathToFileURL } from "node:url";
const { Store } = await import(pathToFileURL(path.join(process.cwd(), "src/db.js")));
const { analyzeDocketSequence, getDocketSequenceValue } = await import(pathToFileURL(path.join(process.cwd(), "src/docket-sequence.js")));

const db = new DatabaseSync("/app/data/tro-watch.sqlite", { readOnly: true });
const store = Object.create(Store.prototype);
store.db = db;
store.caseCacheVersion = 0;
store.caseDetailCache = new Map();
const ids = String(process.env.AUDIT_CASE_IDS || "").split(",").map(Number).filter((value) => Number.isInteger(value) && value > 0).slice(0, 20);
const cases = ids.length
  ? db.prepare(`SELECT * FROM cases WHERE id IN (${ids.map(() => "?").join(",")}) ORDER BY created_at DESC,id DESC`).all(...ids)
  : db.prepare("SELECT * FROM cases ORDER BY created_at DESC,id DESC LIMIT 20").all();
const output = cases.map((row) => {
  const detail = store.getCase(row.id);
  const entries = detail.entries || [];
  const sequence = analyzeDocketSequence(entries, { upperBound: detail.latest_docket_number });
  return {
    id: row.id, docket: row.docket_number, court: row.court_id, courtName: row.court_name,
    name: row.case_name, createdAt: row.created_at, primarySource: row.primary_source,
    sources: JSON.parse(row.source_urls_json || "[]"), raw: JSON.parse(row.raw_json || "{}"),
    storedCount: row.docket_count, displayedCount: entries.length, sequence,
    entries: entries.map((entry) => ({
      id: entry.id, number: getDocketSequenceValue(entry), filed: entry.filed_at,
      source: entry.primary_source, text: entry.description, url: entry.absolute_url,
      raw: entry.raw
    }))
  };
});
console.log(JSON.stringify(output));
db.close();
