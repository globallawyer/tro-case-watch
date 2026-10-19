import { DatabaseSync } from "node:sqlite";
import { Store } from "../src/db.js";
import { normalizeDocketIdentity } from "../src/insights.js";
import { getDocketContentStatus } from "../src/docket-content.js";
import { getDocketSequenceValue } from "../src/docket-sequence.js";
import { getPriorityFeedRaw } from "../src/priority-feed.js";

const db = new DatabaseSync(process.env.DB_PATH || "/app/data/tro-watch.sqlite", { readOnly: true });
const store = Object.assign(Object.create(Store.prototype), { db, caseCacheVersion: 0, caseDetailCache: new Map() });
const seen = new Set();
const items = [];
try {
  const candidates = db.prepare(`SELECT id FROM cases WHERE is_seller_watch=1
    AND date_filed >= '2026-01-01' AND date_filed < '2026-10-21'
    ORDER BY date_filed DESC,created_at DESC,id DESC LIMIT 500`).all();
  for (const { id } of candidates) {
    const row = store.getCase(Number(id));
    const identity = `${row.court_id}|${normalizeDocketIdentity(row.docket_number)}`;
    if (seen.has(identity)) continue;
    seen.add(identity);
    const entries = row.entries || [];
    items.push({ id: row.id, court_id: row.court_id, court_name: row.court_name, docket_number: row.docket_number,
      case_name: row.case_name, date_filed: row.date_filed, created_at: row.created_at,
      plaintiffs: row.plaintiffs, source_urls: row.source_urls,
      raw: { priorityFeed: getPriorityFeedRaw(row), signal_feed: row.raw?.signal_feed },
      latest_docket_filed_at: row.latest_docket_filed_at, last_docket_sync_at: row.last_docket_sync_at,
      coverage: store.getEntryCoverageForCaseIds([row.id]).get(row.id) || null,
      entries: entries.map(entry => ({ id: entry.id, primary_source: entry.primary_source,
        entry_number: getDocketSequenceValue(entry) || null, document_type: entry.document_type,
        filed_at: entry.filed_at, description: entry.description, content_status: getDocketContentStatus(entry) })) });
    if (items.length === 20) break;
  }
  console.log(JSON.stringify({ fetchedAt: new Date().toISOString(), ordering: "filing-date-desc,created-desc", items }));
} finally { db.close(); }
