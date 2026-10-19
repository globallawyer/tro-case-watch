import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { DatabaseSync } from "node:sqlite";

const root = process.cwd();
const { Store } = await import(pathToFileURL(path.join(root, "src/db.js")));
const { CaseSyncService } = await import(pathToFileURL(path.join(root, "src/sync.js")));
const { normalizeDocketIdentity } = await import(pathToFileURL(path.join(root, "src/insights.js")));
const payload = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
if (!Array.isArray(payload.items) || payload.items.length > 20 ||
    !Number.isFinite(Date.parse(payload.fetchedAt)) || Math.abs(Date.now() - Date.parse(payload.fetchedAt)) > 86400000) {
  throw new Error("Expected a fresh, bounded public audit snapshot");
}
const apply = process.argv.includes("--apply");
const dbPath = process.env.DB_PATH || path.join(root, "data/tro-watch.sqlite");
const store = apply ? new Store(dbPath) : Object.assign(Object.create(Store.prototype), {
  db: new DatabaseSync(dbPath, { readOnly: true }), caseCacheVersion: 0, caseDetailCache: new Map()
});
const service = new CaseSyncService({ config: { sync: {} }, store });
const results = [];
let transaction = false;
try {
  if (apply) { store.db.exec("BEGIN IMMEDIATE"); transaction = true; }
  for (const { targetIds, item } of payload.items) {
    const url = new URL(item.detailUrl);
    if (url.protocol !== "https:" || !["www.trohub.com", "trohub.com"].includes(url.hostname)) throw new Error("Untrusted snapshot origin");
    const placeholder = store.findCatalogPlaceholderForCase(item);
    const matched = (targetIds || []).some((id) => {
      const row = store.getCase(Number(id));
      return row && (placeholder?.id === row.id ||
        (row.court_id === item.courtId && normalizeDocketIdentity(row.docket_number) === normalizeDocketIdentity(item.docketNumber)));
    });
    if (!matched) throw new Error(`Snapshot identity mismatch: ${item.docketNumber}`);
    const result = apply ? service.ingestSignalFeedItems([item]) : {};
    results.push({ targetIds, docket: item.docketNumber, entries: item.entries?.length || 0, ...result });
  }
  if (transaction) { store.db.exec("COMMIT"); transaction = false; }
  console.log(JSON.stringify({ apply, results }));
} catch (error) {
  if (transaction) store.db.exec("ROLLBACK");
  throw error;
} finally {
  store.db.close();
}
