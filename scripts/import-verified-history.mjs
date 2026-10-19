import fs from "node:fs";
import crypto from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import { pathToFileURL } from "node:url";
import path from "node:path";

const root = process.cwd();
const { Store } = await import(pathToFileURL(path.join(root, "src/db.js")));
const { CaseSyncService } = await import(pathToFileURL(path.join(root, "src/sync.js")));
const { CatalogClient } = await import(pathToFileURL(path.join(root, "src/providers/catalog.js")));
const { __test: signalParser } = await import(pathToFileURL(path.join(root, "src/providers/signal-feed.js")));
const { normalizeDocketIdentity } = await import(pathToFileURL(path.join(root, "src/insights.js")));
const snapshot = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
if (!Number.isFinite(Date.parse(snapshot.fetchedAt)) || Math.abs(Date.now() - Date.parse(snapshot.fetchedAt)) > 86400000 ||
    !Array.isArray(snapshot.cases) || snapshot.cases.length > 4) throw new Error("Expected fresh bounded verified history snapshot");
const apply = process.argv.includes("--apply");
const dbPath = process.env.DB_PATH || "/app/data/tro-watch.sqlite";
const store = apply ? new Store(dbPath) : Object.assign(Object.create(Store.prototype), {
  db: new DatabaseSync(dbPath, { readOnly: true }), caseCacheVersion: 0, caseDetailCache: new Map()
});
const db = store.db;
const validated = [];
let transaction = false;
try {
  for (const source of snapshot.cases) {
    if (Buffer.byteLength(source.signalHtml || "") > 4 * 1024 * 1024 || Buffer.byteLength(source.mirrorHtml || "") > 4 * 1024 * 1024) throw new Error("Snapshot too large");
    const row = store.getCase(Number(source.id));
    const url = new URL(source.signalUrl);
    if (!row || url.protocol !== "https:" || !["www.trohub.com", "trohub.com"].includes(url.hostname) || !/^\/(?:en\/|zh\/)?cases\//.test(url.pathname)) throw new Error("Untrusted detail or missing target");
    const item = signalParser.parsePublicDetail(source.signalHtml, source.signalUrl);
    if (!item?.entries?.length || item.courtId !== row.court_id || normalizeDocketIdentity(item.docketNumber) !== normalizeDocketIdentity(row.docket_number)) throw new Error("Snapshot case identity mismatch");
    const parts = String(row.docket_number).match(/(\d{2})-cv-(\d+)$/);
    const mirrorUrl = new URL(source.mirrorUrl);
    const expectedPath = `/case-${row.court_id.slice(0, 2).toUpperCase()}-20${parts?.[1]}-cv-${parts?.[2]}.html`;
    if (mirrorUrl.protocol !== "https:" || !["worldtro.com", "www.worldtro.com"].includes(mirrorUrl.hostname) || mirrorUrl.pathname !== expectedPath) throw new Error("Mirror docket identity mismatch");
    const adapter = new CatalogClient({ enabled: true });
    const payload = adapter.parseCasePage(source.mirrorHtml, source.mirrorUrl, {});
    if (!payload.entries.length || !adapter.isPlausiblePayload(row, payload)) throw new Error("Unverified mirror content");
    if (item.entries.length > 2000 || payload.entries.length > 2000) throw new Error("Entry bound exceeded");
    validated.push({ row, item, payload });
  }
  if (apply) {
    db.exec(`CREATE TABLE IF NOT EXISTS source_quality_repair_journal (
      repair_key TEXT PRIMARY KEY, record_type TEXT NOT NULL, record_id INTEGER NOT NULL,
      reason TEXT NOT NULL, snapshot_json TEXT NOT NULL, created_at TEXT NOT NULL);
      BEGIN IMMEDIATE;`);
    transaction = true;
  }
  const batch = `history:${crypto.randomUUID()}`;
  const results = [];
  for (const { row, item, payload } of validated) {
    if (apply) {
      const archive = db.prepare("INSERT INTO source_quality_repair_journal VALUES(?,?,?,?,?,?)");
      archive.run(`${batch}:case:${row.id}`, "case", row.id, "verified historical completion", JSON.stringify(db.prepare("SELECT * FROM cases WHERE id=?").get(row.id)), new Date().toISOString());
      for (const entry of db.prepare("SELECT * FROM docket_entries WHERE case_id=?").all(row.id)) {
        archive.run(`${batch}:entry:${entry.id}`, "docket_entry", entry.id, "verified historical completion", JSON.stringify(entry), new Date().toISOString());
      }
      const service = new CaseSyncService({ config: { sync: {} }, store, priorityFeed: { async enrichCase() { return payload; } } });
      service.ingestSignalFeedItems([item]);
      await service.syncSinglePriorityFeedCase(store.getCase(row.id));
      store.refreshCaseDocketSummary(row.id, { touchUpdatedAt: false, exactDocketCount: true });
    }
    results.push({ caseId: row.id, docket: row.docket_number, sourceEntries: item.entries.length, mirrorEntries: payload.entries.length,
      coverage: store.getEntryCoverageForCaseIds([row.id]).get(row.id) });
  }
  if (transaction) { db.exec("COMMIT"); transaction = false; }
  console.log(JSON.stringify({ apply, batch, results }));
} catch (error) {
  if (transaction) db.exec("ROLLBACK");
  throw error;
} finally { db.close(); }
