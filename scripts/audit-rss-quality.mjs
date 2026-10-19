import { DatabaseSync } from "node:sqlite";

const base = process.env.AUDIT_CODE_ROOT || "/app";
const { getDocketSequenceValue } = await import(`${base}/src/docket-sequence.js`);
const { getDocketContentStatus } = await import(`${base}/src/docket-content.js`);
const { getRssDocketEvidence } = await import(`${base}/src/docket-rss.js`);
const db = new DatabaseSync(process.env.DB_PATH || "/app/data/tro-watch.sqlite", { readOnly: true });
const counts = { rows: 0, numbered: 0, unnumbered: 0, titleOnly: 0, wrongStoredNumber: 0 };
const sourceCounts = {};
// Stream only RSS-like rows; never load the full docket archive into memory.
for (const row of db.prepare("SELECT entry_number,document_number,primary_source,description,description_zh,raw_json FROM docket_entries WHERE description LIKE '%court-rss:%' OR description LIKE '%法院 RSS 更新%'").iterate()) {
  let raw = {};
  try { raw = JSON.parse(row.raw_json || "{}"); } catch {}
  const entry = { ...row, raw };
  if (!getRssDocketEvidence(entry)) continue;
  const number = getDocketSequenceValue(entry);
  counts.rows++;
  counts[number ? "numbered" : "unnumbered"]++;
  if (getDocketContentStatus(entry) === "title-only") counts.titleOnly++;
  if (String(row.entry_number || "") !== number) counts.wrongStoredNumber++;
  sourceCounts[row.primary_source] = (sourceCounts[row.primary_source] || 0) + 1;
}

const cases = [];
for (const id of [5307760, 5267284, 5315681]) {
  const start = Date.now();
  const response = await fetch(`http://127.0.0.1:4127/api/cases/${id}?full=1`, {
    headers: { "x-admin-token": process.env.ADMIN_TOKEN || "" }, signal: AbortSignal.timeout(20000)
  });
  const item = await response.json();
  cases.push({ id, status: response.status, elapsedMs: Date.now()-start, court: item.court_id, docket: item.docket_number,
    count: item.docket_entries_count, textCount: item.docket_text_entries_count, unnumbered: item.unnumbered_docket_entries_count,
    entries: (item.entries || []).map(e => ({ id: e.id, number: e.docket_sequence, date: e.filed_at, quality: e.content_status,
      textPrefix: e.description?.slice(0, 70) })) });
}
const runs = db.prepare("SELECT provider,mode,status,started_at,finished_at,substr(error_text,1,300) AS error FROM sync_runs ORDER BY id DESC LIMIT 12").all();
console.log(JSON.stringify({ capturedAt: new Date().toISOString(), counts, sourceCounts, cases, runs }));
db.close();
