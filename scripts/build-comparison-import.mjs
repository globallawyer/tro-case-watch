import fs from "node:fs";
import { normalizeDocketIdentity } from "../src/insights.js";
const comparison = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
if (!comparison.completedAt || !Number.isFinite(Date.parse(comparison.fetchedAt)) || Date.now() - Date.parse(comparison.fetchedAt) > 86400000) throw new Error("Comparison must be complete and fresh");
const grouped = new Map();
for (const row of comparison.rows) {
  const source = row.sources?.trohub;
  const item = source?.item;
  if (source?.status !== "matched" || !(source.descriptive > 0) || !item?.entries?.length) continue;
  const url = new URL(item.detailUrl);
  if (url.protocol !== "https:" || !["www.trohub.com", "trohub.com"].includes(url.hostname)) throw new Error("Untrusted source URL");
  if (row.court !== item.courtId || normalizeDocketIdentity(row.docket).replace(/^\d+:/, "") !== normalizeDocketIdentity(item.docketNumber).replace(/^\d+:/, "")) throw new Error("Source identity differs");
  const key = `${item.courtId}|${normalizeDocketIdentity(item.docketNumber)}`;
  if (!grouped.has(key)) grouped.set(key, { targetIds: [], item });
  grouped.get(key).targetIds.push(row.id);
}
fs.writeFileSync(process.argv[3], JSON.stringify({ fetchedAt: comparison.fetchedAt, items: [...grouped.values()] }, null, 2));
console.log(JSON.stringify({ cases: grouped.size, entries: [...grouped.values()].reduce((sum, row) => sum + row.item.entries.length, 0) }));
