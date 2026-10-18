import fs from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";
import { LawFirmClient, parse61troCasePage } from "../src/providers/lawfirm.js";
import { getDocketContentStatus } from "../src/docket-content.js";

const cases = JSON.parse(fs.readFileSync(process.argv[2], "utf8")).slice(0, 20);
const snapshot = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
const source = new LawFirmClient({ enabled: true, sources: ["61tro"] }).listSources().find(s => s.id === "61tro");
const core = value => String(value).replace(/^\d+:/, "").replace(/^20(\d\d)-/, "$1-").toLowerCase();
const cache = new Map();
const fetchHtml = async url => {
  await sleep(2000);
  const response = await fetch(url, { signal: AbortSignal.timeout(12000), headers: { "User-Agent": "TROTracker/1.0 (+https://www.trotracker.com)" } });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.text();
};
const output = [];
for (const row of cases) {
  const key = `${row.court}:${core(row.docket)}`;
  if (!cache.has(key)) {
    let result;
    try {
      const q = core(row.docket).replace(/^(\d\d)-/, "20$1-");
      const url = `https://61tro.com/index.php/tro.html?q=${encodeURIComponent(q)}`;
      const html = await fetchHtml(url);
      const candidates = [...html.matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)]
        .filter(m => m[1].includes(q)).map(m => m[1].match(/href=["']([^"']*\/detail\/\d+\.html)["']/i)?.[1]).filter(Boolean);
      const items = [];
      for (const href of [...new Set(candidates)].slice(0, 3)) {
        const detailUrl = new URL(href, url).href;
        const item = parse61troCasePage(await fetchHtml(detailUrl), detailUrl, source);
        if (item?.courtId === row.court && core(item.docketNumber) === core(row.docket)) items.push(item);
      }
      const item = items.length === 1 ? items[0] : null;
      result = { status: item ? "matched" : (items.length ? "ambiguous" : "no-matching-public-detail"),
        url: item?.caseUrl || url, count: item?.entries.length ?? null,
        descriptive: item?.entries.filter(e => getDocketContentStatus(e) === "descriptive").length ?? null,
        numbered: item?.entries.filter(e => e.entryNumber).length ?? null,
        items };
    } catch (error) { result = { status: "fetch-failed", error: error.message }; }
    cache.set(key, result);
  }
  const signal = snapshot.items.find(s => s.targetIds.includes(row.id))?.item;
  output.push({ id: row.id, docket: row.docket, court: row.court,
    ours: { count: row.entries.length, descriptive: row.entries.filter(e => getDocketContentStatus({ description: e.text, document_type: e.documentType }) === "descriptive").length },
    trohub: signal ? { status: "matched", url: signal.detailUrl, count: signal.entries.length } : { status: "not-in-public-latest-15" },
    tro61: cache.get(key), docketbird: { status: "not-verified-access-denied", httpStatus: 403 } });
  console.error(JSON.stringify({ id: row.id, tro61: cache.get(key).status, count: cache.get(key).count }));
}
fs.writeFileSync(process.argv[4], JSON.stringify({ fetchedAt: new Date().toISOString(), rows: output }, null, 2));
