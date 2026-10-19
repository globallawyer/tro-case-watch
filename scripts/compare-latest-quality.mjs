import fs from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";
import { CatalogClient } from "../src/providers/catalog.js";
import { SignalFeedClient } from "../src/providers/signal-feed.js";
import { LawFirmClient, parse61troCasePage } from "../src/providers/lawfirm.js";
import { normalizeDocketIdentity } from "../src/insights.js";
import { getDocketContentStatus } from "../src/docket-content.js";

const cohort = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const output = process.argv[3];
if (!output || !Array.isArray(cohort.items) || cohort.items.length > 20) throw new Error("Expected a bounded cohort and output path");
const primary = new CatalogClient({ enabled: true, timeoutMs: 10000, minIntervalMs: 3000 });
const signal = new SignalFeedClient({ enabled: true, publicCasesUrl: "https://www.trohub.com/en/cases/", timeoutMs: 10000, minIntervalMs: 2500 });
const law = new LawFirmClient({ enabled: true, sources: ["61tro"], timeoutMs: 10000, minIntervalMs: 2500 });
const lawSource = law.listSources().find(source => source.id === "61tro");
const core = value => normalizeDocketIdentity(value).replace(/^\d+:/, "");
const cache = new Map();
const blocked = new Map();
const result = { fetchedAt: new Date().toISOString(), snapshotAt: cohort.fetchedAt, rows: [] };
const write = () => fs.writeFileSync(output, JSON.stringify(result, null, 2));
const profile = entries => ({ count: entries.length,
  descriptive: entries.filter(entry => getDocketContentStatus({ ...entry, description: entry.description || entry.title || entry.text }) === "descriptive").length,
  latestFiledAt: entries.map(entry => entry.filed_at || entry.filedAt || "").sort().at(-1) || null });
const identityMatches = (row, item) => item && core(item.docketNumber) === core(row.docket_number) &&
  (item.courtId === row.court_id || (row.court_id.length === 2 && item.courtId?.startsWith(row.court_id)));
async function inspect(provider, row, task) {
  if (blocked.has(provider)) return { status: "access-blocked-not-compared", code: blocked.get(provider) };
  try {
    const item = await task();
    const entries = item?.entries || [];
    return { status: !item ? "not-verified" : entries.length ? "matched" : "metadata-only", ...profile(entries), item };
  } catch (error) {
    const code = error.code || error.cause?.code || error.name;
    if ([403, 429].includes(Number(error.status)) || /CHALLENGE|CAPTCHA|RESPONSE_TOO_LARGE/.test(code)) blocked.set(provider, code);
    return { status: "fetch-failed-not-compared", code, httpStatus: error.status };
  }
}
for (const row of cohort.items) {
  const key = `${row.court_id}|${core(row.docket_number)}`;
  if (!cache.has(key)) {
    // Reuse a fully identified counterpart only as lookup context; imports
    // independently verify court, plaintiff, filing date and office ambiguity.
    const known = cohort.items.find(peer => peer.court_id === row.court_id && core(peer.docket_number) === core(row.docket_number) && /^\d+:/.test(peer.docket_number)) || row;
    const source = {};
    source.worldtro = await inspect("worldtro", known, () => primary.enrichCase(known));
    source.trohub = await inspect("trohub", known, async () => {
      const item = known.raw?.signal_feed?.detailUrl
        ? await signal.fetchKnownCase({ detailUrl: known.raw.signal_feed.detailUrl })
        : await signal.lookupByDocket(known.docket_number, { courtId: known.court_id, caseName: known.case_name, dateFiled: known.date_filed });
      if (item && !identityMatches(known, item)) throw Object.assign(new Error("case identity differs"), { code: "IDENTITY_MISMATCH" });
      return item;
    });
    source.tro61 = await inspect("61tro", known, async () => {
      const url = [...(row.source_urls || []), ...(known.source_urls || [])].find(value => /^https:\/\/(?:www\.)?61tro\.com\/(?:index.php\/)?detail\/\d+\.html/.test(value));
      if (!url) return null;
      const item = parse61troCasePage(await law.fetchText(url), url, lawSource);
      if (item && !identityMatches(known, item)) throw Object.assign(new Error("case identity differs"), { code: "IDENTITY_MISMATCH" });
      return item;
    });
    source.docketbird = { status: "not-compared-no-verified-public-case-url" };
    cache.set(key, source);
  }
  const sources = cache.get(key);
  result.rows.push({ id: row.id, court: row.court_id, docket: row.docket_number, dateFiled: row.date_filed,
    ours: profile(row.entries || []), sources });
  write();
  console.error(JSON.stringify({ id: row.id, docket: row.docket_number,
    sources: Object.fromEntries(Object.entries(sources).map(([key, value]) => [key, { status: value.status, count: value.count, text: value.descriptive }])) }));
  await sleep(1500);
}
result.completedAt = new Date().toISOString();
write();
