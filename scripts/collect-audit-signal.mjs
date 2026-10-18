import fs from "node:fs";
import { SignalFeedClient, __test } from "../src/providers/signal-feed.js";

const audit = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const html = fs.readFileSync(process.argv[3], "utf8");
const client = new SignalFeedClient({ enabled: true, publicCasesUrl: "https://www.trohub.com/en/cases/", timeoutMs:15000, minIntervalMs:2000 });
const items = [];
const failures = [];
for (const row of __test.parsePublicRows(html, client.publicCasesUrl)) {
  const matching = audit.filter((item) => item.docket.replace(/^\d+:/, "") === row.docketNumber.replace(/^\d+:/, ""));
  if (!matching.length) continue;
  try {
    const item = await client.fetchKnownCase(row);
    if (!item) {
      failures.push({ docket: row.docketNumber, reason: "invalid-case-detail" });
      continue;
    }
    if (!item.entries?.length) {
      failures.push({ docket: row.docketNumber, reason: "no-docket-entries" });
    }
    items.push({ targetIds: matching.map((item) => item.id), item });
    console.error(row.docketNumber, item.entries.length, "entries");
  } catch (error) {
    failures.push({ docket: row.docketNumber, reason: error.message });
  }
}
fs.writeFileSync(process.argv[4], JSON.stringify({ fetchedAt: new Date().toISOString(), items, failures }, null, 2));
