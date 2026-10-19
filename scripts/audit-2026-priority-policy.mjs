import { DatabaseSync } from "node:sqlite";
import { pathToFileURL } from "node:url";
import { Store } from "../src/db.js";
import { getDocketContentStatus } from "../src/docket-content.js";
import { DOCKET_PRESENTATION_VERSION, docketTimelineTime, getDocketPresentationPolicy, isPrimaryDocketEntry } from "../src/docket-presentation.js";

export function auditPriorityPolicy(dbPath, { progress = () => {} } = {}) {
  const started = Date.now();
  const db = new DatabaseSync(dbPath, { readOnly: true });
  const store = Object.assign(Object.create(Store.prototype), { db });
  const report = { version: DOCKET_PRESENTATION_VERSION, year: 2026, readOnly: true,
    caseRecords: 0, logicalCases: 0, rawEntries: 0, displayedEntries: 0, primaryRawEntries: 0,
    primaryDisplayedEntries: 0, auxiliaryDisplayedEntries: 0, casesWithPrimaryData: 0,
    casesWithoutPrimaryData: 0, emptyCases: 0, numberingHiddenCases: 0, titleOnlyEntries: 0,
    unknownDateEntries: 0, violations: {}, examples: [] };
  const seen = new Set();
  const fail = (kind, id, entryId) => {
    report.violations[kind] = (report.violations[kind] || 0) + 1;
    if (report.examples.length < 30) report.examples.push({ kind, caseId: id, entryId });
  };
  try {
    // Keep one consistent read snapshot. Run against a SQLite backup rather than
    // holding a long read transaction on the live WAL during a full audit.
    db.exec("BEGIN");
    const page = db.prepare(`SELECT id FROM cases WHERE id > ? AND (
      (date_filed >= '2026-01-01' AND date_filed < '2027-01-01') OR
      lower(docket_number) GLOB '*26-cv-*' OR lower(docket_number) GLOB '*2026-cv-*'
    ) ORDER BY id LIMIT 64`);
    let after = 0;
    while (true) {
      const ids = page.all(after).map(row => Number(row.id));
      if (!ids.length) break;
      after = ids.at(-1);
      report.caseRecords += ids.length;
      for (const { caseIds, peerIds, entries, rawEntries } of store.iterateCaseDockets(ids)) {
        const identity = [...peerIds].sort((a, b) => a - b).join(",");
        if (seen.has(identity)) continue;
        seen.add(identity);
        const id = caseIds[0];
        report.logicalCases++;
        report.rawEntries += rawEntries.length;
        report.displayedEntries += entries.length;
        const primary = rawEntries.filter(isPrimaryDocketEntry);
        report.primaryRawEntries += primary.length;
        report[primary.length ? "casesWithPrimaryData" : "casesWithoutPrimaryData"]++;
        if (!entries.length) report.emptyCases++;
        if (getDocketPresentationPolicy(entries).hideNumbers) report.numberingHiddenCases++;
        const contributors = new Map();
        let previous = Infinity;
        for (const entry of entries) {
          const time = docketTimelineTime(entry);
          if (time > previous) fail("date_order", id, entry.id);
          previous = time;
          if (time === -Infinity) report.unknownDateEntries++;
          if (getDocketContentStatus(entry) !== "descriptive") report.titleOnlyEntries++;
          report[isPrimaryDocketEntry(entry) ? "primaryDisplayedEntries" : "auxiliaryDisplayedEntries"]++;
          for (const rawId of new Set([entry.id, ...(entry.raw?.merged_entry_ids || [])])) {
            const outputs = contributors.get(Number(rawId)) || [];
            outputs.push(entry);
            contributors.set(Number(rawId), outputs);
          }
        }
        for (const raw of rawEntries) {
          const outputs = contributors.get(Number(raw.id)) || [];
          if (!outputs.length) fail("lost_raw_contributor", id, raw.id);
          if (isPrimaryDocketEntry(raw) && outputs.some(entry => !isPrimaryDocketEntry(entry))) {
            fail("auxiliary_displaced_primary", id, raw.id);
          }
        }
        // Every merged primary group must retain the most recently archived
        // primary wording, even when that correction is shorter than its copy.
        const rawById = new Map(primary.map(entry => [Number(entry.id), entry]));
        for (const entry of entries.filter(isPrimaryDocketEntry)) {
          const versions = [entry.id, ...(entry.raw?.merged_entry_ids || [])]
            .map(rawId => rawById.get(Number(rawId))).filter(Boolean)
            .sort((left, right) => (Date.parse(right.last_synced_at || right.updated_at || "") || 0)
              - (Date.parse(left.last_synced_at || left.updated_at || "") || 0));
          const latestTime = versions.length ? Date.parse(versions[0].last_synced_at || versions[0].updated_at || "") || 0 : 0;
          const latest = versions.filter(row => (Date.parse(row.last_synced_at || row.updated_at || "") || 0) === latestTime);
          if (latest.some(row => getDocketContentStatus(row) === "descriptive") && !latest.some(row => row.description === entry.description)) {
            fail("primary_wording_not_retained", id, entry.id);
          }
        }
      }
      if (report.caseRecords % 512 === 0) progress({ caseRecords: report.caseRecords, logicalCases: report.logicalCases,
        elapsedMs: Date.now() - started, rssMB: Math.round(process.memoryUsage().rss / 1048576) });
    }
    db.exec("ROLLBACK");
  } finally { db.close(); }
  report.elapsedMs = Date.now() - started;
  report.maxRssMB = Math.round(process.resourceUsage().maxRSS / 1024);
  report.passed = Object.keys(report.violations).length === 0;
  return report;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const report = auditPriorityPolicy(process.argv[2] || "/app/data/tro-watch.sqlite", {
    progress: value => process.stderr.write(`${JSON.stringify({ progress: value })}\n`)
  });
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
  if (!report.passed) process.exitCode = 1;
}
