#!/usr/bin/env node

import process from "node:process";
import { config } from "./config.js";
import { Store } from "./db.js";

function parseArgs(argv = []) {
  const result = {
    recentDays: 7,
    windowHours: 3,
    windowCount: 16
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];
    if (arg === "--recent-days" && next) {
      result.recentDays = Math.max(Number.parseInt(next, 10) || 7, 1);
      index += 1;
      continue;
    }
    if (arg === "--window-hours" && next) {
      result.windowHours = Math.max(Number.parseInt(next, 10) || 3, 1);
      index += 1;
      continue;
    }
    if (arg === "--window-count" && next) {
      result.windowCount = Math.max(Number.parseInt(next, 10) || 16, 1);
      index += 1;
      continue;
    }
  }

  return result;
}

function toCount(value) {
  const parsed = Number(value || 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function summarizeSyncRunStats(stats = {}) {
  return {
    casesWritten:
      toCount(stats.casesUpserted) +
      toCount(stats.courtFeedCasesUpserted) +
      toCount(stats.recentFilingsCasesUpserted) +
      toCount(stats.pacerCasesUpserted) +
      toCount(stats.lawFirmCasesUpserted),
    docketEntriesWritten:
      toCount(stats.docketEntriesUpserted) +
      toCount(stats.courtFeedEntriesUpserted) +
      toCount(stats.lawFirmEntriesUpserted),
    priorityFeedCasesSynced: toCount(stats.priorityFeedCasesSynced),
    docketCasesSynced: toCount(stats.docketCasesSynced),
    notes: Array.isArray(stats.notes) ? stats.notes : []
  };
}

function mapCounts(rows = [], keyName = "key") {
  return rows.map((row) => ({
    [keyName]: row[keyName],
    count: toCount(row.count)
  }));
}

const args = parseArgs(process.argv.slice(2));
const store = new Store(config.dbPath);
const db = store.db;
const recentStartIso = new Date(Date.now() - args.recentDays * 24 * 60 * 60 * 1000).toISOString();
const recentWindowStartIso = new Date(Date.now() - args.windowHours * args.windowCount * 60 * 60 * 1000).toISOString();
const nowIso = new Date().toISOString();

const totals = db
  .prepare(`
    SELECT
      (SELECT COUNT(*) FROM cases) AS total_cases,
      (SELECT COUNT(*) FROM docket_entries) AS total_docket_entries,
      (SELECT MAX(created_at) FROM cases) AS latest_case_created_at,
      (SELECT MAX(created_at) FROM docket_entries) AS latest_docket_created_at
  `)
  .get();

const casesByFiledYear = mapCounts(
  db.prepare(`
    SELECT COALESCE(strftime('%Y', date_filed), 'unknown') AS key, COUNT(*) AS count
    FROM cases
    GROUP BY key
    ORDER BY key = 'unknown', key DESC
  `).all(),
  "key"
).map((row) => ({ year: row.key, count: row.count }));

const casesByPrimarySource = mapCounts(
  db.prepare(`
    SELECT COALESCE(primary_source, 'unknown') AS key, COUNT(*) AS count
    FROM cases
    GROUP BY key
    ORDER BY count DESC, key ASC
  `).all(),
  "key"
).map((row) => ({ source: row.key, count: row.count }));

const docketsByFiledYear = mapCounts(
  db.prepare(`
    SELECT COALESCE(strftime('%Y', filed_at), 'unknown') AS key, COUNT(*) AS count
    FROM docket_entries
    GROUP BY key
    ORDER BY key = 'unknown', key DESC
  `).all(),
  "key"
).map((row) => ({ year: row.key, count: row.count }));

const docketsByCaseFiledYear = mapCounts(
  db.prepare(`
    SELECT COALESCE(strftime('%Y', c.date_filed), 'unknown') AS key, COUNT(*) AS count
    FROM docket_entries de
    JOIN cases c ON c.id = de.case_id
    GROUP BY key
    ORDER BY key = 'unknown', key DESC
  `).all(),
  "key"
).map((row) => ({ year: row.key, count: row.count }));

const docketsByPrimarySource = mapCounts(
  db.prepare(`
    SELECT COALESCE(primary_source, 'unknown') AS key, COUNT(*) AS count
    FROM docket_entries
    GROUP BY key
    ORDER BY count DESC, key ASC
  `).all(),
  "key"
).map((row) => ({ source: row.key, count: row.count }));

const keySourceSummary = {
  worldtro: {
    cases: toCount(
      db.prepare(`SELECT COUNT(*) AS count FROM cases WHERE lower(COALESCE(primary_source, '')) = 'worldtro'`).get()?.count
    ),
    docketEntries: toCount(
      db.prepare(`SELECT COUNT(*) AS count FROM docket_entries WHERE lower(COALESCE(primary_source, '')) = 'worldtro'`).get()?.count
    )
  },
  courtlistener: {
    cases: toCount(
      db.prepare(`SELECT COUNT(*) AS count FROM cases WHERE lower(COALESCE(primary_source, '')) = 'courtlistener'`).get()?.count
    ),
    docketEntries: toCount(
      db.prepare(`SELECT COUNT(*) AS count FROM docket_entries WHERE lower(COALESCE(primary_source, '')) = 'courtlistener'`).get()?.count
    )
  }
};

const recentDocketWritesByDay = mapCounts(
  db.prepare(`
    SELECT substr(created_at, 1, 10) AS key, COUNT(*) AS count
    FROM docket_entries
    WHERE created_at >= ?
    GROUP BY key
    ORDER BY key DESC
  `).all(recentStartIso),
  "key"
).map((row) => ({ day: row.key, count: row.count }));

const recentDocketWritesByDayAndSource = db
  .prepare(`
    SELECT
      substr(created_at, 1, 10) AS day,
      COALESCE(primary_source, 'unknown') AS source,
      COUNT(*) AS count
    FROM docket_entries
    WHERE created_at >= ?
    GROUP BY day, source
    ORDER BY day DESC, count DESC, source ASC
  `)
  .all(recentStartIso)
  .map((row) => ({
    day: row.day,
    source: row.source || "unknown",
    count: toCount(row.count)
  }));

const recentDocketWritesBy3h = db
  .prepare(`
    SELECT
      datetime((CAST(strftime('%s', created_at) AS INTEGER) / (? * 3600)) * (? * 3600), 'unixepoch') AS bucket,
      COUNT(*) AS count
    FROM docket_entries
    WHERE created_at >= ?
    GROUP BY bucket
    ORDER BY bucket DESC
    LIMIT ?
  `)
  .all(args.windowHours, args.windowHours, recentWindowStartIso, args.windowCount)
  .map((row) => ({
    bucket: row.bucket,
    count: toCount(row.count)
  }));

const recentCourtListenerWritesBy3h = db
  .prepare(`
    SELECT
      datetime((CAST(strftime('%s', created_at) AS INTEGER) / (? * 3600)) * (? * 3600), 'unixepoch') AS bucket,
      COUNT(*) AS count
    FROM docket_entries
    WHERE created_at >= ?
      AND lower(COALESCE(primary_source, '')) = 'courtlistener'
    GROUP BY bucket
    ORDER BY bucket DESC
    LIMIT ?
  `)
  .all(args.windowHours, args.windowHours, recentWindowStartIso, args.windowCount)
  .map((row) => ({
    bucket: row.bucket,
    count: toCount(row.count)
  }));

const recentSyncRuns = store.getRecentSyncRuns(16).map((row) => {
  const stats = summarizeSyncRunStats(row.stats || {});
  return {
    id: row.id,
    provider: row.provider,
    mode: row.mode,
    status: row.status,
    started_at: row.started_at,
    finished_at: row.finished_at,
    casesWritten: stats.casesWritten,
    docketEntriesWritten: stats.docketEntriesWritten,
    priorityFeedCasesSynced: stats.priorityFeedCasesSynced,
    docketCasesSynced: stats.docketCasesSynced,
    notes: stats.notes.slice(0, 3)
  };
});

const payload = {
  generatedAt: nowIso,
  dbPath: config.dbPath,
  syncScope: {
    publicStartDate: config.sync.startDate,
    discoveryStartDate: config.sync.discoveryStartDate,
    priorityFeedDiscoveryPages: config.priorityFeed.discoveryPages
  },
  totals: {
    cases: toCount(totals?.total_cases),
    docketEntries: toCount(totals?.total_docket_entries),
    latestCaseCreatedAt: totals?.latest_case_created_at || null,
    latestDocketCreatedAt: totals?.latest_docket_created_at || null
  },
  casesByFiledYear,
  casesByPrimarySource,
  docketsByFiledYear,
  docketsByCaseFiledYear,
  docketsByPrimarySource,
  keySourceSummary,
  recentDocketWritesByDay,
  recentDocketWritesByDayAndSource,
  recentDocketWritesBy3h,
  recentCourtListenerWritesBy3h,
  syncBreakdownLast24h: store.getSyncIngestBreakdown({
    startIso: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
    endIso: nowIso
  }),
  syncBreakdownLast72h: store.getSyncIngestBreakdown({
    startIso: new Date(Date.now() - 72 * 60 * 60 * 1000).toISOString(),
    endIso: nowIso
  }),
  recentSyncRuns
};

console.log(JSON.stringify(payload, null, 2));
db.close();
