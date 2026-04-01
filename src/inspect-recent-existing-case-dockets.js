#!/usr/bin/env node

import process from "node:process";
import { config } from "./config.js";
import { Store } from "./db.js";

function parseArgs(argv = []) {
  const result = {
    days: 3,
    startDay: "",
    endDay: "",
    tzOffsetHours: 8,
    topCasesLimit: 12
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];

    if (arg === "--days" && next) {
      result.days = Math.max(Number.parseInt(next, 10) || 3, 1);
      index += 1;
      continue;
    }
    if (arg === "--start-day" && next) {
      result.startDay = String(next || "").trim();
      index += 1;
      continue;
    }
    if (arg === "--end-day" && next) {
      result.endDay = String(next || "").trim();
      index += 1;
      continue;
    }
    if (arg === "--tz-offset-hours" && next) {
      result.tzOffsetHours = Number.parseInt(next, 10);
      index += 1;
      continue;
    }
    if (arg === "--top-cases-limit" && next) {
      result.topCasesLimit = Math.max(Number.parseInt(next, 10) || 12, 1);
      index += 1;
    }
  }

  return result;
}

function assertDay(value, label) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(value || ""))) {
    throw new Error(`Invalid ${label}: expected YYYY-MM-DD, received "${value}"`);
  }
}

function shiftDate(date, days) {
  return new Date(date.getTime() + days * 24 * 60 * 60 * 1000);
}

function formatDayWithOffset(date, tzOffsetHours = 8) {
  const shifted = new Date(date.getTime() + tzOffsetHours * 60 * 60 * 1000);
  return shifted.toISOString().slice(0, 10);
}

function resolveWindow(args) {
  const tzOffsetHours = Number.isFinite(args.tzOffsetHours) ? args.tzOffsetHours : 8;

  if (args.startDay || args.endDay) {
    const endDay = args.endDay || formatDayWithOffset(new Date(), tzOffsetHours);
    const startDay = args.startDay || endDay;
    assertDay(startDay, "--start-day");
    assertDay(endDay, "--end-day");
    return { startDay, endDay, tzOffsetHours };
  }

  const endDate = new Date();
  const startDate = shiftDate(endDate, -(Math.max(args.days, 1) - 1));
  return {
    startDay: formatDayWithOffset(startDate, tzOffsetHours),
    endDay: formatDayWithOffset(endDate, tzOffsetHours),
    tzOffsetHours
  };
}

function toCount(value) {
  const parsed = Number(value || 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function buildSqliteHourOffset(tzOffsetHours = 8) {
  const hours = Number.isFinite(tzOffsetHours) ? tzOffsetHours : 8;
  return `${hours >= 0 ? "+" : ""}${hours} hours`;
}

const args = parseArgs(process.argv.slice(2));
const windowBounds = resolveWindow(args);
const sqliteHourOffset = buildSqliteHourOffset(windowBounds.tzOffsetHours);
const store = new Store(config.dbPath);
const db = store.db;
const filterParams = [
  sqliteHourOffset,
  windowBounds.startDay,
  sqliteHourOffset,
  windowBounds.startDay,
  windowBounds.endDay,
  sqliteHourOffset,
  windowBounds.startDay,
  windowBounds.endDay
];

const totals = db.prepare(`
  SELECT
    COUNT(*) AS entry_count,
    COUNT(DISTINCT de.case_id) AS case_count
  FROM docket_entries de
  JOIN cases c ON c.id = de.case_id
  WHERE date(datetime(c.created_at, ?)) < ?
    AND de.filed_at IS NOT NULL
    AND date(datetime(de.filed_at, ?)) BETWEEN ? AND ?
    AND date(datetime(de.created_at, ?)) BETWEEN ? AND ?
`).get(...filterParams);

const bySource = db.prepare(`
  SELECT
    COALESCE(de.primary_source, 'unknown') AS source,
    COUNT(*) AS entry_count,
    COUNT(DISTINCT de.case_id) AS case_count
  FROM docket_entries de
  JOIN cases c ON c.id = de.case_id
  WHERE date(datetime(c.created_at, ?)) < ?
    AND de.filed_at IS NOT NULL
    AND date(datetime(de.filed_at, ?)) BETWEEN ? AND ?
    AND date(datetime(de.created_at, ?)) BETWEEN ? AND ?
  GROUP BY source
  ORDER BY entry_count DESC, case_count DESC, source ASC
`).all(...filterParams).map((row) => ({
  source: row.source || "unknown",
  docketEntries: toCount(row.entry_count),
  cases: toCount(row.case_count)
}));

const byGrabbedDayFiledDayAndSource = db.prepare(`
  SELECT
    date(datetime(de.created_at, ?)) AS grabbed_day,
    date(datetime(de.filed_at, ?)) AS filed_day,
    COALESCE(de.primary_source, 'unknown') AS source,
    COUNT(*) AS entry_count,
    COUNT(DISTINCT de.case_id) AS case_count
  FROM docket_entries de
  JOIN cases c ON c.id = de.case_id
  WHERE date(datetime(c.created_at, ?)) < ?
    AND de.filed_at IS NOT NULL
    AND date(datetime(de.filed_at, ?)) BETWEEN ? AND ?
    AND date(datetime(de.created_at, ?)) BETWEEN ? AND ?
  GROUP BY grabbed_day, filed_day, source
  ORDER BY grabbed_day DESC, filed_day DESC, entry_count DESC, source ASC
`).all(
  sqliteHourOffset,
  sqliteHourOffset,
  ...filterParams
).map((row) => ({
  grabbedDay: row.grabbed_day,
  filedDay: row.filed_day,
  source: row.source || "unknown",
  docketEntries: toCount(row.entry_count),
  cases: toCount(row.case_count)
}));

const topCases = db.prepare(`
  SELECT
    c.id,
    c.docket_number,
    c.case_name,
    COUNT(*) AS entry_count,
    GROUP_CONCAT(DISTINCT COALESCE(de.primary_source, 'unknown')) AS sources,
    MIN(date(datetime(de.filed_at, ?))) AS first_filed_day,
    MAX(date(datetime(de.filed_at, ?))) AS last_filed_day,
    MIN(date(datetime(de.created_at, ?))) AS first_grabbed_day,
    MAX(date(datetime(de.created_at, ?))) AS last_grabbed_day
  FROM docket_entries de
  JOIN cases c ON c.id = de.case_id
  WHERE date(datetime(c.created_at, ?)) < ?
    AND de.filed_at IS NOT NULL
    AND date(datetime(de.filed_at, ?)) BETWEEN ? AND ?
    AND date(datetime(de.created_at, ?)) BETWEEN ? AND ?
  GROUP BY c.id, c.docket_number, c.case_name
  ORDER BY entry_count DESC, last_grabbed_day DESC, c.id DESC
  LIMIT ?
`).all(
  sqliteHourOffset,
  sqliteHourOffset,
  sqliteHourOffset,
  sqliteHourOffset,
  ...filterParams,
  args.topCasesLimit
).map((row) => ({
  id: Number(row.id),
  docketNumber: row.docket_number || null,
  caseName: row.case_name || null,
  docketEntries: toCount(row.entry_count),
  sources: String(row.sources || "")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean),
  firstFiledDay: row.first_filed_day || null,
  lastFiledDay: row.last_filed_day || null,
  firstGrabbedDay: row.first_grabbed_day || null,
  lastGrabbedDay: row.last_grabbed_day || null
}));

const payload = {
  generatedAt: new Date().toISOString(),
  dbPath: config.dbPath,
  window: {
    startDay: windowBounds.startDay,
    endDay: windowBounds.endDay,
    tzOffsetHours: windowBounds.tzOffsetHours,
    existingCasesCreatedBeforeDay: windowBounds.startDay
  },
  summary: {
    docketEntries: toCount(totals?.entry_count),
    cases: toCount(totals?.case_count)
  },
  bySource,
  byGrabbedDayFiledDayAndSource,
  topCases
};

console.log(JSON.stringify(payload, null, 2));
db.close();
