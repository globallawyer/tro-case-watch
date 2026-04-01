#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import process from "node:process";
import { config } from "./config.js";
import { Store } from "./db.js";
import { normalizeDocket } from "./insights.js";
import { PRIORITY_FEED_PROVIDER_KEY, PRIORITY_FEED_SOURCE } from "./priority-feed.js";

function parseArgs(argv = []) {
  const result = {
    year: 2026,
    limit: 20,
    sampleSize: 240,
    dryRun: false,
    sleepMs: 800,
    recentWindowDays: 365,
    providers: [PRIORITY_FEED_SOURCE, "courtlistener", "pacermonitor", "docketalarm", "unicourt"],
    onlyTargetCases: true
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];

    if (arg === "--year" && next) {
      result.year = Math.max(Number.parseInt(next, 10) || 2026, 2025);
      index += 1;
      continue;
    }

    if (arg === "--limit" && next) {
      result.limit = Math.max(Number.parseInt(next, 10) || 20, 1);
      index += 1;
      continue;
    }

    if (arg === "--sample-size" && next) {
      result.sampleSize = Math.max(Number.parseInt(next, 10) || 240, 50);
      index += 1;
      continue;
    }

    if (arg === "--sleep-ms" && next) {
      result.sleepMs = Math.max(Number.parseInt(next, 10) || 0, 0);
      index += 1;
      continue;
    }

    if (arg === "--recent-window-days" && next) {
      result.recentWindowDays = Math.max(Number.parseInt(next, 10) || 365, 30);
      index += 1;
      continue;
    }

    if (arg === "--providers" && next) {
      result.providers = String(next)
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean);
      index += 1;
      continue;
    }

    if (arg === "--all-cases") {
      result.onlyTargetCases = false;
      continue;
    }

    if (arg === "--dry-run") {
      result.dryRun = true;
    }
  }

  return result;
}

function wait(ms) {
  if (ms <= 0) {
    return;
  }

  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

function normalizeProviders(value = []) {
  return value.map((item) => {
    if (item === PRIORITY_FEED_PROVIDER_KEY) {
      return PRIORITY_FEED_SOURCE;
    }
    return item;
  });
}

function isPlaceholderCase(caseRow = {}) {
  const caseName = String(caseRow.case_name || "").trim().toLowerCase();
  const cause = String(caseRow.cause || "").trim().toLowerCase();
  const docketNumber = String(caseRow.docket_number || "").trim().toLowerCase();

  return Boolean(
    caseName === "v." ||
    caseName === "plaintiff(s) v. defendant(s)" ||
    cause.includes("civil shell case opening") ||
    cause.includes("civil miscellaneous case") ||
    docketNumber.includes("99-mc-09999") ||
    docketNumber.includes("26-cv-11111")
  );
}

function looksLikeTargetCase(item = {}, caseRow = {}) {
  if (!caseRow || isPlaceholderCase({ ...caseRow, docket_number: item.docket_number })) {
    return false;
  }

  const caseName = String(caseRow.case_name || "").toLowerCase();
  const cause = String(caseRow.cause || "").toLowerCase();
  const natureOfSuit = String(caseRow.nature_of_suit || "").toLowerCase();
  const ipText = `${caseName} | ${cause} | ${natureOfSuit}`;

  return Boolean(
    caseName.includes("schedule a") ||
    caseName.includes("unincorporated associations") ||
    /\b(820|830|840)\b/.test(natureOfSuit) ||
    /\b15:1114\b/.test(cause) ||
    /\b15:1125\b/.test(cause) ||
    /\b17:501\b/.test(cause) ||
    /\b35:271\b/.test(cause) ||
    ipText.includes("trademark") ||
    ipText.includes("copyright") ||
    ipText.includes("patent") ||
    ipText.includes("counterfeit") ||
    ipText.includes("infringement")
  );
}

function parseComparableNumber(value) {
  const normalized = String(value || "").trim().replace(/\.0$/, "");
  const parsed = Number.parseInt(normalized, 10);
  return Number.isFinite(parsed) ? parsed : 0;
}

function plaintiffKey(caseName) {
  const raw = String(caseName || "").split(/\s+v\.?\s+/i)[0] || "";
  return raw
    .toLowerCase()
    .replace(/[^\w]+/g, " ")
    .replace(/\b(llc|inc|incorporated|corp|corporation|company|co|ltd|limited|gmbh|s a|llp|lp)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function plaintiffsLookCompatible(leftCaseName, rightCaseName) {
  const left = plaintiffKey(leftCaseName);
  const right = plaintiffKey(rightCaseName);
  if (!left || !right) {
    return false;
  }

  return left === right || left.includes(right) || right.includes(left);
}

function buildSelectedProviders(item, allowProviders) {
  const selectedProviders = [];
  const hasCivilDocketNumber = /\b\d{2}-cv-\d{3,6}\b/i.test(String(item.docket_number || ""));

  if (allowProviders.has(PRIORITY_FEED_SOURCE)) {
    selectedProviders.push(PRIORITY_FEED_SOURCE);
  }

  if (Number(item.courtlistener_gap || 0) > 0 && allowProviders.has("courtlistener")) {
    selectedProviders.push("courtlistener");
  }

  if (hasCivilDocketNumber && Number(item.gap || 0) > 0 && allowProviders.has("pacermonitor")) {
    selectedProviders.push("pacermonitor");
  }

  if (Number(item.gap || 0) > 0 && allowProviders.has("docketalarm")) {
    selectedProviders.push("docketalarm");
  }

  if (Number(item.gap || 0) > 0 && allowProviders.has("unicourt")) {
    selectedProviders.push("unicourt");
  }

  return [...new Set(selectedProviders)];
}

function compareActivityDesc(left, right) {
  return String(right || "").localeCompare(String(left || ""));
}

const args = parseArgs(process.argv.slice(2));
const allowProviders = new Set(normalizeProviders(args.providers));
const store = new Store(config.dbPath);
const candidatePoolSize = Math.max(args.sampleSize * 8, args.limit * 40, 600);
const candidateRows = store.db
  .prepare(`
    SELECT
      id,
      primary_source,
      case_name,
      docket_number,
      date_filed,
      cause,
      nature_of_suit,
      latest_docket_filed_at,
      latest_docket_number,
      docket_count
    FROM cases
    WHERE date(date_filed) >= date(?)
      AND docket_number IS NOT NULL
      AND trim(docket_number) <> ''
      AND lower(primary_source) <> lower(?)
      AND (
        tags_marker LIKE '%|seller_tro|%'
        OR tags_marker LIKE '%|tro|%'
        OR tags_marker LIKE '%|schedule_a|%'
        OR COALESCE(latest_docket_filed_at, date_filed, updated_at) >= ?
      )
    ORDER BY COALESCE(latest_docket_filed_at, date_filed, updated_at) DESC
    LIMIT ?
  `)
  .all(
    `${args.year}-01-01`,
    PRIORITY_FEED_SOURCE,
    new Date(Date.now() - args.recentWindowDays * 24 * 60 * 60 * 1000).toISOString(),
    candidatePoolSize
  );

const entryCoverage = store.getEntryCoverageForCaseIds(candidateRows.map((row) => Number(row.id)));

const worldtroRows = store.db
  .prepare(`
    SELECT id, court_id, docket_number, case_name, date_filed, docket_count, latest_docket_number, latest_docket_filed_at
    FROM cases
    WHERE lower(primary_source) = lower(?)
      AND date(date_filed) >= date(?)
      AND docket_number IS NOT NULL
      AND trim(docket_number) <> ''
  `)
  .all(PRIORITY_FEED_SOURCE, `${args.year}-01-01`);

const worldtroByDocket = new Map();
for (const row of worldtroRows) {
  const docketKey = normalizeDocket(row.docket_number);
  if (!docketKey) {
    continue;
  }

  const expectedEntries = Math.max(
    Number(row.docket_count || 0),
    parseComparableNumber(row.latest_docket_number)
  );
  const existing = worldtroByDocket.get(docketKey);

  if (
    !existing ||
    expectedEntries > existing.expected_entries ||
    compareActivityDesc(row.latest_docket_filed_at, existing.latest_docket_filed_at) < 0
  ) {
    worldtroByDocket.set(docketKey, {
      id: Number(row.id),
      court_id: row.court_id || null,
      docket_number: row.docket_number || null,
      case_name: row.case_name || null,
      date_filed: row.date_filed || null,
      latest_docket_filed_at: row.latest_docket_filed_at || row.date_filed || null,
      expected_entries: expectedEntries,
      docket_count: Number(row.docket_count || 0),
      latest_docket_number: row.latest_docket_number || null
    });
  }
}

const selected = candidateRows
  .map((item) => {
    const coverage = entryCoverage.get(Number(item.id)) || {
      totalEntries: 0,
      courtlistenerEntries: 0
    };
    const totalEntries = Number(coverage.totalEntries || 0);
    const latestNumber = parseComparableNumber(item.latest_docket_number);
    const worldtroMatch = worldtroByDocket.get(normalizeDocket(item.docket_number));
    const worldtroExpectedEntries = Number(worldtroMatch?.expected_entries || 0);
    const courtListenerGap = Math.max(0, latestNumber - totalEntries);
    const expectedEntries = Math.max(
      Number(item.docket_count || 0),
      latestNumber,
      worldtroExpectedEntries,
      totalEntries
    );
    const gap = Math.max(0, expectedEntries - totalEntries);

    return {
      ...item,
      total_entries: totalEntries,
      courtlistener_gap: courtListenerGap,
      gap,
      worldtro_match: worldtroMatch || null,
      worldtro_expected_entries: worldtroExpectedEntries,
      worldtro_gap: Math.max(0, worldtroExpectedEntries - totalEntries)
    };
  })
  .filter((item) => String(item.date_filed || "").startsWith(`${args.year}-`))
  .filter((item) => !args.onlyTargetCases || looksLikeTargetCase(item, item))
  .filter((item) => item.worldtro_match && Number(item.worldtro_match.id || 0) > 0)
  .filter((item) => plaintiffsLookCompatible(item.case_name, item.worldtro_match?.case_name))
  .filter((item) => item.worldtro_gap > 0)
  .map((item) => ({
    ...item,
    selected_providers: buildSelectedProviders(item, allowProviders)
  }))
  .filter((item) => item.selected_providers.length > 0)
  .sort((left, right) => {
    if ((left.worldtro_gap || 0) !== (right.worldtro_gap || 0)) {
      return (right.worldtro_gap || 0) - (left.worldtro_gap || 0);
    }

    if ((left.gap || 0) !== (right.gap || 0)) {
      return (right.gap || 0) - (left.gap || 0);
    }

    return compareActivityDesc(left.latest_docket_filed_at, right.latest_docket_filed_at);
  })
  .slice(0, args.limit);

console.log(JSON.stringify({
  year: args.year,
  limit: args.limit,
  candidatePool: candidateRows.length,
  worldtroCatalogCases: worldtroRows.length,
  selected: selected.length,
  dryRun: args.dryRun,
  providers: [...allowProviders]
}, null, 2));

for (const item of selected) {
  const payload = {
    id: item.id,
    docket_number: item.docket_number,
    case_name: item.case_name,
    primary_source: item.primary_source || null,
    docket_count: item.docket_count,
    total_entries: item.total_entries,
    latest_docket_filed_at: item.latest_docket_filed_at,
    gap: item.gap,
    courtlistener_gap: item.courtlistener_gap,
    worldtro_case_id: item.worldtro_match?.id || null,
    worldtro_docket_number: item.worldtro_match?.docket_number || null,
    worldtro_expected_entries: item.worldtro_expected_entries,
    worldtro_gap: item.worldtro_gap,
    selected_providers: item.selected_providers
  };

  console.log(`[worldtro-gap-fill] candidate ${JSON.stringify(payload)}`);

  if (args.dryRun) {
    continue;
  }

  const command = [
    "src/server.js",
    "--enrich-case-id",
    String(item.id),
    "--providers",
    item.selected_providers.join(",")
  ];

  const result = spawnSync(process.execPath, command, {
    cwd: process.cwd(),
    env: process.env,
    stdio: "inherit"
  });

  if (result.status !== 0) {
    console.error(`[worldtro-gap-fill] failed case ${item.id} status=${result.status}`);
  }

  wait(args.sleepMs);
}
