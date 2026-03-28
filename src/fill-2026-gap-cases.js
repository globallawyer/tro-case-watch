#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import process from "node:process";
import { config } from "./config.js";
import { Store } from "./db.js";
import { PRIORITY_FEED_PROVIDER_KEY, PRIORITY_FEED_SOURCE } from "./priority-feed.js";

function parseArgs(argv = []) {
  const result = {
    year: 2026,
    limit: 50,
    sampleSize: 400,
    dryRun: false,
    sleepMs: 1000,
    providers: ["courtlistener", PRIORITY_FEED_SOURCE]
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
      result.limit = Math.max(Number.parseInt(next, 10) || 50, 1);
      index += 1;
      continue;
    }

    if (arg === "--sample-size" && next) {
      result.sampleSize = Math.max(Number.parseInt(next, 10) || 400, 50);
      index += 1;
      continue;
    }

    if (arg === "--sleep-ms" && next) {
      result.sleepMs = Math.max(Number.parseInt(next, 10) || 0, 0);
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

const args = parseArgs(process.argv.slice(2));
const allowProviders = new Set(normalizeProviders(args.providers));
const store = new Store(config.dbPath);

const gapPayload = store.getCoverageGapCases(Math.max(args.sampleSize, args.limit * 6), {
  recentWindowDays: 365
});
const candidatePool = Array.isArray(gapPayload?.items) ? gapPayload.items : [];
const candidateIds = candidatePool
  .map((item) => Number(item.id))
  .filter((value) => Number.isFinite(value) && value > 0);
const candidateMeta = new Map();

if (candidateIds.length) {
  const placeholders = candidateIds.map(() => "?").join(", ");
  const rows = store.db
    .prepare(`
      SELECT id, primary_source, tags_marker, case_name, date_filed, cause, nature_of_suit
      FROM cases
      WHERE id IN (${placeholders})
    `)
    .all(...candidateIds);

  for (const row of rows) {
    candidateMeta.set(Number(row.id), row);
  }
}

const watchlistCandidates = candidatePool
  .map((item) => ({
    item,
    caseRow: candidateMeta.get(Number(item.id)) || null
  }))
  .filter(({ item, caseRow }) => {
    if (!caseRow) {
      return false;
    }
    if (isPlaceholderCase({ ...caseRow, docket_number: item.docket_number })) {
      return false;
    }

    const caseName = String(caseRow.case_name || "").toLowerCase();
    const cause = String(caseRow.cause || "").toLowerCase();
    const natureOfSuit = String(caseRow.nature_of_suit || "").toLowerCase();
    const ipText = `${caseName} | ${cause} | ${natureOfSuit}`;
    return Boolean(
      String(caseRow.primary_source || "").toLowerCase() === PRIORITY_FEED_SOURCE ||
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
  })
  .map(({ item, caseRow }) => ({
    ...item,
    date_filed: item.date_filed || caseRow.date_filed || null,
    primary_source: item.primary_source || caseRow.primary_source || null
  }));

const selected = watchlistCandidates
  .filter((item) => String(item.date_filed || "").startsWith(`${args.year}-`))
  .map((item) => {
    const mappedProviders = normalizeProviders(item.providers_needed || []);
    const selectedProviders = mappedProviders.filter((provider) => allowProviders.has(provider));

    if (!selectedProviders.length) {
      if (Number(item.priority_row_count || 0) > 0 && allowProviders.has(PRIORITY_FEED_SOURCE)) {
        selectedProviders.push(PRIORITY_FEED_SOURCE);
      }
      if (Number(item.courtlistener_gap || 0) > 0 && allowProviders.has("courtlistener")) {
        selectedProviders.push("courtlistener");
      }
    }

    return {
      ...item,
      selected_providers: [...new Set(selectedProviders)]
    };
  })
  .filter((item) => item.selected_providers.length > 0)
  .slice(0, args.limit);

console.log(JSON.stringify({
  year: args.year,
  limit: args.limit,
  candidatePool: candidatePool.length,
  watchlistCandidates: watchlistCandidates.length,
  selected: selected.length,
  summary: gapPayload?.summary || null,
  dryRun: args.dryRun,
  providers: [...allowProviders]
}, null, 2));

for (const item of selected) {
  const payload = {
    id: item.id,
    docket_number: item.docket_number,
    case_name: item.case_name,
    date_filed: item.date_filed,
    docket_count: item.docket_count,
    total_entries: item.total_entries,
    latest_docket_filed_at: item.latest_docket_filed_at,
    gap: item.gap,
    courtlistener_gap: item.courtlistener_gap,
    priority_row_count: item.priority_row_count,
    providers_needed: item.providers_needed,
    selected_providers: item.selected_providers,
    reasons: item.reasons
  };

  console.log(`[gap-fill] candidate ${JSON.stringify(payload)}`);

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
    console.error(`[gap-fill] failed case ${item.id} status=${result.status}`);
  }

  wait(args.sleepMs);
}
