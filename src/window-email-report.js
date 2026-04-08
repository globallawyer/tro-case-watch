#!/usr/bin/env node

import nodemailer from "nodemailer";
import process from "node:process";
import { config } from "./config.js";
import { Store } from "./db.js";

function pad(value) {
  return String(value).padStart(2, "0");
}

function formatDateTime(date = new Date(), timeZone = "Asia/Shanghai") {
  const formatter = new Intl.DateTimeFormat("sv-SE", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  });
  return formatter.format(date).replace(" ", " ");
}

function formatDateKey(date = new Date(), timeZone = "Asia/Shanghai") {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  });
  return formatter.format(date);
}

function getOffsetMinutes(date = new Date(), timeZone = "Asia/Shanghai") {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    timeZoneName: "shortOffset"
  });
  const zone = formatter.formatToParts(date).find((part) => part.type === "timeZoneName")?.value || "GMT+0";
  const match = zone.match(/GMT([+-])(\d{1,2})(?::?(\d{2}))?/i);
  if (!match) {
    return 0;
  }

  const sign = match[1] === "-" ? -1 : 1;
  const hours = Number(match[2] || 0);
  const minutes = Number(match[3] || 0);
  return sign * (hours * 60 + minutes);
}

function zonedDateTimeToIso(dateKey, hour, minute, second = 0, timeZone = "Asia/Shanghai") {
  const [year, month, day] = String(dateKey || "").split("-").map((value) => Number(value));
  const approxUtc = new Date(Date.UTC(year, (month || 1) - 1, day || 1, hour || 0, minute || 0, second || 0));
  const offsetMinutes = getOffsetMinutes(approxUtc, timeZone);
  return new Date(approxUtc.getTime() - offsetMinutes * 60 * 1000).toISOString();
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function parseArgs(argv = []) {
  const result = {
    windowHours: 3,
    timeZone: "Asia/Shanghai",
    untilIso: null,
    to: null,
    force: false,
    caseLimit: 10
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];
    if (arg === "--window-hours" && next) {
      result.windowHours = Math.max(Number.parseInt(next, 10) || 3, 1);
      index += 1;
      continue;
    }
    if (arg === "--time-zone" && next) {
      result.timeZone = String(next || result.timeZone);
      index += 1;
      continue;
    }
    if (arg === "--until-iso" && next) {
      result.untilIso = String(next || "").trim() || null;
      index += 1;
      continue;
    }
    if (arg === "--to" && next) {
      result.to = String(next || "").trim() || null;
      index += 1;
      continue;
    }
    if (arg === "--case-limit" && next) {
      result.caseLimit = Math.max(Number.parseInt(next, 10) || 10, 1);
      index += 1;
      continue;
    }
    if (arg === "--force") {
      result.force = true;
    }
  }

  return result;
}

function buildWindowBounds(windowHours, timeZone) {
  const now = new Date();
  const dateKey = formatDateKey(now, timeZone);
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    hour12: false,
    hour: "2-digit",
    minute: "2-digit"
  });
  const clock = Object.fromEntries(formatter.formatToParts(now).map((part) => [part.type, part.value]));
  const localHour = Number(clock.hour || 0);
  const localMinute = Number(clock.minute || 0);
  const endBucketHour = localHour - (localHour % windowHours);
  const endIso = zonedDateTimeToIso(dateKey, endBucketHour, 0, 0, timeZone);
  const startIso = new Date(Date.parse(endIso) - windowHours * 60 * 60 * 1000).toISOString();

  return {
    startIso,
    endIso,
    startLabel: formatDateTime(new Date(startIso), timeZone),
    endLabel: formatDateTime(new Date(endIso), timeZone),
    bucketKey: `${timeZone}:${windowHours}:${startIso}:${endIso}`
  };
}

function getSourceBreakdown(db, tableName, startIso, endIso, sourceColumn = "primary_source") {
  return db
    .prepare(`
      SELECT COALESCE(${sourceColumn}, 'unknown') AS source, COUNT(*) AS count
      FROM ${tableName}
      WHERE created_at >= ?
        AND created_at < ?
      GROUP BY COALESCE(${sourceColumn}, 'unknown')
      ORDER BY count DESC, source ASC
    `)
    .all(startIso, endIso)
    .map((row) => ({
      source: row.source || "unknown",
      count: Number(row.count || 0)
    }));
}

function getTopCases(store, startIso, endIso, limit = 10) {
  return store.db
    .prepare(`
      SELECT
        c.id,
        c.docket_number,
        c.case_name,
        c.court_id,
        c.primary_source,
        COUNT(DISTINCT de.id) AS new_entry_count,
        MAX(de.created_at) AS last_entry_created_at
      FROM cases c
      LEFT JOIN docket_entries de
        ON de.case_id = c.id
       AND de.created_at >= ?
       AND de.created_at < ?
      WHERE c.created_at >= ?
        AND c.created_at < ?
         OR EXISTS (
           SELECT 1
           FROM docket_entries de2
           WHERE de2.case_id = c.id
             AND de2.created_at >= ?
             AND de2.created_at < ?
         )
      GROUP BY c.id
      ORDER BY
        COUNT(DISTINCT de.id) DESC,
        MAX(de.created_at) DESC,
        c.created_at DESC,
        c.id DESC
      LIMIT ?
    `)
    .all(startIso, endIso, startIso, endIso, startIso, endIso, Math.max(1, limit))
    .map((row) => ({
      id: Number(row.id || 0),
      docket_number: row.docket_number || null,
      case_name: row.case_name || null,
      court_id: row.court_id || null,
      primary_source: row.primary_source || null,
      new_entry_count: Number(row.new_entry_count || 0),
      last_entry_created_at: row.last_entry_created_at || null
    }));
}

function formatBreakdown(items = []) {
  if (!items.length) {
    return "无";
  }
  return items.map((item) => `${item.source}: ${item.count}`).join(" | ");
}

function formatSyncModeSummary(summary = {}) {
  return `运行 ${Number(summary.runCount || 0)} 次 / 案件 ${Number(summary.casesWritten || 0)} / docket ${Number(summary.docketEntriesWritten || 0)}`;
}

function buildIngestMetricSummary(payload = {}) {
  const syncBreakdown = payload.syncBreakdown || {};
  const totalVolume = Number(syncBreakdown.total?.docketEntriesWritten || 0);
  const netNew = Number(payload.newDocketEntriesCount || 0);
  return {
    netNew,
    totalVolume,
    replayWrites: Math.max(0, totalVolume - netNew)
  };
}

function buildMessage(payload) {
  const subject = `TRO Tracker 3小时快报 - ${payload.startLabel} 至 ${payload.endLabel}`;
  const ingestMetrics = buildIngestMetricSummary(payload);
  const topLines = payload.topCases.length
    ? payload.topCases.map((item, index) =>
        `${index + 1}. ${item.docket_number || "无案号"} | ${item.case_name || "未命名案件"} | ${item.court_id || "unknown"} | +${item.new_entry_count} | ${item.primary_source || "unknown"}`
      )
    : ["本窗口没有新增案件或新增 docket entry。"];

  const text = [
    "TRO Tracker 3小时快报",
    `窗口：${payload.startLabel} 至 ${payload.endLabel}`,
    `新增案件：${payload.newCasesCount}`,
    `新增 docket entries：${payload.newDocketEntriesCount}`,
    `净新增 vs 任务写入：净新增 ${ingestMetrics.netNew} / 任务写入 ${ingestMetrics.totalVolume} / 估算重复补写 ${ingestMetrics.replayWrites}`,
    `实时增量（recent任务）：${formatSyncModeSummary(payload.syncBreakdown?.recent)}`,
    `历史补写（backfill任务）：${formatSyncModeSummary(payload.syncBreakdown?.backfill)}`,
    `案件新增来源：${formatBreakdown(payload.caseSources)}`,
    `docket 新增来源：${formatBreakdown(payload.docketSources)}`,
    "",
    "重点变化：",
    ...topLines
  ].join("\n");

  const html = `
    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;line-height:1.6;">
      <h2>TRO Tracker 3小时快报</h2>
      <p><strong>窗口：</strong>${escapeHtml(payload.startLabel)} 至 ${escapeHtml(payload.endLabel)}</p>
      <p>
        <strong>新增案件：</strong>${payload.newCasesCount}<br>
        <strong>新增 docket entries：</strong>${payload.newDocketEntriesCount}<br>
        <strong>净新增 vs 任务写入：</strong>${ingestMetrics.netNew} / ${ingestMetrics.totalVolume} / 估算重复补写 ${ingestMetrics.replayWrites}<br>
        <strong>实时增量（recent任务）：</strong>${escapeHtml(formatSyncModeSummary(payload.syncBreakdown?.recent))}<br>
        <strong>历史补写（backfill任务）：</strong>${escapeHtml(formatSyncModeSummary(payload.syncBreakdown?.backfill))}<br>
        <strong>案件新增来源：</strong>${escapeHtml(formatBreakdown(payload.caseSources))}<br>
        <strong>docket 新增来源：</strong>${escapeHtml(formatBreakdown(payload.docketSources))}
      </p>
      <h3>重点变化</h3>
      ${payload.topCases.length ? `<ol>${payload.topCases.map((item) => `<li>${escapeHtml(item.docket_number || "无案号")} | ${escapeHtml(item.case_name || "未命名案件")} | ${escapeHtml(item.court_id || "unknown")} | +${item.new_entry_count} | ${escapeHtml(item.primary_source || "unknown")}</li>`).join("")}</ol>` : "<p>本窗口没有新增案件或新增 docket entry。</p>"}
    </div>
  `.trim();

  return { subject, text, html };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const store = new Store(config.dbPath);
  const to = args.to || config.reports?.dailyEmail?.to || config.email?.user || "";
  const timeZone = args.timeZone || config.reports?.dailyEmail?.timeZone || "Asia/Shanghai";
  const untilIso = args.untilIso || process.env.WINDOW_EMAIL_REPORT_UNTIL || null;

  if (!config.reports?.windowEmail?.enabled && !args.force) {
    console.log(JSON.stringify({ sent: false, reason: "disabled" }, null, 2));
    return;
  }

  if (untilIso && Date.now() > Date.parse(untilIso)) {
    console.log(JSON.stringify({ sent: false, reason: "campaign-ended", untilIso }, null, 2));
    return;
  }

  if (!config.email?.host || !config.email?.port || !config.email?.user || !config.email?.pass || !config.email?.from || !to) {
    console.log(JSON.stringify({ sent: false, reason: "missing-config", to }, null, 2));
    return;
  }

  const bounds = buildWindowBounds(args.windowHours, timeZone);
  const checkpointKey = `window-email-report:${bounds.bucketKey}:${to}`;
  const existing = store.getCheckpoint(checkpointKey);
  if (existing && !args.force) {
    console.log(JSON.stringify({ sent: false, reason: "already-sent", checkpointKey }, null, 2));
    return;
  }

  const runId = store.claimSyncRun("window-email", `report-${args.windowHours}h`, 30);
  if (!runId) {
    console.log(JSON.stringify({ sent: false, reason: "already-running" }, null, 2));
    return;
  }

  try {
    const newCasesCount = Number(
      store.db
        .prepare(`
          SELECT COUNT(*) AS n
          FROM cases
          WHERE created_at >= ?
            AND created_at < ?
        `)
        .get(bounds.startIso, bounds.endIso)?.n || 0
    );

    const newDocketEntriesCount = Number(
      store.db
        .prepare(`
          SELECT COUNT(*) AS n
          FROM docket_entries
          WHERE created_at >= ?
            AND created_at < ?
        `)
        .get(bounds.startIso, bounds.endIso)?.n || 0
    );

    const payload = {
      ...bounds,
      newCasesCount,
      newDocketEntriesCount,
      syncBreakdown: store.getSyncIngestBreakdown({ startIso: bounds.startIso, endIso: bounds.endIso }),
      caseSources: getSourceBreakdown(store.db, "cases", bounds.startIso, bounds.endIso),
      docketSources: getSourceBreakdown(store.db, "docket_entries", bounds.startIso, bounds.endIso),
      topCases: getTopCases(store, bounds.startIso, bounds.endIso, args.caseLimit)
    };

    const message = buildMessage(payload);
    const transport = nodemailer.createTransport({
      host: config.email.host,
      port: Number(config.email.port || 465),
      secure: Boolean(config.email.secure),
      auth: {
        user: config.email.user,
        pass: config.email.pass
      }
    });

    const info = await transport.sendMail({
      from: config.email.from,
      to,
      subject: message.subject,
      text: message.text,
      html: message.html
    });

    const checkpointPayload = {
      ...payload,
      sentAt: new Date().toISOString(),
      messageId: info.messageId || null,
      to
    };
    store.saveCheckpoint(checkpointKey, checkpointPayload);
    store.finishSyncRun(runId, "succeeded", checkpointPayload);
    console.log(JSON.stringify({ sent: true, checkpointKey, messageId: info.messageId || null, ...payload }, null, 2));
  } catch (error) {
    store.finishSyncRun(runId, "failed", { checkpointKey }, error?.message || String(error));
    throw error;
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
