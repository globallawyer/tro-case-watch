import nodemailer from "nodemailer";

function pad(value) {
  return String(value).padStart(2, "0");
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

function getClockParts(date = new Date(), timeZone = "Asia/Shanghai") {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    hour12: false,
    hour: "2-digit",
    minute: "2-digit"
  });
  const parts = Object.fromEntries(formatter.formatToParts(date).map((part) => [part.type, part.value]));
  return {
    hour: Number(parts.hour || 0),
    minute: Number(parts.minute || 0)
  };
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

function getDayBounds(dateKey, timeZone) {
  return {
    startIso: zonedDateTimeToIso(dateKey, 0, 0, 0, timeZone),
    endIso: zonedDateTimeToIso(dateKey, 24, 0, 0, timeZone)
  };
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function summarizeItem(item, index) {
  const badge = item.is_new_case ? "NEW" : `+${item.new_entry_count}`;
  const court = item.court_id || item.court_name || "unknown";
  const docket = item.docket_number || "无案号";
  const title = item.case_name || "未命名案件";
  const summary = item.summary || "暂无摘要";
  return {
    text: `${index + 1}. [${badge}] ${docket} | ${title} | ${court}\n   ${summary}`,
    html: `<li><strong>[${escapeHtml(badge)}]</strong> ${escapeHtml(docket)} | ${escapeHtml(title)} | ${escapeHtml(court)}<br>${escapeHtml(summary)}</li>`
  };
}

export class DailyEmailReportService {
  constructor({ config, store }) {
    this.config = config;
    this.store = store;
    this.emailConfig = config.email || {};
    this.reportConfig = config.reports?.dailyEmail || {};
    this.checkpointKey = `daily-email-report:${this.reportConfig.to || "default"}`;
  }

  isEnabled() {
    return Boolean(this.reportConfig.enabled);
  }

  hasTransportConfig() {
    return Boolean(
      this.emailConfig.host &&
      this.emailConfig.port &&
      this.emailConfig.user &&
      this.emailConfig.pass &&
      this.emailConfig.from &&
      this.reportConfig.to
    );
  }

  getStatus() {
    if (!this.isEnabled()) {
      return { enabled: false, state: "disabled" };
    }

    if (!this.hasTransportConfig()) {
      return { enabled: false, state: "missing-smtp-config" };
    }

    return { enabled: true, state: "ready" };
  }

  async maybeSendScheduledReport() {
    if (!this.isEnabled()) {
      return { sent: false, reason: "disabled" };
    }

    if (!this.hasTransportConfig()) {
      return { sent: false, reason: "missing-config" };
    }

    const now = new Date();
    const { hour, minute } = getClockParts(now, this.reportConfig.timeZone);
    if (hour !== Number(this.reportConfig.hour || 23) || minute !== Number(this.reportConfig.minute || 59)) {
      return { sent: false, reason: "not-time" };
    }

    const localDate = formatDateKey(now, this.reportConfig.timeZone);
    const checkpoint = this.store.getCheckpoint(this.checkpointKey) || {};
    if (checkpoint.localDate === localDate) {
      return { sent: false, reason: "already-sent", localDate };
    }

    return this.sendReport({ localDate });
  }

  async sendReport({ localDate = formatDateKey(new Date(), this.reportConfig.timeZone), force = false } = {}) {
    if (!this.hasTransportConfig()) {
      return { sent: false, reason: "missing-config" };
    }

    const checkpoint = this.store.getCheckpoint(this.checkpointKey) || {};
    if (!force && checkpoint.localDate === localDate) {
      return { sent: false, reason: "already-sent", localDate };
    }

    const runId = this.store.claimSyncRun("daily-email", "report", 30);
    if (!runId) {
      return { sent: false, reason: "already-running", localDate };
    }

    try {
      const { startIso, endIso } = getDayBounds(localDate, this.reportConfig.timeZone);
      const report = this.store.getDailyEmailReport({
        startIso,
        endIso,
        caseLimit: this.reportConfig.caseLimit
      });

      const mail = this.buildMessage(localDate, report);
      const transport = nodemailer.createTransport({
        host: this.emailConfig.host,
        port: Number(this.emailConfig.port || 465),
        secure: Boolean(this.emailConfig.secure),
        auth: {
          user: this.emailConfig.user,
          pass: this.emailConfig.pass
        }
      });

      const info = await transport.sendMail({
        from: this.emailConfig.from,
        to: this.reportConfig.to,
        subject: mail.subject,
        text: mail.text,
        html: mail.html
      });

      const payload = {
        localDate,
        sentAt: new Date().toISOString(),
        messageId: info.messageId || null,
        newCasesCount: report.newCasesCount,
        newDocketEntriesCount: report.newDocketEntriesCount
      };
      this.store.saveCheckpoint(this.checkpointKey, payload);
      this.store.finishSyncRun(runId, "succeeded", payload);
      return {
        sent: true,
        localDate,
        messageId: info.messageId || null,
        ...payload
      };
    } catch (error) {
      this.store.finishSyncRun(runId, "failed", { localDate }, error.message || String(error));
      throw error;
    }
  }

  buildMessage(localDate, report) {
    const subject = `TRO Tracker 日报 - ${localDate}`;
    const items = Array.isArray(report.items) ? report.items : [];
    const itemBlocks = items.map(summarizeItem);
    const textLines = [
      `TRO Tracker 日报`,
      `日期：${localDate}`,
      ``,
      `当日新增案件：${report.newCasesCount}`,
      `当日新增 docket entries：${report.newDocketEntriesCount}`,
      ``,
      `重点案件摘要：`,
      ...(itemBlocks.length ? itemBlocks.map((item) => item.text) : ["今天没有新增案件或新增 docket entry。"])
    ];

    const html = `
      <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; line-height: 1.6;">
        <h2>TRO Tracker 日报</h2>
        <p><strong>日期：</strong>${escapeHtml(localDate)}</p>
        <p><strong>当日新增案件：</strong>${report.newCasesCount}<br>
        <strong>当日新增 docket entries：</strong>${report.newDocketEntriesCount}</p>
        <h3>重点案件摘要</h3>
        ${itemBlocks.length ? `<ol>${itemBlocks.map((item) => item.html).join("")}</ol>` : "<p>今天没有新增案件或新增 docket entry。</p>"}
      </div>
    `.trim();

    return {
      subject,
      text: textLines.join("\n"),
      html
    };
  }
}
