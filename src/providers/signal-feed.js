const DEFAULT_USER_AGENT =
  "Mozilla/5.0 (compatible; TROTracker/1.0; +https://www.trotracker.com)";

export const SIGNAL_FEED_PROVIDER_KEY = "signal-feed";

function htmlDecode(value) {
  return String(value || "")
    .replace(/&#x([0-9a-f]+);?/gi, (_, value) => {
      try {
        return String.fromCodePoint(Number.parseInt(value, 16));
      } catch {
        return _;
      }
    })
    .replace(/&#(\d+);?/g, (_, value) => {
      try {
        return String.fromCodePoint(Number(value));
      } catch {
        return _;
      }
    })
    .replace(/&nbsp;/gi, " ")
    .replace(/&quot;/g, '"')
    .replace(/&#39;|&apos;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&");
}

function stripHtml(value) {
  return htmlDecode(String(value || "")
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, " ")
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, " ")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/(?:p|div|li|h[1-6]|tr|td|section|article|button)>/gi, "\n")
    .replace(/<[^>]+>/g, " "))
    .replace(/[ \t\r\f]+/g, " ")
    .replace(/\n\s+/g, "\n")
    .trim();
}

function removeNonContentHtml(value) {
  return String(value || "")
    .replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, " ")
    .replace(/<style\b[^>]*>[\s\S]*?<\/style>/gi, " ");
}

function readHtmlAttribute(tag, name) {
  const pattern = new RegExp(`\\b${name}\\s*=\\s*(?:"([^"]*)"|'([^']*)')`, "i");
  const match = String(tag || "").match(pattern);
  return match ? htmlDecode(match[1] ?? match[2] ?? "") : null;
}

function normalizeSignalDate(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return null;
  }
  if (/^\d{4}-\d{2}-\d{2}T/.test(raw)) {
    return raw.slice(0, 10);
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return raw;
  }
  const parsed = new Date(`${raw} 00:00:00 UTC`);
  return Number.isFinite(parsed.getTime()) ? parsed.toISOString().slice(0, 10) : raw;
}

function parseDateFromText(value) {
  const match = String(value || "").match(
    /\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+20\d{2}\b|\b20\d{2}-\d{2}-\d{2}\b/i
  );
  return match ? normalizeSignalDate(match[0]) : null;
}

function parseLastDateFromText(value) {
  const matches = [...String(value || "").matchAll(
    /\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+20\d{2}\b|\b20\d{2}-\d{2}-\d{2}\b/gi
  )];
  return matches.length ? normalizeSignalDate(matches.at(-1)[0]) : null;
}

function extractTagValues(html, tagName) {
  const pattern = new RegExp(`<${tagName}\\b[^>]*>([\\s\\S]*?)</${tagName}>`, "gi");
  return [...String(html || "").matchAll(pattern)].map((match) => stripHtml(match[1])).filter(Boolean);
}

function readRenderedField(text, label) {
  const lines = String(text || "")
    .split(/\n+/)
    .map((line) => line.trim())
    .filter(Boolean);
  const normalizedLabel = String(label || "").trim().toLowerCase();
  const index = lines.findIndex((line) => line.toLowerCase() === normalizedLabel);
  if (index >= 0 && lines[index + 1]) {
    return lines[index + 1];
  }

  const inline = String(text || "").match(new RegExp(`\\b${String(label).replace(/[.*+?^${}()|[\\]\\\\]/g, "\\\\$&")}\\b\\s+([^\\n]+)`, "i"));
  return inline?.[1]?.trim() || null;
}

function detailFromItem(item, detailUrl) {
  if (!item?.case_number) {
    return null;
  }

  return {
    docketId: item.docket_id || null,
    docketNumber: normalizeDocketNumber(item.case_number),
    caseName: item.case_name || null,
    caseUrl: item.case_url || null,
    detailUrl,
    plaintiff: item.plaintiff || null,
    plaintiffs: parseJsonArray(item.plaintiffs_json, item.plaintiff),
    defendant: item.defendant || null,
    defendants: item.defendant ? [item.defendant] : [],
    courtId: item.court_id || null,
    courtName: item.court || null,
    dateFiled: normalizeSignalDate(item.case_date),
    status: item.is_active === false ? "terminated" : "open",
    judge: item.judge || null,
    counsel: item.plaintiff_counsel || null,
    counselFirm: item.plaintiff_counsel_firm || null,
    entries: (Array.isArray(item.entries) ? item.entries : []).map((entry) => ({
      entryId: entry.entry_id || entry.db_doc_id || null,
      title: entry.title || null,
      filedAt: normalizeSignalDate(entry.entry_date),
      documentNumber: entry.doc_number ?? null,
      documentUrl: entry.docLink || null,
      downloadable: Boolean(entry.downloadable),
      restricted: Boolean(entry.restricted),
      attachments: (Array.isArray(entry.attachments) ? entry.attachments : []).map((attachment) => ({
        id: attachment.id ?? null,
        title: attachment.title || null,
        number: attachment.att_number ?? null,
        documentUrl: attachment.docLink || null,
        downloadable: Boolean(attachment.downloadable),
        restricted: Boolean(attachment.restricted)
      }))
    }))
  };
}

function parseAstroCase(html, detailUrl) {
  const islands = [...removeNonContentHtml(html).matchAll(/<astro-island\b[^>]*>/gi)];
  for (const island of islands) {
    const tag = island[0];
    const propsValue = readHtmlAttribute(tag, "props");
    if (!propsValue) {
      continue;
    }

    try {
      const props = unpackAstroValue(JSON.parse(propsValue));
      const item = detailFromItem(props?.troCase, detailUrl);
      if (item) {
        return item;
      }
    } catch {
      // Continue to the rendered HTML fallback when the island format changes.
    }
  }

  return null;
}

function parseRenderedEntries(html, detailUrl) {
  const source = removeNonContentHtml(html);
  const headings = [...source.matchAll(/<h3\b[^>]*>([\s\S]*?)<\/h3>/gi)];
  const occurrenceByKey = new Map();
  const entries = [];

  for (let index = 0; index < headings.length; index += 1) {
    const heading = headings[index];
    const title = stripHtml(heading[1]);
    if (!title) {
      continue;
    }

    const start = Number(heading.index || 0);
    const prefix = source.slice(Math.max(0, start - 1400), start);
    const filedAt = parseLastDateFromText(prefix.slice(-900));
    if (!filedAt) {
      continue;
    }

    const end = index + 1 < headings.length
      ? Number(headings[index + 1].index || source.length)
      : Math.min(source.length, start + 4000);
    // Only read attachments after this heading, never from the preceding entry.
    const block = source.slice(start, end);
    const attachmentMatches = [...stripHtml(block).matchAll(
      /Document\s+Attachment\s+(\d+)\s*:\s*([\s\S]*?)(?=Document\s+Attachment\s+\d+\s*:|Document\s*$|$)/gi
    )];
    const attachments = attachmentMatches.map((match) => ({
      id: null,
      title: String(match[2] || "").replace(/\s+/g, " ").trim() || null,
      number: Number(match[1]) || null,
      documentUrl: null,
      downloadable: false,
      restricted: false
    }));
    const documentUrlMatch = block.match(/href=["']([^"']+\.(?:pdf|docx?|txt)(?:\?[^"']*)?)["']/i);
    const documentUrl = documentUrlMatch ? absoluteUrl(htmlDecode(documentUrlMatch[1]), detailUrl) : null;
    const key = `${filedAt.slice(0, 10)}:${title.toLowerCase().replace(/\s+/g, " ").trim()}`;
    const occurrence = Number(occurrenceByKey.get(key) || 0) + 1;
    occurrenceByKey.set(key, occurrence);

    entries.push({
      entryId: `rendered:${key}:${occurrence}`,
      title,
      filedAt,
      documentNumber: null,
      documentUrl,
      downloadable: Boolean(documentUrl),
      restricted: false,
      attachments
    });
  }

  return entries;
}

function parseRenderedPublicDetail(html, detailUrl) {
  const source = removeNonContentHtml(html);
  const plainText = stripHtml(source);
  const docketNumber = normalizeDocketNumber(
    plainText.match(/\b\d+:20\d{2}-cv-\d+\b/i)?.[0] || ""
  );
  if (!docketNumber) {
    return null;
  }

  const heading = extractTagValues(source, "h1")[0] || "";
  const caseName = heading.replace(new RegExp(`^#?\\s*${docketNumber.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\\\$&")}\\s*`, "i"), "").trim() || heading || null;
  const dateFiled = parseDateFromText(readRenderedField(plainText, "Filing Date") || "");
  const plaintiff = readRenderedField(plainText, "Plaintiff");
  const court = readRenderedField(plainText, "Court");
  const entries = parseRenderedEntries(source, detailUrl);

  return {
    docketId: null,
    docketNumber,
    caseName,
    caseUrl: detailUrl,
    detailUrl,
    plaintiff,
    plaintiffs: plaintiff ? [plaintiff] : [],
    defendants: [],
    courtId: null,
    courtName: court,
    dateFiled,
    status: "open",
    judge: null,
    counsel: null,
    counselFirm: null,
    entries
  };
}

function normalizeDocketNumber(value) {
  return String(value || "")
    .trim()
    .replace(/^(\d+):20(\d{2})-cv-/i, "$1:$2-cv-")
    .replace(/\s+/g, "");
}

function unpackAstroValue(value) {
  if (Array.isArray(value)) {
    if (value.length === 2 && Number.isInteger(value[0])) {
      if (value[0] === 0) {
        return unpackAstroValue(value[1]);
      }
      if (value[0] === 1) {
        return Array.isArray(value[1]) ? value[1].map(unpackAstroValue) : [];
      }
    }
    return value.map(unpackAstroValue);
  }

  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, nestedValue]) => [key, unpackAstroValue(nestedValue)])
    );
  }

  return value;
}

function absoluteUrl(value, baseUrl) {
  try {
    return new URL(String(value || ""), baseUrl).toString();
  } catch {
    return null;
  }
}

function parsePublicRows(html, publicCasesUrl) {
  const items = [];
  const rows = String(html || "").match(/<tr\b[^>]*>[\s\S]*?<\/tr>/gi) || [];

  for (const row of rows) {
    const linkMatch = row.match(/href="([^"]*\/cases\/[^"]+)"/i);
    const docketMatch = row.match(/>(\d+:20\d{2}-cv-\d+)</i);
    if (!linkMatch || !docketMatch) {
      continue;
    }

    const titleValues = [...row.matchAll(/\btitle="([^"]+)"/gi)].map((match) => htmlDecode(match[1]).trim());
    const dateMatch = row.match(/>([A-Z][a-z]{2}\s+\d{1,2},\s+20\d{2})<\/td>/);
    const detailUrl = absoluteUrl(htmlDecode(linkMatch[1]), publicCasesUrl);
    const idMatch = String(linkMatch[1]).match(/\/cases\/([a-f0-9]{32})-/i);
    const filedAt = dateMatch ? new Date(`${dateMatch[1]} 00:00:00 UTC`) : null;

    items.push({
      docketId: idMatch?.[1] || normalizeDocketNumber(docketMatch[1]),
      docketNumber: normalizeDocketNumber(docketMatch[1]),
      plaintiff: titleValues[0] || null,
      courtName: titleValues[1] || null,
      dateFiled: filedAt && Number.isFinite(filedAt.getTime()) ? filedAt.toISOString().slice(0, 10) : null,
      detailUrl
    });
  }

  return [...new Map(items.map((item) => [item.docketId, item])).values()];
}

function parsePublicDetail(html, detailUrl) {
  return parseAstroCase(html, detailUrl) || parseRenderedPublicDetail(html, detailUrl);
}

function parseJsonArray(rawValue, fallbackValue = null) {
  try {
    const parsed = JSON.parse(String(rawValue || "[]"));
    if (Array.isArray(parsed)) {
      return parsed.map((value) => String(value || "").trim()).filter(Boolean);
    }
  } catch {}
  return fallbackValue ? [String(fallbackValue).trim()].filter(Boolean) : [];
}

function normalizeApiRecentItem(item = {}) {
  return {
    docketId: item.docketID || item.docketId || null,
    docketNumber: normalizeDocketNumber(item.caseNumber),
    caseName: item.title || null,
    plaintiff: item.plaintiff || null,
    plaintiffs: item.plaintiff ? [item.plaintiff] : [],
    defendants: [],
    courtName: item.court || null,
    dateFiled: item.caseDate || null,
    entries: []
  };
}

function normalizeApiEntry(entry = {}) {
  return {
    entryId: entry.entryID || entry.entryId || null,
    title: entry.title || null,
    filedAt: entry.entryDate || null,
    documentNumber: entry.docNumber ?? null,
    documentUrl: entry.docLink || null,
    downloadable: Boolean(entry.downloadable),
    restricted: Boolean(entry.restricted),
    attachments: (Array.isArray(entry.attachments) ? entry.attachments : []).map((attachment) => ({
      id: attachment.id ?? null,
      title: attachment.title || null,
      number: attachment.attachmentNumber ?? attachment.attNumber ?? null,
      documentUrl: attachment.docLink || null,
      downloadable: Boolean(attachment.downloadable),
      restricted: Boolean(attachment.restricted)
    }))
  };
}

function signalFeedError(code, message, details = {}) {
  const error = new Error(message);
  error.code = code;
  Object.assign(error, details);
  return error;
}

function failureCode(error) {
  const status = Number(error?.status || 0);
  if (status) {
    return `http-${status}`;
  }
  if (error?.code === "EMPTY_DISCOVERY") {
    return "empty-discovery";
  }
  if (error?.name === "AbortError" || error?.code === "ETIMEDOUT") {
    return "timeout";
  }
  return "request-failed";
}

export class SignalFeedClient {
  constructor(options = {}) {
    this.enabled = Boolean(options.enabled && options.publicCasesUrl);
    this.publicCasesUrl = String(options.publicCasesUrl || "").trim();
    this.apiBaseUrl = String(options.apiBaseUrl || "").replace(/\/+$/, "");
    this.apiKey = String(options.apiKey || "").trim();
    this.timeoutMs = Math.max(Number(options.timeoutMs || 15_000), 1000);
    this.minIntervalMs = Math.max(Number(options.minIntervalMs || 1500), 0);
    this.recentDays = Math.min(Math.max(Number(options.recentDays || 7), 1), 7);
    this.recentLimit = Math.min(Math.max(Number(options.recentLimit || 100), 1), 100);
    // Public discovery exposes 15 rows; the authenticated API supports up to 100.
    this.maxCasesPerRun = Math.min(Math.max(Number(options.maxCasesPerRun || 100), 1), 100);
    this.lastRequestAt = 0;
  }

  getStatus() {
    return {
      enabled: this.enabled,
      state: this.enabled ? "ready" : "disabled",
      apiEnabled: Boolean(this.apiKey && this.apiBaseUrl),
      maxCasesPerRun: this.maxCasesPerRun
    };
  }

  async fetchRecent({ hydrate = true, maxCases = this.maxCasesPerRun } = {}) {
    if (!this.enabled) {
      return { items: [], source: "disabled" };
    }

    const candidates = this.apiKey && this.apiBaseUrl
      ? await this.fetchApiRecent()
      : await this.fetchPublicRecent();
    const selected = candidates.slice(0, Math.min(Math.max(Number(maxCases || 1), 1), this.maxCasesPerRun));
    if (!hydrate) {
      return { items: selected, source: this.apiKey ? "api" : "public" };
    }

    const items = [];
    const hydrationFailures = {};
    let hydratedItems = 0;
    for (const candidate of selected) {
      try {
        const detail = candidate.detailUrl
          ? await this.fetchPublicDetail(candidate.detailUrl)
          : this.apiKey
            ? await this.fetchApiEntries(candidate)
            : null;
        items.push({ ...candidate, ...(detail || {}) });
        if (detail?.entries?.length) {
          hydratedItems += 1;
        } else {
          hydrationFailures["no-entries"] = Number(hydrationFailures["no-entries"] || 0) + 1;
        }
      } catch (error) {
        const code = failureCode(error);
        hydrationFailures[code] = Number(hydrationFailures[code] || 0) + 1;
        items.push(candidate);
      }
    }

    return {
      items,
      source: this.apiKey ? "api" : "public",
      discoveredItems: candidates.length,
      hydratedItems,
      hydrationFailures
    };
  }

  async lookupByDocket(docketNumber) {
    if (!this.enabled) {
      return null;
    }

    const normalized = normalizeDocketNumber(docketNumber).toLowerCase();
    const candidates = this.apiKey && this.apiBaseUrl
      ? await this.fetchApiRecent()
      : await this.fetchPublicRecent();
    const candidate = candidates.find((item) => normalizeDocketNumber(item.docketNumber).toLowerCase() === normalized);
    if (!candidate) {
      return null;
    }

    const detail = candidate.detailUrl
      ? await this.fetchPublicDetail(candidate.detailUrl)
      : this.apiKey
        ? await this.fetchApiEntries(candidate)
        : null;
    return { ...candidate, ...(detail || {}) };
  }

  async fetchKnownCase(candidate = {}) {
    if (!this.enabled) {
      return null;
    }

    if (this.apiKey && this.apiBaseUrl && candidate.docketId) {
      return this.fetchApiEntries(candidate);
    }

    if (!candidate.detailUrl) {
      return null;
    }

    let lastError = null;
    for (const detailUrl of this.buildPublicDetailCandidates(candidate.detailUrl)) {
      try {
        const detail = await this.fetchPublicDetail(detailUrl);
        if (detail) {
          return { ...candidate, ...detail, detailUrl };
        }
      } catch (error) {
        lastError = error;
        if (Number(error?.status || 0) !== 404) {
          throw error;
        }
      }
    }

    if (lastError) {
      throw lastError;
    }
    return null;
  }

  buildPublicDetailCandidates(detailUrl) {
    const urls = new Set();
    const append = (value) => {
      const normalized = String(value || "").trim();
      if (!normalized) {
        return;
      }
      urls.add(normalized);
      urls.add(normalized.endsWith("/") ? normalized.slice(0, -1) : `${normalized}/`);
    };

    append(detailUrl);
    append(String(detailUrl || "").replace("/en/cases/", "/cases/"));
    append(String(detailUrl || "").replace("/cases/", "/en/cases/"));
    return [...urls];
  }

  async fetchPublicRecent() {
    const html = await this.requestText(this.publicCasesUrl);
    const rows = parsePublicRows(html, this.publicCasesUrl);
    if (rows.length) {
      return rows;
    }

    const pageUrl = new URL(this.publicCasesUrl);
    const languageRoot = pageUrl.pathname.startsWith("/en/") ? "/en/" : "/";
    const fallbackUrl = new URL(languageRoot, pageUrl.origin).toString();
    if (fallbackUrl === this.publicCasesUrl) {
      throw signalFeedError(
        "EMPTY_DISCOVERY",
        "signal feed public discovery returned no case rows"
      );
    }
    const fallbackHtml = await this.requestText(fallbackUrl);
    const fallbackRows = parsePublicRows(fallbackHtml, fallbackUrl);
    if (!fallbackRows.length) {
      throw signalFeedError(
        "EMPTY_DISCOVERY",
        "signal feed public discovery returned no case rows"
      );
    }
    return fallbackRows;
  }

  async fetchPublicDetail(detailUrl) {
    const html = await this.requestText(detailUrl);
    return parsePublicDetail(html, detailUrl);
  }

  async fetchApiRecent() {
    const payload = await this.requestJson(`${this.apiBaseUrl}/tro/recent`, {
      days: this.recentDays,
      limit: this.recentLimit
    });
    const rows = Array.isArray(payload?.data) ? payload.data : [];
    return rows.map(normalizeApiRecentItem).filter((item) => item.docketId && item.docketNumber);
  }

  async fetchApiEntries(candidate) {
    const payload = await this.requestJson(`${this.apiBaseUrl}/tro/entries`, {
      docketID: candidate.docketId
    });
    const rows = Array.isArray(payload?.data) ? payload.data : Array.isArray(payload?.data?.entries) ? payload.data.entries : [];
    return {
      ...candidate,
      entries: rows.map(normalizeApiEntry)
    };
  }

  async requestText(url) {
    await this.throttle();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const response = await fetch(url, {
        headers: {
          accept: "text/html,application/xhtml+xml",
          "user-agent": DEFAULT_USER_AGENT
        },
        signal: controller.signal
      });
      if (!response.ok) {
        const error = new Error(`signal feed request failed with HTTP ${response.status}`);
        error.status = response.status;
        throw error;
      }
      return await response.text();
    } finally {
      clearTimeout(timer);
      this.lastRequestAt = Date.now();
    }
  }

  async requestJson(url, body) {
    await this.throttle();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: {
          accept: "application/json",
          "content-type": "application/json",
          "user-agent": DEFAULT_USER_AGENT,
          "x-api-key": this.apiKey
        },
        body: JSON.stringify(body),
        signal: controller.signal
      });
      if (!response.ok) {
        const error = new Error(`signal feed API failed with HTTP ${response.status}`);
        error.status = response.status;
        throw error;
      }
      return await response.json();
    } finally {
      clearTimeout(timer);
      this.lastRequestAt = Date.now();
    }
  }

  async throttle() {
    const waitMs = this.minIntervalMs - (Date.now() - this.lastRequestAt);
    if (waitMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    }
  }
}

export const __test = {
  failureCode,
  normalizeDocketNumber,
  parsePublicRows,
  parsePublicDetail,
  unpackAstroValue
};
