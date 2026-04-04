function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function decodeHtml(value) {
  return String(value || "")
    .replace(/&#(\d+);/g, (_, code) => String.fromCharCode(Number(code)))
    .replace(/&#x([0-9a-f]+);/gi, (_, code) => String.fromCharCode(Number.parseInt(code, 16)))
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'");
}

function stripTags(value) {
  return String(value || "").replace(/<[^>]+>/g, " ");
}

function cleanText(value) {
  return decodeHtml(stripTags(value)).replace(/\s+/g, " ").trim();
}

function normalizeLookupText(value) {
  return cleanText(value).toLowerCase().replace(/[^\w]+/g, " ").replace(/\s+/g, " ").trim();
}

function absoluteUrl(value, baseUrl) {
  const raw = String(value || "").trim();
  if (!raw) {
    return null;
  }

  try {
    return new URL(raw, baseUrl).toString();
  } catch {
    return null;
  }
}

function extractHref(value, baseUrl) {
  const match = String(value || "").match(/href=['"]([^'"]+)['"]/i);
  return absoluteUrl(match?.[1] || "", baseUrl);
}

function extractRows(html) {
  return [...String(html || "").matchAll(/<tr\b[^>]*>([\s\S]*?)<\/tr>/gi)].map((match) => match[1]);
}

function extractCells(rowHtml) {
  return [...String(rowHtml || "").matchAll(/<t[dh]\b[^>]*>([\s\S]*?)<\/t[dh]>/gi)].map((match) => match[1]);
}

function extractAspNetHiddenFields(html) {
  const fields = {};
  for (const match of String(html || "").matchAll(/<input[^>]+type="hidden"[^>]+name="([^"]+)"[^>]+value="([^"]*)"[^>]*>/gi)) {
    fields[match[1]] = decodeHtml(match[2]);
  }
  return fields;
}

function parseUsDate(value) {
  const match = String(value || "").match(/\b(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b/);
  if (!match) {
    return null;
  }

  const year = match[3].length === 2 ? `20${match[3]}` : match[3];
  return `${year}-${match[1].padStart(2, "0")}-${match[2].padStart(2, "0")}`;
}

function extractCoreDocketNumber(value) {
  const match = String(value || "").match(/\b(?:(\d+):)?(?:(?:20)?(\d{2}))-([a-z]{2})-(\d{3,6})\b/i);
  if (!match) {
    return "";
  }

  const division = match[1] ? `${match[1]}:` : "";
  return `${division}${match[2]}-${String(match[3] || "").toLowerCase()}-${match[4]}`;
}

function extractPacerCaseId(value) {
  const match = String(value || "").match(/[?&](\d{3,})\b/);
  return match ? match[1] : null;
}

function trimCourtRows(items = [], maxItems = 100) {
  return items
    .filter((item) => item?.docketNumber)
    .slice(0, Math.max(Number(maxItems || 0), 0) || items.length);
}

function recentFilingsMatchesCourtName(source, courtName = "") {
  const left = normalizeLookupText(source?.courtName || "");
  const right = normalizeLookupText(courtName);
  if (!left || !right) {
    return false;
  }

  return left === right || left.includes(right) || right.includes(left);
}

function parseIlndRows(html, source) {
  const tableMatch = String(html || "").match(/<table[^>]+id="ContentPlaceHolder1_GridView1"[\s\S]*?<\/table>/i);
  if (!tableMatch) {
    return [];
  }

  const rows = [];
  for (const rowHtml of extractRows(tableMatch[0])) {
    const cells = extractCells(rowHtml);
    if (cells.length < 4) {
      continue;
    }

    const docketNumber = extractCoreDocketNumber(cleanText(cells[0]));
    if (!docketNumber || !/\b\d{2}-cv-\d{3,6}\b/i.test(docketNumber)) {
      continue;
    }

    rows.push({
      sourceId: source.id,
      courtId: source.courtId,
      courtName: source.courtName,
      docketNumber,
      rawDocketNumber: cleanText(cells[0]),
      caseName: cleanText(cells[1]),
      judge: cleanText(cells[2]),
      dateFiled: parseUsDate(cells[3]),
      caseUrl: extractHref(cells[0], source.url),
      docketUrl: extractHref(cells[0], source.url),
      sourceUrl: source.url,
      pacerCaseId: null,
      natureOfSuit: null,
      cause: null,
      category: "Civil"
    });
  }

  return rows;
}

function hasIlndNextPage(html) {
  return /Page\$Next/i.test(String(html || ""));
}

function parseCandRows(html, source) {
  const tableMatch = String(html || "").match(/<table[^>]*class="[^"]*\bviews-table\b[^"]*"[\s\S]*?<\/table>/i);
  if (!tableMatch) {
    return [];
  }

  const tbodyMatch = tableMatch[0].match(/<tbody\b[^>]*>([\s\S]*?)<\/tbody>/i);
  const body = tbodyMatch ? tbodyMatch[1] : tableMatch[0];
  const rows = [];

  for (const rowHtml of extractRows(body)) {
    const cells = extractCells(rowHtml);
    if (cells.length < 5) {
      continue;
    }

    const dateFiled = parseUsDate(cells[0]);
    const category = cleanText(cells[1]);
    const caseName = cleanText(cells[2]);
    const caseUrl = extractHref(cells[2], source.url);
    const docketRaw = cleanText(cells[3]);
    const docketNumber = extractCoreDocketNumber(docketRaw);
    const docketUrl = extractHref(cells[3], source.url);
    const basisAndNature = cleanText(cells[4]);
    const judge = cleanText(cells[5] || "");
    const location = cleanText(cells[6] || "");

    if (!docketNumber || !/\b\d{2}-cv-\d{3,6}\b/i.test(docketNumber)) {
      continue;
    }

    rows.push({
      sourceId: source.id,
      courtId: source.courtId,
      courtName: source.courtName,
      docketNumber,
      rawDocketNumber: docketRaw,
      caseName,
      judge,
      dateFiled,
      caseUrl,
      docketUrl,
      sourceUrl: source.url,
      pacerCaseId: extractPacerCaseId(docketUrl),
      natureOfSuit: basisAndNature || null,
      cause: null,
      category,
      location: location || null
    });
  }

  return rows;
}

function parseFlsdRows(html, source, iframeUrl = null) {
  const tableMatch = String(html || "").match(/<table[^>]+id="casescvlast7"[\s\S]*?<\/table>/i);
  if (!tableMatch) {
    return [];
  }

  const rows = [];
  const body = tableMatch[0].match(/<tbody\b[^>]*>([\s\S]*?)<\/tbody>/i)?.[1] || tableMatch[0];

  for (const rowHtml of extractRows(body)) {
    const cells = extractCells(rowHtml);
    if (cells.length < 6) {
      continue;
    }

    const docketRaw = cleanText(cells[0]);
    const docketNumber = extractCoreDocketNumber(docketRaw);
    if (!docketNumber || !/\b\d{2}-cv-\d{3,6}\b/i.test(docketNumber)) {
      continue;
    }

    rows.push({
      sourceId: source.id,
      courtId: source.courtId,
      courtName: source.courtName,
      docketNumber,
      rawDocketNumber: docketRaw,
      caseName: cleanText(cells[1]),
      judge: cleanText(cells[2]),
      dateFiled: parseUsDate(cells[3]),
      caseUrl: null,
      docketUrl: null,
      sourceUrl: iframeUrl || source.url,
      pacerCaseId: null,
      natureOfSuit: cleanText(cells[4]) || null,
      cause: cleanText(cells[5]) || null,
      category: "Civil"
    });
  }

  return rows;
}

const DEFAULT_RECENT_FILINGS_SOURCES = [
  {
    id: "ilnd",
    courtId: "ilnd",
    courtName: "Northern District of Illinois",
    url: "https://www.ilnd.uscourts.gov/RecentlyFiledCase.aspx",
    parser: "ilnd-grid"
  },
  {
    id: "cand",
    courtId: "cand",
    courtName: "Northern District of California",
    url: "https://cand.uscourts.gov/cases-e-filing/cases/recently-filed-cases",
    parser: "cand-table"
  },
  {
    id: "flsd",
    courtId: "flsd",
    courtName: "Southern District of Florida",
    url: "https://www.flsd.uscourts.gov/recent-civil-filings",
    parser: "flsd-iframe"
  }
];

export class RecentFilingsClient {
  constructor(config) {
    this.enabled = Boolean(config.enabled);
    this.timeoutMs = Number(config.timeoutMs || 20000);
    this.minIntervalMs = Number(config.minIntervalMs || 1200);
    this.maxItemsPerCourt = Number(config.maxItemsPerCourt || 120);
    this.maxPagesPerCourt = Number(config.maxPagesPerCourt || 3);
    this.maxLookupsPerRun = Number(config.maxLookupsPerRun || 12);
    this.selectedIds = new Set((config.courts || []).map((value) => String(value || "").trim().toLowerCase()).filter(Boolean));
    this.sources = DEFAULT_RECENT_FILINGS_SOURCES.filter((source) => !this.selectedIds.size || this.selectedIds.has(source.id));
    this.lastRequestAt = 0;
  }

  getStatus() {
    return {
      enabled: this.enabled,
      state: this.enabled ? "ready" : "disabled",
      trackedCourts: this.sources.length,
      sources: this.sources.map((source) => ({
        id: source.id,
        court_id: source.courtId,
        court_name: source.courtName,
        url: source.url
      }))
    };
  }

  listSources() {
    return this.sources.map((source) => ({ ...source }));
  }

  async fetchUrl(url) {
    const elapsed = Date.now() - this.lastRequestAt;
    if (elapsed < this.minIntervalMs) {
      await wait(this.minIntervalMs - elapsed);
    }

    const response = await fetch(url, {
      headers: {
        "user-agent": "tro-case-watch/1.0"
      },
      signal: AbortSignal.timeout(this.timeoutMs)
    });

    this.lastRequestAt = Date.now();

    if (!response.ok) {
      throw new Error(`request failed: ${response.status}`);
    }

    return response.text();
  }

  async postForm(url, body) {
    const elapsed = Date.now() - this.lastRequestAt;
    if (elapsed < this.minIntervalMs) {
      await wait(this.minIntervalMs - elapsed);
    }

    const response = await fetch(url, {
      method: "POST",
      headers: {
        "content-type": "application/x-www-form-urlencoded",
        "user-agent": "tro-case-watch/1.0"
      },
      body: new URLSearchParams(body).toString(),
      signal: AbortSignal.timeout(this.timeoutMs)
    });

    this.lastRequestAt = Date.now();

    if (!response.ok) {
      throw new Error(`request failed: ${response.status}`);
    }

    return response.text();
  }

  async fetchIlndRecentPages(source, firstPageHtml = null) {
    const firstPage = firstPageHtml || (await this.fetchUrl(source.url));
    const pages = [firstPage];
    let currentHtml = firstPage;

    for (let page = 1; page < this.maxPagesPerCourt; page += 1) {
      if (!hasIlndNextPage(currentHtml)) {
        break;
      }

      const hidden = extractAspNetHiddenFields(currentHtml);
      if (!hidden.__VIEWSTATE) {
        break;
      }

      currentHtml = await this.postForm(source.url, {
        __EVENTTARGET: "ctl00$ContentPlaceHolder1$GridView1",
        __EVENTARGUMENT: "Page$Next",
        __LASTFOCUS: "",
        __VIEWSTATE: hidden.__VIEWSTATE || "",
        __VIEWSTATEGENERATOR: hidden.__VIEWSTATEGENERATOR || "",
        __EVENTVALIDATION: hidden.__EVENTVALIDATION || ""
      });
      pages.push(currentHtml);

      if (pages.flatMap((html) => parseIlndRows(html, source)).length >= this.maxItemsPerCourt) {
        break;
      }
    }

    return pages;
  }

  async fetchRecentForCourt(source) {
    const pageHtml = await this.fetchUrl(source.url);
    let items = [];
    let note = null;

    if (source.parser === "ilnd-grid") {
      const pages = await this.fetchIlndRecentPages(source, pageHtml);
      const seen = new Set();
      items = pages
        .flatMap((html) => parseIlndRows(html, source))
        .filter((item) => {
          const key = `${item.docketNumber}|${item.caseName}|${item.dateFiled || ""}`;
          if (seen.has(key)) {
            return false;
          }
          seen.add(key);
          return true;
        });
      note = `Northern District of Illinois recently filed cases${pages.length > 1 ? ` (${pages.length} pages)` : ""}`;
    } else if (source.parser === "cand-table") {
      items = parseCandRows(pageHtml, source);
      note = "Northern District of California recently filed cases";
    } else if (source.parser === "flsd-iframe") {
      const iframeUrl = absoluteUrl(
        String(pageHtml).match(/<iframe\b[^>]+src=['"]([^'"]+)['"]/i)?.[1] || "",
        source.url
      );
      const iframeHtml = iframeUrl ? await this.fetchUrl(iframeUrl) : "";
      items = parseFlsdRows(iframeHtml, source, iframeUrl);
      note = iframeUrl
        ? "Southern District of Florida recent civil filings iframe"
        : "Southern District of Florida recent civil filings";
    }

    return {
      sourceId: source.id,
      sourceLabel: source.courtName,
      note,
      items: trimCourtRows(items, this.maxItemsPerCourt)
    };
  }

  async lookupByDocket(docketNumber, { courtName = "" } = {}) {
    if (!this.enabled) {
      return null;
    }

    const normalizedTarget = extractCoreDocketNumber(docketNumber);
    if (!normalizedTarget) {
      return null;
    }

    const prioritizedSources = [
      ...this.sources.filter((source) => recentFilingsMatchesCourtName(source, courtName)),
      ...this.sources.filter((source) => !recentFilingsMatchesCourtName(source, courtName))
    ];

    for (const source of prioritizedSources) {
      const result = await this.fetchRecentForCourt(source);
      const match = (result.items || []).find(
        (item) => extractCoreDocketNumber(item.docketNumber) === normalizedTarget
      );

      if (match) {
        return {
          source,
          item: match,
          note: result.note || `${source.courtName} recently filed lookup`
        };
      }
    }

    return null;
  }
}
