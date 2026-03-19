import { normalizeDocket, normalizeText } from "../insights.js";

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function stripTags(value) {
  return String(value || "").replace(/<[^>]+>/g, " ");
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

function cleanText(value) {
  return decodeHtml(stripTags(value)).replace(/\s+/g, " ").trim();
}

function parseFiledAt(value) {
  const match = String(value || "").match(/\b(\d{2})\/(\d{2})\/(\d{4})\b/);
  if (!match) {
    return null;
  }

  return `${match[3]}-${match[1]}-${match[2]}`;
}

function canonicalizeUrl(value) {
  const url = new URL(value);
  url.search = "";
  url.hash = "";
  return url.toString();
}

function decodeDuckResultUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return null;
  }

  const absolute = raw.startsWith("//") ? `https:${raw}` : raw;
  try {
    const url = new URL(absolute);
    const redirect = url.searchParams.get("uddg");
    return canonicalizeUrl(redirect || absolute);
  } catch {
    return null;
  }
}

function buildCourtSearchText(caseRow) {
  const courtName = String(caseRow.court_name || "").trim();
  if (!courtName) {
    return "";
  }

  return courtName
    .replace(/\bU\.?S\.?\b/gi, "United States")
    .replace(/[.]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function buildCourtSearchVariants(caseRow) {
  const courtName = buildCourtSearchText(caseRow);
  if (!courtName) {
    return [];
  }

  const variants = new Set([courtName]);
  const simplified = courtName
    .replace(/\bDistrict Court for the\b/gi, "")
    .replace(/\bUnited States\b/gi, "")
    .replace(/[.]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (simplified) {
    variants.add(simplified);
  }

  const districtMatch = simplified.match(/\b(?:Northern|Southern|Eastern|Western|Central|Middle)\s+District\s+of\s+[A-Za-z .-]+\b/i);
  if (districtMatch?.[0]) {
    variants.add(districtMatch[0].trim());
  }

  const stateMatch = simplified.match(/\bDistrict\s+of\s+[A-Za-z .-]+\b/i);
  if (stateMatch?.[0]) {
    variants.add(stateMatch[0].trim());
  }

  return [...variants].filter(Boolean);
}

function buildPlaintiffSearchText(caseRow) {
  const plaintiff = caseRow.plaintiffs?.[0] || caseRow.case_name?.split(/\s+v\.?\s+/i)?.[0];
  return String(plaintiff || "").replace(/\s+/g, " ").trim();
}

function buildCaseNameSearchText(caseRow) {
  return String(caseRow.case_name || "").replace(/\s+/g, " ").trim();
}

function normalizePublicCaseUrl(value) {
  const url = String(value || "").trim();
  if (!url || !/pacermonitor\.com\/public\/case\//i.test(url)) {
    return null;
  }

  try {
    return canonicalizeUrl(url);
  } catch {
    return null;
  }
}

export class PacerMonitorAdapter {
  constructor(config) {
    this.enabled = Boolean(config.enabled);
    this.baseUrl = String(config.baseUrl || "https://www.pacermonitor.com").replace(/\/$/, "");
    this.publicSearchBaseUrl = String(config.publicSearchBaseUrl || "https://html.duckduckgo.com/html/").replace(/\/$/, "");
    this.apiKey = config.apiKey || "";
    this.minIntervalMs = Number(config.minIntervalMs || 2000);
    this.timeoutMs = Number(config.timeoutMs || 15000);
    this.maxSearchQueries = Math.max(1, Number(config.maxSearchQueries || 3));
    this.lastRequestAt = 0;
  }

  getStatus() {
    if (!this.enabled) {
      return {
        enabled: false,
        state: "disabled",
        note: "默认关闭。"
      };
    }

    return {
      enabled: true,
      state: "public-exact-lookup",
      note: "以精确案号做低频补充，不参与全国发现；详情页和后台补缺口都会尝试，遇到验证码或限流会缓存跳过。"
    };
  }

  async syncRecent() {
    return {
      provider: "pacermonitor",
      ...this.getStatus(),
      syncedCases: 0,
      note: "PACERMonitor 仅做精确案号补充，不参与全国发现。"
    };
  }

  async enrichCase(caseRow) {
    if (!this.enabled) {
      return null;
    }

    const docketNumber = String(caseRow.docket_number || "").trim();
    if (!docketNumber || !/\b\d{2}-cv-\d{3,6}\b/i.test(docketNumber)) {
      return null;
    }

    const caseUrl = await this.lookupCaseUrl(caseRow);
    if (!caseUrl) {
      return {
        state: "not_found",
        url: null,
        title: null,
        entries: [],
        syncedAt: new Date().toISOString()
      };
    }

    const page = await this.fetchPage(caseUrl);
    const text = page.text || "";
    if (page.status === 429 || this.isChallengePage(text)) {
      return {
        state: page.status === 429 ? "rate_limited" : "challenge",
        url: caseUrl,
        title: this.extractTitle(text),
        entries: [],
        syncedAt: new Date().toISOString()
      };
    }

    const payload = this.parseCasePage(text, caseUrl, caseRow);
    return {
      ...payload,
      state: payload.entries.length ? "ok" : "empty",
      syncedAt: new Date().toISOString()
    };
  }

  async lookupCaseUrl(caseRow) {
    const existing = [
      caseRow.raw?.pacermonitor?.caseUrl,
      ...(Array.isArray(caseRow.source_urls) ? caseRow.source_urls : [])
    ]
      .map(normalizePublicCaseUrl)
      .filter(Boolean);

    if (existing.length) {
      return existing[0];
    }

    const queries = this.buildSearchQueries(caseRow);
    for (const query of queries.slice(0, this.maxSearchQueries)) {
      const url = await this.searchPublicCaseUrl(query, caseRow.docket_number);
      if (url) {
        return url;
      }
    }

    return null;
  }

  buildSearchQueries(caseRow) {
    const docketNumber = String(caseRow.docket_number || "").trim();
    const normalizedDocket = normalizeDocket(docketNumber);
    const courtVariants = buildCourtSearchVariants(caseRow);
    const plaintiff = buildPlaintiffSearchText(caseRow);
    const caseName = buildCaseNameSearchText(caseRow);
    const queries = [
      ...courtVariants.flatMap((courtName) => [
        `site:pacermonitor.com/public/case "${docketNumber}" "${courtName}"`,
        normalizedDocket ? `site:pacermonitor.com/public/case "${normalizedDocket}" "${courtName}"` : ""
      ]),
      caseName ? `site:pacermonitor.com/public/case "${docketNumber}" "${caseName}"` : "",
      caseName && normalizedDocket ? `site:pacermonitor.com/public/case "${normalizedDocket}" "${caseName}"` : "",
      plaintiff ? `site:pacermonitor.com/public/case "${docketNumber}" "${plaintiff}"` : "",
      plaintiff && normalizedDocket ? `site:pacermonitor.com/public/case "${normalizedDocket}" "${plaintiff}"` : "",
      `site:pacermonitor.com/public/case "${docketNumber}" pacermonitor`
    ];

    return [...new Set(queries.map((query) => query.trim()).filter(Boolean))];
  }

  async searchPublicCaseUrl(query, docketNumber = "") {
    const url = new URL(`${this.publicSearchBaseUrl}/`);
    url.searchParams.set("q", query);

    const page = await this.fetchPage(url.toString());
    const links = [...page.text.matchAll(/class="result__a" href="([^"]+)"/g)]
      .map((match) => decodeDuckResultUrl(match[1]))
      .filter((value) => normalizePublicCaseUrl(value));

    const docketNeedle = normalizeDocket(docketNumber);
    const exact = links.find((link) => normalizeText(link).includes(docketNeedle));
    return exact || links[0] || null;
  }

  parseCasePage(html, pageUrl, caseRow) {
    const entries = [];
    let lastEntry = null;
    const rowPattern = /<tr[^>]*class="([^"]*\bdocketItem(?:Row|RowService|AttachService)\b[^"]*)"[^>]*>([\s\S]*?)<\/tr>/gi;
    let match;

    while ((match = rowPattern.exec(html)) !== null) {
      const className = String(match[1] || "");
      const rowHtml = String(match[2] || "");
      const number = cleanText(
        rowHtml.match(/<td[^>]*class="[^"]*\bdocketItemNum\b[^"]*"[^>]*>([\s\S]*?)<\/td>/i)?.[1]
      );
      const text = cleanText(
        rowHtml.match(/<td[^>]*class="[^"]*\bdocketItemText(?:Attach)?\b[^"]*"[^>]*>([\s\S]*?)<\/td>/i)?.[1]
      );

      if (!text) {
        continue;
      }

      if (className.includes("docketItemAttachService") && lastEntry) {
        lastEntry.description = `${lastEntry.description} 附件: ${text}`;
        continue;
      }

      const filedAt = parseFiledAt(text);
      const entry = {
        row_number: number || null,
        filed_at: filedAt,
        description: text
      };

      entries.push(entry);
      lastEntry = entry;
    }

    const title = this.extractTitle(html);
    const metaDescription = cleanText(html.match(/<meta name="description" content="([\s\S]*?)"/i)?.[1]);
    const canonicalUrl = canonicalizeUrl(pageUrl);

    return {
      url: canonicalUrl,
      title,
      metaDescription: metaDescription || null,
      caseName: caseRow.case_name || null,
      entries
    };
  }

  extractTitle(html) {
    return cleanText(html.match(/<title>([\s\S]*?)<\/title>/i)?.[1]) || null;
  }

  isChallengePage(html) {
    const text = String(html || "");
    return (
      /id="recaptchaform"/i.test(text) ||
      /action="\/recaptcha"/i.test(text) ||
      /Please wait\.\.\./i.test(text) ||
      (/Welcome to PacerMonitor/i.test(text) && /grecaptcha\.execute/i.test(text))
    );
  }

  async fetchPage(url, options = {}) {
    const elapsed = Date.now() - this.lastRequestAt;
    if (elapsed < this.minIntervalMs) {
      await wait(this.minIntervalMs - elapsed);
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const response = await fetch(url, {
        ...options,
        headers: {
          "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
          accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          ...options.headers
        },
        signal: controller.signal
      });

      const text = await response.text();
      this.lastRequestAt = Date.now();
      return {
        status: response.status,
        url: response.url || url,
        text
      };
    } finally {
      clearTimeout(timeout);
    }
  }
}
