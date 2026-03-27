import { normalizeDocket, normalizeText } from "../insights.js";

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
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

function normalizeLookupText(value) {
  return normalizeText(value).replace(/[^\w]+/g, " ").replace(/\s+/g, " ").trim();
}

function parseFiledAt(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return null;
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return raw;
  }

  const slash = raw.match(/\b(\d{1,2})\/(\d{1,2})\/(\d{4})\b/);
  if (slash) {
    return `${slash[3]}-${String(slash[1]).padStart(2, "0")}-${String(slash[2]).padStart(2, "0")}`;
  }

  const dashed = raw.match(/\b(\d{4})-(\d{1,2})-(\d{1,2})\b/);
  if (dashed) {
    return `${dashed[1]}-${String(dashed[2]).padStart(2, "0")}-${String(dashed[3]).padStart(2, "0")}`;
  }

  const long = Date.parse(raw);
  if (Number.isFinite(long)) {
    return new Date(long).toISOString().slice(0, 10);
  }

  return null;
}

function absoluteUrl(baseUrl, value) {
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

function buildPlaintiffSearchText(caseRow) {
  return String(
    caseRow.plaintiffs?.[0] ||
    String(caseRow.case_name || "").split(/\s+v\.?\s+/i)?.[0] ||
    ""
  ).replace(/\s+/g, " ").trim();
}

function buildCaseNameSearchText(caseRow) {
  return String(caseRow.case_name || "").replace(/\s+/g, " ").trim();
}

function buildPartySearchTerms(caseRow) {
  const values = [
    buildPlaintiffSearchText(caseRow),
    ...(caseRow.plaintiffs || []),
    ...(caseRow.defendants || []),
    ...(Array.isArray(caseRow.raw?.party) ? caseRow.raw.party : [])
  ];
  return [...new Set(values.map((value) => String(value || "").replace(/\s+/g, " ").trim()).filter(Boolean))];
}

function normalizeCourtLookupText(value) {
  return normalizeLookupText(
    String(value || "")
      .replace(/\bU\.?S\.?\b/gi, "United States")
      .replace(/\bDist\.?\b/gi, "District")
      .replace(/\bCt\.?\b/gi, "Court")
  );
}

function courtNamesLikelyMatch(expected, actual) {
  const left = normalizeCourtLookupText(expected);
  const right = normalizeCourtLookupText(actual);
  if (!left || !right) {
    return false;
  }

  return left === right || left.includes(right) || right.includes(left);
}

function compareScoreDesc(left, right) {
  return right.score - left.score;
}

function scoreSearchResult(result, caseRow) {
  const docketNeedle = normalizeDocket(caseRow.docket_number);
  const resultDocket = normalizeDocket(
    result.caseNumber ||
    result.case_number ||
    result.docketNumber ||
    result.docket_number ||
    result.docket
  );
  if (!docketNeedle || !resultDocket || docketNeedle !== resultDocket) {
    return -1;
  }

  let score = 1000;

  if (courtNamesLikelyMatch(caseRow.court_name, result.courtName || result.court_name || result.court)) {
    score += 250;
  }

  const caseNameNeedle = normalizeLookupText(buildCaseNameSearchText(caseRow));
  const resultTitle = normalizeLookupText(result.title || result.caseName || result.case_name);
  if (caseNameNeedle && resultTitle) {
    if (resultTitle.includes(caseNameNeedle) || caseNameNeedle.includes(resultTitle)) {
      score += 200;
    }
  }

  const partyTerms = buildPartySearchTerms(caseRow).map(normalizeLookupText).filter(Boolean);
  const partyHaystack = normalizeLookupText([
    result.partyName,
    result.party_name,
    result.plaintiffName,
    result.plaintiff_name,
    result.title,
    result.caseName,
    result.case_name
  ].join(" "));
  score += partyTerms.filter((term) => partyHaystack.includes(term)).length * 40;

  if (parseFiledAt(result.dateFiled || result.date_filed) === caseRow.date_filed) {
    score += 80;
  }

  return score;
}

function pickBestSearchResult(results, caseRow) {
  return results
    .map((result) => ({ result, score: scoreSearchResult(result, caseRow) }))
    .filter((item) => item.score >= 1000)
    .sort(compareScoreDesc)[0]?.result || null;
}

function extractSearchResults(payload) {
  const candidates = [
    payload,
    payload?.data,
    payload?.results,
    payload?.cases,
    payload?.caseSearchResults,
    payload?.caseSearchResultArray,
    payload?.data?.results,
    payload?.data?.cases,
    payload?.data?.caseSearchResults,
    payload?.data?.caseSearchResultArray
  ];

  for (const candidate of candidates) {
    if (Array.isArray(candidate)) {
      return candidate;
    }
  }

  return [];
}

function extractCaseId(value = {}) {
  return value.caseId || value.case_id || value.ucid || value.id || null;
}

function extractCasePayload(payload = {}) {
  return payload?.case || payload?.data?.case || payload?.data || payload || {};
}

function extractDocketEntries(payload = {}) {
  const casePayload = extractCasePayload(payload);
  const candidates = [
    payload?.docketEntries,
    payload?.docket_entries,
    payload?.dockets,
    payload?.caseDockets,
    payload?.case_dockets,
    payload?.caseActivities,
    casePayload?.docketEntries,
    casePayload?.docket_entries,
    casePayload?.dockets,
    casePayload?.caseDockets,
    casePayload?.case_dockets,
    casePayload?.caseActivities
  ];

  for (const candidate of candidates) {
    if (Array.isArray(candidate)) {
      return candidate;
    }
  }

  return [];
}

function parseDocketEntries(payload, baseUrl) {
  return extractDocketEntries(payload)
    .map((entry, index) => {
      const number = String(
        entry.entryNumber ||
        entry.entry_number ||
        entry.documentNumber ||
        entry.document_number ||
        entry.sequenceNumber ||
        entry.sequence_number ||
        entry.number ||
        ""
      ).trim();
      const filedAt = parseFiledAt(
        entry.dateFiled ||
        entry.date_filed ||
        entry.filedAt ||
        entry.filed_at ||
        entry.entryDate ||
        entry.entry_date ||
        entry.date
      );
      const description = cleanText(
        entry.description ||
        entry.docketText ||
        entry.docket_text ||
        entry.text ||
        entry.shortDescription ||
        entry.short_description ||
        entry.event
      );
      if (!number && !filedAt && !description) {
        return null;
      }

      return {
        row_number: number || String(index + 1),
        filed_at: filedAt,
        description: description || `Docket entry ${index + 1}`,
        absolute_url: absoluteUrl(baseUrl, entry.url || entry.link || entry.documentUrl || entry.absolute_url),
        raw: entry
      };
    })
    .filter(Boolean);
}

function extractAccessToken(payload = {}) {
  return String(
    payload?.accessToken ||
    payload?.access_token ||
    payload?.token ||
    payload?.apiToken ||
    payload?.api_token ||
    payload?.data?.accessToken ||
    payload?.data?.access_token ||
    payload?.data?.token ||
    ""
  ).trim();
}

export class UniCourtClient {
  constructor(config) {
    this.enabled = Boolean(config.enabled);
    this.baseUrl = String(config.baseUrl || "https://enterpriseapi.unicourt.com").replace(/\/$/, "");
    this.username = String(config.username || "").trim();
    this.password = String(config.password || "").trim();
    this.apiToken = String(config.apiToken || "").trim();
    this.tokenPath = String(config.tokenPath || "/generateNewToken").trim();
    this.tokenMethod = String(config.tokenMethod || "POST").trim().toUpperCase() || "POST";
    this.caseSearchPath = String(config.caseSearchPath || "/caseSearch").trim();
    this.caseSearchMethod = String(config.caseSearchMethod || "POST").trim().toUpperCase() || "POST";
    this.caseDetailPath = String(config.caseDetailPath || "/case").trim();
    this.caseDetailMethod = String(config.caseDetailMethod || "GET").trim().toUpperCase() || "GET";
    this.authHeader = String(config.authHeader || "Authorization").trim() || "Authorization";
    this.authScheme = String(config.authScheme || "Bearer").trim();
    this.minIntervalMs = Number(config.minIntervalMs || 2000);
    this.timeoutMs = Number(config.timeoutMs || 20000);
    this.testMode = Boolean(config.testMode);
    this.lastRequestAt = 0;
    this.accessToken = "";
    this.accessTokenCreatedAt = 0;
  }

  hasCredentials() {
    return Boolean(this.apiToken || (this.username && this.password));
  }

  getStatus() {
    if (!this.enabled) {
      return {
        enabled: false,
        state: "disabled",
        note: "默认关闭。"
      };
    }

    if (!this.hasCredentials()) {
      return {
        enabled: false,
        state: "missing-credentials",
        note: "已配置入口，但缺少 UniCourt API token 或账号密码。"
      };
    }

    return {
      enabled: true,
      state: this.apiToken ? "api-token" : "session-token",
      note: "通过 UniCourt API 做补充案件和 docket 校验。当前实现按案号精确匹配，再拉案件详情里的 docket。"
    };
  }

  async enrichCase(caseRow) {
    if (!this.enabled || !this.hasCredentials()) {
      return null;
    }

    const docketNumber = String(caseRow.docket_number || "").trim();
    if (!docketNumber || !/\b\d{2}-[a-z]{2}-\d{3,6}\b/i.test(docketNumber)) {
      return null;
    }

    const match = await this.lookupCase(caseRow);
    const timestamp = new Date().toISOString();
    if (!match) {
      return {
        state: "not_found",
        url: null,
        title: null,
        entries: [],
        syncedAt: timestamp
      };
    }

    const payload = await this.getCaseDetail(match);
    const entries = parseDocketEntries(payload, this.baseUrl);
    const casePayload = extractCasePayload(payload);

    return {
      state: entries.length ? "ok" : "empty",
      url: absoluteUrl(this.baseUrl, casePayload.caseUrl || casePayload.url || match.url),
      title: cleanText(casePayload.caseName || casePayload.case_name || match.title || caseRow.case_name),
      court: cleanText(casePayload.courtName || casePayload.court_name || match.court),
      docket: cleanText(casePayload.caseNumber || casePayload.case_number || match.docket || caseRow.docket_number),
      caseId: extractCaseId(match),
      entries,
      raw: payload,
      syncedAt: timestamp
    };
  }

  async lookupCase(caseRow) {
    const searchPayload = {
      caseNumber: String(caseRow.docket_number || "").trim(),
      courtName: String(caseRow.court_name || "").trim() || undefined,
      caseName: buildCaseNameSearchText(caseRow) || undefined,
      partyName: buildPlaintiffSearchText(caseRow) || undefined
    };
    const payload = await this.call(this.caseSearchPath, {
      method: this.caseSearchMethod,
      ...(this.caseSearchMethod === "GET"
        ? { query: searchPayload }
        : { body: searchPayload })
    });

    const results = extractSearchResults(payload);
    return pickBestSearchResult(results, caseRow);
  }

  async getCaseDetail(match) {
    const caseId = extractCaseId(match);
    if (!caseId) {
      throw new Error("UniCourt search result missing case id");
    }

    return this.call(`${this.caseDetailPath.replace(/\/$/, "")}/${encodeURIComponent(String(caseId))}`, {
      method: this.caseDetailMethod
    });
  }

  async call(pathname, { method = "GET", query = {}, body = null, includeAuth = true } = {}) {
    return this.callWithRetry(pathname, { method, query, body, includeAuth }, 0);
  }

  async callWithRetry(pathname, options, attempt) {
    try {
      return await this.performCall(pathname, {
        ...options,
        headers: options.includeAuth === false ? {} : await this.buildAuthHeaders()
      });
    } catch (error) {
      const shouldRetryAuth =
        options.includeAuth !== false &&
        attempt < 1 &&
        (error.status === 401 || error.status === 403 || /unauthorized|forbidden|token/i.test(String(error.message || "")));
      if (shouldRetryAuth) {
        this.accessToken = "";
        this.accessTokenCreatedAt = 0;
        return this.callWithRetry(pathname, options, attempt + 1);
      }

      throw error;
    }
  }

  async buildAuthHeaders() {
    const token = await this.ensureAccessToken();
    if (!token) {
      return {};
    }

    const headerValue = this.authScheme ? `${this.authScheme} ${token}` : token;
    return {
      [this.authHeader]: headerValue
    };
  }

  async performCall(pathname, { method = "GET", query = {}, body = null, headers = {} } = {}) {
    await this.throttle();

    const url = new URL(String(pathname || "").replace(/^\/*/, "/"), this.baseUrl);
    for (const [key, value] of Object.entries(query || {})) {
      if (value !== undefined && value !== null && value !== "") {
        url.searchParams.set(key, String(value));
      }
    }

    const request = {
      method,
      headers: {
        accept: "application/json",
        "user-agent": "tro-case-watch/0.1",
        ...headers
      },
      signal: AbortSignal.timeout(this.timeoutMs)
    };

    if (method !== "GET" && body) {
      request.headers["content-type"] = "application/json";
      request.body = JSON.stringify(body);
    }

    const response = await fetch(url, request);
    const text = await response.text();
    let payload;
    try {
      payload = text ? JSON.parse(text) : {};
    } catch {
      const error = new Error(`UniCourt returned non-JSON for ${pathname}`);
      error.status = response.status;
      error.body = text;
      throw error;
    }

    if (!response.ok || payload?.success === false || payload?.error) {
      const error = new Error(payload?.error || `UniCourt request failed: ${response.status}`);
      error.status = response.status;
      error.body = text;
      throw error;
    }

    return payload;
  }

  async ensureAccessToken() {
    if (this.apiToken) {
      return this.apiToken;
    }

    const maxAgeMs = 45 * 60 * 1000;
    if (this.accessToken && Date.now() - this.accessTokenCreatedAt < maxAgeMs) {
      return this.accessToken;
    }

    const tokenRequestPayload = {
      username: this.username,
      password: this.password
    };
    const payload = await this.performCall(this.tokenPath, {
      method: this.tokenMethod,
      ...(this.tokenMethod === "GET"
        ? { query: tokenRequestPayload }
        : { body: tokenRequestPayload })
    });

    const token = extractAccessToken(payload);
    if (!token) {
      throw new Error("UniCourt token response missing access token");
    }

    this.accessToken = token;
    this.accessTokenCreatedAt = Date.now();
    return this.accessToken;
  }

  async throttle() {
    const waitMs = Math.max(0, this.minIntervalMs - (Date.now() - this.lastRequestAt));
    if (waitMs > 0) {
      await wait(waitMs);
    }
    this.lastRequestAt = Date.now();
  }
}
