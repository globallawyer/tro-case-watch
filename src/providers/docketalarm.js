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

  return null;
}

function normalizeLookupText(value) {
  return normalizeText(value).replace(/[^\w]+/g, " ").replace(/\s+/g, " ").trim();
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

function compareScoreDesc(left, right) {
  return right.score - left.score;
}

function scoreSearchResult(result, caseRow) {
  const docketNeedle = normalizeDocket(caseRow.docket_number);
  const resultDocket = normalizeDocket(result.docket || result.docket_number || result.docketNumber);
  if (!docketNeedle || !resultDocket || docketNeedle !== resultDocket) {
    return -1;
  }

  let score = 1000;

  if (courtNamesLikelyMatch(caseRow.court_name, result.court)) {
    score += 250;
  }

  const caseNameNeedle = normalizeLookupText(buildCaseNameSearchText(caseRow));
  const resultTitle = normalizeLookupText(result.title || result.case_name || result.caseName);
  if (caseNameNeedle && resultTitle) {
    if (resultTitle.includes(caseNameNeedle) || caseNameNeedle.includes(resultTitle)) {
      score += 200;
    }
  }

  const partyTerms = buildPartySearchTerms(caseRow).map(normalizeLookupText).filter(Boolean);
  const partyHaystack = normalizeLookupText([
    result.party_name,
    result.partyName,
    result.title,
    result.case_name
  ].join(" "));
  score += partyTerms.filter((term) => partyHaystack.includes(term)).length * 40;

  if (parseFiledAt(result.date_filed) && parseFiledAt(result.date_filed) === caseRow.date_filed) {
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

function parseDocketEntries(payload, baseUrl) {
  return asArray(payload?.docket_report || payload?.docketReport || payload?.entries)
    .map((entry, index) => {
      const number = String(
        entry.number ||
        entry.entry_number ||
        entry.entryNumber ||
        entry.document_number ||
        entry.documentNumber ||
        ""
      ).trim();
      const filedAt = parseFiledAt(entry.entry_date || entry.date || entry.filed_at || entry.date_filed);
      const description = cleanText(entry.contents || entry.description || entry.text || entry.short_description);
      if (!filedAt && !number && !description) {
        return null;
      }

      return {
        row_number: number || String(index + 1),
        filed_at: filedAt,
        description: description || `Docket entry ${index + 1}`,
        absolute_url: absoluteUrl(baseUrl, entry.link || entry.url || entry.absolute_url),
        raw: entry
      };
    })
    .filter(Boolean);
}

export class DocketAlarmClient {
  constructor(config) {
    this.enabled = Boolean(config.enabled);
    this.baseUrl = String(config.baseUrl || "https://www.docketalarm.com").replace(/\/$/, "");
    this.username = String(config.username || "").trim();
    this.password = String(config.password || "").trim();
    this.clientMatter = String(config.clientMatter || "tro-case-watch").trim();
    this.minIntervalMs = Number(config.minIntervalMs || 2000);
    this.timeoutMs = Number(config.timeoutMs || 20000);
    this.useCachedDockets = Boolean(config.useCachedDockets);
    this.testMode = Boolean(config.testMode);
    this.lastRequestAt = 0;
    this.loginToken = "";
    this.loginTokenCreatedAt = 0;
  }

  hasCredentials() {
    return Boolean(this.username && this.password);
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
        note: "已配置入口，但缺少 Docket Alarm 用户名或密码。"
      };
    }

    return {
      enabled: true,
      state: this.testMode ? "api-test-mode" : "api-cached-docket",
      note: this.useCachedDockets
        ? "通过官方 API 做精确 docket 补全，默认只取 Docket Alarm 缓存副本，避免额外法院费用。"
        : "通过官方 API 做精确 docket 补全，允许按需拉取法院最新 docket，可能产生额外法院费用。"
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

    const payload = await this.getDocket(match);
    const entries = parseDocketEntries(payload, this.baseUrl);
    return {
      state: entries.length ? "ok" : "empty",
      url: absoluteUrl(this.baseUrl, payload.link || payload.url || match.link),
      title: cleanText(payload.title || payload.case_name || payload.caseName || match.title || caseRow.case_name),
      court: match.court || payload.court || null,
      docket: match.docket || payload.docket || caseRow.docket_number,
      entries,
      raw: payload,
      syncedAt: timestamp
    };
  }

  async lookupCase(caseRow) {
    const payload = await this.call("searchpacer", {
      method: "GET",
      params: {
        docket_num: String(caseRow.docket_number || "").trim(),
        party_name: buildPlaintiffSearchText(caseRow) || undefined,
        client_matter: this.clientMatter
      }
    });

    const results = asArray(payload.search_results || payload.results);
    return pickBestSearchResult(results, caseRow);
  }

  async getDocket(match) {
    return this.call("getdocket", {
      method: "GET",
      params: {
        court: match.court,
        docket: match.docket || match.docket_number || match.docketNumber,
        client_matter: this.clientMatter,
        cached: this.useCachedDockets ? "true" : undefined
      }
    });
  }

  async call(endpoint, { method = "GET", params = {} } = {}) {
    return this.callWithRetry(endpoint, { method, params }, 0);
  }

  async callWithRetry(endpoint, options, attempt) {
    try {
      const safeParams = Object.fromEntries(
        Object.entries({
          ...(options.params || {}),
          ...(this.testMode ? { test: "true" } : {}),
          ...(endpoint === "login" ? {} : { login_token: await this.ensureLoginToken() })
        }).filter(([, value]) => value !== undefined && value !== null && value !== "")
      );

      return await this.performCall(endpoint, { ...options, params: safeParams });
    } catch (error) {
      const shouldRetryAuth =
        endpoint !== "login" &&
        attempt < 1 &&
        (error.status === 401 || error.status === 403 || /login token|unauthorized|forbidden/i.test(String(error.message || "")));
      if (shouldRetryAuth) {
        this.loginToken = "";
        this.loginTokenCreatedAt = 0;
        return this.callWithRetry(endpoint, options, attempt + 1);
      }

      throw error;
    }
  }

  async performCall(endpoint, { method = "GET", params = {} } = {}) {
    await this.throttle();

    const url = new URL(`/api/v1/${String(endpoint || "").replace(/^\/+|\/+$/g, "")}/`, this.baseUrl);
    const search = new URLSearchParams();
    for (const [key, value] of Object.entries(params)) {
      search.append(key, String(value));
    }

    const request = {
      method,
      headers: {
        accept: "application/json",
        "user-agent": "tro-case-watch/0.1"
      },
      signal: AbortSignal.timeout(this.timeoutMs)
    };

    if (method === "GET") {
      url.search = search.toString();
    } else {
      request.headers["content-type"] = "application/x-www-form-urlencoded";
      request.body = search.toString();
    }

    const response = await fetch(url, request);
    const text = await response.text();
    let payload;
    try {
      payload = JSON.parse(text);
    } catch {
      const error = new Error(`Docket Alarm returned non-JSON for ${endpoint}`);
      error.status = response.status;
      error.body = text;
      throw error;
    }

    if (!response.ok || payload?.success === false || payload?.error) {
      const error = new Error(payload?.error || `Docket Alarm request failed: ${response.status}`);
      error.status = response.status;
      error.body = text;
      throw error;
    }

    return payload;
  }

  async ensureLoginToken() {
    const maxAgeMs = 45 * 60 * 1000;
    if (this.loginToken && Date.now() - this.loginTokenCreatedAt < maxAgeMs) {
      return this.loginToken;
    }

    const payload = await this.performCall("login", {
      method: "POST",
      params: {
        username: this.username,
        password: this.password
      }
    });

    if (!payload?.login_token) {
      throw new Error(payload?.error || "Docket Alarm login failed");
    }

    this.loginToken = String(payload.login_token);
    this.loginTokenCreatedAt = Date.now();
    return this.loginToken;
  }

  async throttle() {
    const waitMs = Math.max(0, this.minIntervalMs - (Date.now() - this.lastRequestAt));
    if (waitMs > 0) {
      await wait(waitMs);
    }
    this.lastRequestAt = Date.now();
  }
}
