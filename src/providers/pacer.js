import { buildCourtDirectoryMaps, fetchPacerCourtDirectory } from "../court-directory.js";

function wait(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function shouldRetryStatus(status) {
  return [408, 409, 425, 429, 500, 502, 503, 504].includes(Number(status));
}

function shouldRetryError(error) {
  return error?.name === "AbortError" || error instanceof TypeError || String(error?.message || "").includes("fetch failed");
}

function retryDelayMs(attempt) {
  return 1500 * (attempt + 1);
}

function todayIso() {
  return new Date().toISOString().slice(0, 10);
}

function shiftDate(dateIso, deltaDays) {
  const base = Date.parse(`${String(dateIso).slice(0, 10)}T00:00:00Z`);
  if (!Number.isFinite(base)) {
    return todayIso();
  }

  return new Date(base + deltaDays * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function clean(value) {
  return String(value || "").trim();
}

function unique(values = []) {
  return [...new Set(values.map((value) => clean(value)).filter(Boolean))];
}

export class PacerAdapter {
  constructor(config, store) {
    this.config = config;
    this.store = store;
    this.lastRequestAt = 0;
    this.authToken = "";
    this.directoryMaps = {
      bySlug: new Map(),
      byPacerCourtId: new Map()
    };
    this.directoryLoaded = false;
  }

  hasCredentials() {
    return Boolean(this.config.loginId && this.config.password);
  }

  getStatus() {
    const usage = this.store.getProviderUsage("pacer");
    const remaining = Math.max(0, this.config.dailyBudgetUsd - usage.estimated_cost_usd);

    if (!this.config.enabled) {
      return {
        enabled: false,
        state: "disabled",
        remainingBudgetUsd: remaining,
        note: "PACER 官方发现链路已关闭。"
      };
    }

    if (!this.hasCredentials()) {
      return {
        enabled: true,
        state: "missing-credentials",
        remainingBudgetUsd: remaining,
        note: "已启用 PACER，但缺少登录凭据。"
      };
    }

    return {
      enabled: true,
      state: "ready",
      remainingBudgetUsd: remaining,
      recentWindowDays: this.config.recentWindowDays,
      maxPagesPerRun: this.config.maxPagesPerRun,
      note: "PACER 官方发现链路可用于补 2025/2026 民事案件发现。"
    };
  }

  async syncRecent() {
    return {
      provider: "pacer",
      ...this.getStatus(),
      syncedCases: 0
    };
  }

  async discoverRecentCases({ mode = "recent" } = {}) {
    if (!this.config.enabled) {
      return {
        items: [],
        pagesFetched: 0,
        requestCount: 0,
        estimatedCostUsd: 0,
        note: "PACER 官方发现链路已关闭。"
      };
    }

    if (!this.hasCredentials()) {
      return {
        items: [],
        pagesFetched: 0,
        requestCount: 0,
        estimatedCostUsd: 0,
        note: "PACER 已启用，但缺少登录凭据。"
      };
    }

    const maxPages = mode === "backfill" ? this.config.backfillMaxPagesPerRun : this.config.maxPagesPerRun;
    const windowDays = mode === "backfill" ? this.config.backfillWindowDays : this.config.recentWindowDays;
    const dateTo = todayIso();
    const dateFrom = shiftDate(dateTo, -Math.max(windowDays - 1, 0));
    try {
      await this.ensureCourtDirectory();
    } catch {
      // Court directory is helpful for court-id normalization, but discovery can still proceed without it.
    }
    const targetCourtIds = await this.resolveTargetCourtIds();
    const perRequestCost = Number(this.config.estimatedCostUsdPerRequest || 0);
    const usage = this.store.getProviderUsage("pacer");
    const remainingBudget = Math.max(0, Number(this.config.dailyBudgetUsd || 0) - Number(usage.estimated_cost_usd || 0));
    const budgetCap = Math.max(0, Number(this.config.perRunBudgetUsd || 0));
    if (perRequestCost > 0 && (remainingBudget < perRequestCost || budgetCap < perRequestCost)) {
      return {
        items: [],
        pagesFetched: 0,
        requestCount: 0,
        estimatedCostUsd: 0,
        dateFiledFrom: dateFrom,
        dateFiledTo: dateTo,
        note: "PACER 官方发现链路本轮预算不足，已跳过检索。"
      };
    }
    const maxAffordableByRun = perRequestCost > 0 ? Math.floor(budgetCap / perRequestCost) : maxPages;
    const maxAffordableByDay = perRequestCost > 0 ? Math.floor(remainingBudget / perRequestCost) : maxPages;
    const pageBudget = Math.max(1, Math.min(maxPages, maxAffordableByRun || maxPages, maxAffordableByDay || maxPages));

    const items = [];
    let pagesFetched = 0;

    for (let page = 0; page < pageBudget; page += 1) {
      const payload = await this.searchCases({
        page,
        dateFiledFrom: dateFrom,
        dateFiledTo: dateTo,
        courtIds: targetCourtIds,
        caseTypes: this.config.caseTypes,
        natureOfSuit: this.config.natureOfSuit
      });

      const pageItems = asArray(payload?.content).map((item) => this.normalizeCase(item));
      items.push(...pageItems);
      pagesFetched += 1;

      const lastPage = Boolean(payload?.pageInfo?.last);
      const numberOfElements = Number(payload?.pageInfo?.numberOfElements || pageItems.length || 0);
      if (lastPage || numberOfElements === 0) {
        break;
      }
    }

    const requestCount = pagesFetched + 1;
    const estimatedCostUsd = Number((pagesFetched * perRequestCost).toFixed(2));
    if (pagesFetched > 0 && estimatedCostUsd > 0) {
      this.store.addProviderUsage("pacer", pagesFetched, estimatedCostUsd);
    }

    return {
      items,
      pagesFetched,
      requestCount,
      estimatedCostUsd,
      dateFiledFrom: dateFrom,
      dateFiledTo: dateTo,
      note: items.length
        ? `PACER 官方检索在 ${dateFrom} 至 ${dateTo} 之间发现 ${items.length} 个候选案件。`
        : `PACER 官方检索在 ${dateFrom} 至 ${dateTo} 之间没有返回候选案件。`
    };
  }

  async resolveTargetCourtIds() {
    const configured = unique(this.config.courtIds || []);
    if (!configured.length) {
      return [];
    }

    try {
      await this.ensureCourtDirectory();
    } catch {
      return unique(configured.map((value) => String(value || "").trim().toUpperCase()));
    }
    return unique(
      configured.map((value) => {
        const raw = String(value || "").trim();
        if (!raw) {
          return "";
        }
        const exact = this.directoryMaps.byPacerCourtId.get(raw.toUpperCase());
        if (exact?.pacerCourtId) {
          return exact.pacerCourtId;
        }
        const mapped = this.directoryMaps.bySlug.get(raw.toLowerCase());
        if (mapped?.pacerCourtId) {
          return mapped.pacerCourtId;
        }
        return raw.toUpperCase();
      })
    );
  }

  async ensureCourtDirectory() {
    if (this.directoryLoaded || !this.config.courtLookupUrl) {
      return;
    }

    const entries = await fetchPacerCourtDirectory(this.config.courtLookupUrl, { timeoutMs: this.config.timeoutMs });
    this.directoryMaps = buildCourtDirectoryMaps(entries);
    this.directoryLoaded = true;
  }

  normalizeCase(item = {}) {
    const rawCourtId = clean(item.courtId).toUpperCase();
    const directoryEntry = this.directoryMaps.byPacerCourtId.get(rawCourtId) || null;

    return {
      courtId: directoryEntry?.slug || rawCourtId.toLowerCase(),
      pacerCourtId: rawCourtId,
      courtName: directoryEntry?.courtName || clean(item.courtName) || rawCourtId,
      caseId: clean(item.caseId),
      caseTitle: clean(item.caseTitle),
      caseNumberFull: clean(item.caseNumberFull),
      caseType: clean(item.caseType).toLowerCase(),
      natureOfSuit: clean(item.natureOfSuit),
      dateFiled: clean(item.dateFiled),
      dateTermed: clean(item.dateTermed),
      judgeLastName: clean(item.judgeLastName),
      caseLink: clean(item.caseLink),
      raw: item
    };
  }

  async searchCases({
    page = 0,
    dateFiledFrom,
    dateFiledTo,
    courtIds = [],
    caseTypes = [],
    natureOfSuit = []
  } = {}) {
    const body = {
      courtId: unique(courtIds),
      caseType: unique(caseTypes),
      natureOfSuit: unique(natureOfSuit),
      nos: unique(natureOfSuit),
      dateFiledFrom: clean(dateFiledFrom),
      dateFiledTo: clean(dateFiledTo),
      requestSource: this.config.requestSource,
      searchType: "case",
      requestType: "query"
    };

    const url = new URL(`${this.config.baseUrl.replace(/\/$/, "")}/cases/find`);
    url.searchParams.set("page", String(Math.max(Number(page) || 0, 0)));

    return this.fetchJson(url.toString(), {
      method: "POST",
      body
    });
  }

  async fetchJson(url, { method = "GET", body = null } = {}) {
    return this.fetchJsonWithRetry(url, { method, body }, 0);
  }

  async fetchJsonWithRetry(url, { method, body }, attempt) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.config.timeoutMs);

    try {
      const token = await this.getAuthToken(attempt > 0);
      const waitMs = this.config.minIntervalMs - (Date.now() - this.lastRequestAt);
      if (waitMs > 0) {
        await wait(waitMs);
      }

      const response = await fetch(url, {
        method,
        headers: {
          accept: "application/json",
          "content-type": "application/json",
          "user-agent": "tro-case-watch/0.1",
          "x-next-gen-cso": token
        },
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal
      });
      const text = await response.text();
      this.lastRequestAt = Date.now();

      const nextToken = clean(response.headers.get("x-next-gen-cso"));
      if (nextToken) {
        this.authToken = nextToken;
      }

      if (!response.ok) {
        if ((response.status === 401 || response.status === 403) && attempt < 1) {
          this.authToken = "";
          await wait(retryDelayMs(attempt));
          return this.fetchJsonWithRetry(url, { method, body }, attempt + 1);
        }

        if (attempt < 2 && shouldRetryStatus(response.status)) {
          await wait(retryDelayMs(attempt));
          return this.fetchJsonWithRetry(url, { method, body }, attempt + 1);
        }

        const error = new Error(`PACER request failed: ${response.status}`);
        error.status = response.status;
        error.body = text;
        throw error;
      }

      return JSON.parse(text);
    } catch (error) {
      if (attempt < 2 && shouldRetryError(error)) {
        await wait(retryDelayMs(attempt));
        return this.fetchJsonWithRetry(url, { method, body }, attempt + 1);
      }

      if (error?.name === "AbortError") {
        const timeoutError = new Error(`PACER request timed out after ${this.config.timeoutMs}ms`);
        timeoutError.code = "ETIMEDOUT";
        throw timeoutError;
      }

      throw error;
    } finally {
      clearTimeout(timer);
    }
  }

  async getAuthToken(forceRefresh = false) {
    if (this.authToken && !forceRefresh) {
      return this.authToken;
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.config.timeoutMs);

    try {
      const response = await fetch(this.config.authUrl, {
        method: "POST",
        headers: {
          accept: "application/json",
          "content-type": "application/json",
          "user-agent": "tro-case-watch/0.1"
        },
        body: JSON.stringify({
          loginId: this.config.loginId,
          password: this.config.password,
          clientCode: this.config.clientCode,
          otpCode: this.config.otpCode || undefined,
          redactionFlag: 1
        }),
        signal: controller.signal
      });
      const text = await response.text();
      if (!response.ok) {
        const error = new Error(`PACER auth failed: ${response.status}`);
        error.status = response.status;
        error.body = text;
        throw error;
      }

      const payload = JSON.parse(text);
      const token = clean(payload?.nextGenCSO || payload?.NextGenCSO || response.headers.get("x-next-gen-cso"));
      if (!token) {
        throw new Error("PACER auth succeeded but no nextGenCSO token was returned");
      }

      this.authToken = token;
      return token;
    } catch (error) {
      if (error?.name === "AbortError") {
        const timeoutError = new Error(`PACER auth timed out after ${this.config.timeoutMs}ms`);
        timeoutError.code = "ETIMEDOUT";
        throw timeoutError;
      }

      throw error;
    } finally {
      clearTimeout(timer);
    }
  }
}
