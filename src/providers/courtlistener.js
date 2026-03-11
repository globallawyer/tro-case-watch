export class CourtListenerClient {
  constructor(config) {
    this.baseUrl = config.baseUrl.replace(/\/$/, "");
    this.apiToken = config.apiToken || "";
    this.enableDocketSync = Boolean(config.enableDocketSync);
    this.capabilities = {
      docket: Boolean(this.apiToken) && this.enableDocketSync,
      docketEntries: Boolean(this.apiToken) && this.enableDocketSync
    };
  }

  hasDocketAccess() {
    return this.capabilities.docket;
  }

  hasDocketEntriesAccess() {
    return this.capabilities.docketEntries;
  }

  absoluteUrl(value) {
    if (!value) {
      return null;
    }

    if (value.startsWith("http://") || value.startsWith("https://")) {
      return value;
    }

    return `https://www.courtlistener.com${value}`;
  }

  async search({ query, cursorUrl = null, startDate, pageSize = 20 }) {
    const url = cursorUrl ? new URL(cursorUrl) : new URL(`${this.baseUrl}/search/`);

    if (!cursorUrl) {
      url.searchParams.set("type", "r");
      url.searchParams.set("q", query);
      url.searchParams.set("filed_after", startDate);
      url.searchParams.set("order_by", "dateFiled desc");
      url.searchParams.set("page_size", String(pageSize));
    }

    return this.fetchJson(url.toString(), { requiresAuth: false });
  }

  async fetchDocket(docketId) {
    return this.fetchJson(`${this.baseUrl}/dockets/${docketId}/`, { requiresAuth: true });
  }

  async fetchDocketEntries(docketId, pageSize = 100) {
    if (!this.capabilities.docketEntries) {
      return [];
    }

    let nextUrl = `${this.baseUrl}/docket-entries/?docket=${encodeURIComponent(docketId)}&page_size=${pageSize}`;
    const entries = [];

    while (nextUrl) {
      let payload;
      try {
        payload = await this.fetchJson(nextUrl, { requiresAuth: true });
      } catch (error) {
        if (error.status === 403) {
          this.capabilities.docketEntries = false;
          return [];
        }
        throw error;
      }
      entries.push(...(payload.results || []));
      nextUrl = payload.next;
      if (entries.length >= pageSize) {
        break;
      }
    }

    return entries;
  }

  async fetchJson(url, { requiresAuth }) {
    const headers = {
      accept: "application/json",
      "user-agent": "tro-case-watch/0.1"
    };

    if (this.apiToken) {
      headers.authorization = `Token ${this.apiToken}`;
    }

    const response = await fetch(url, { headers });
    const text = await response.text();

    if (!response.ok) {
      const error = new Error(`CourtListener request failed: ${response.status}`);
      error.status = response.status;
      error.body = text;
      error.requiresAuth = requiresAuth;
      throw error;
    }

    return JSON.parse(text);
  }
}
