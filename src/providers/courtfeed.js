const DEFAULT_COURT_FEEDS = [
  {
    id: "ilnd",
    courtId: "ilnd",
    courtName: "Northern District of Illinois",
    feedUrl: "https://ecf.ilnd.uscourts.gov/cgi-bin/rss_outside.pl"
  },
  {
    id: "flsd",
    courtId: "flsd",
    courtName: "Southern District of Florida",
    feedUrl: "https://ecf.flsd.uscourts.gov/cgi-bin/rss_outside.pl"
  },
  {
    id: "cacd",
    courtId: "cacd",
    courtName: "Central District of California",
    feedUrl: "https://ecf.cacd.uscourts.gov/cgi-bin/rss_outside.pl"
  },
  {
    id: "cand",
    courtId: "cand",
    courtName: "Northern District of California",
    feedUrl: "https://ecf.cand.uscourts.gov/cgi-bin/rss_outside.pl"
  },
  {
    id: "casd",
    courtId: "casd",
    courtName: "Southern District of California",
    feedUrl: "https://ecf.casd.uscourts.gov/cgi-bin/rss_outside.pl"
  },
  {
    id: "gand",
    courtId: "gand",
    courtName: "Northern District of Georgia",
    feedUrl: "https://ecf.gand.uscourts.gov/cgi-bin/rss_outside.pl"
  },
  {
    id: "gasd",
    courtId: "gasd",
    courtName: "Southern District of Georgia",
    feedUrl: "https://ecf.gasd.uscourts.gov/cgi-bin/rss_outside.pl"
  },
  {
    id: "paed",
    courtId: "paed",
    courtName: "Eastern District of Pennsylvania",
    feedUrl: "https://ecf.paed.uscourts.gov/cgi-bin/rss_outside.pl"
  },
  {
    id: "mdpa",
    courtId: "mdpa",
    courtName: "Middle District of Pennsylvania",
    feedUrl: "https://ecf.mdpa.uscourts.gov/cgi-bin/rss_outside.pl"
  },
  {
    id: "pawd",
    courtId: "pawd",
    courtName: "Western District of Pennsylvania",
    feedUrl: "https://ecf.pawd.uscourts.gov/cgi-bin/rss_outside.pl"
  },
  {
    id: "tned",
    courtId: "tned",
    courtName: "Eastern District of Tennessee",
    feedUrl: "https://ecf.tned.uscourts.gov/cgi-bin/rss_outside.pl"
  },
  {
    id: "tnmd",
    courtId: "tnmd",
    courtName: "Middle District of Tennessee",
    feedUrl: "https://ecf.tnmd.uscourts.gov/cgi-bin/rss_outside.pl"
  },
  {
    id: "tnwd",
    courtId: "tnwd",
    courtName: "Western District of Tennessee",
    feedUrl: "https://ecf.tnwd.uscourts.gov/cgi-bin/rss_outside.pl"
  },
  {
    id: "waed",
    courtId: "waed",
    courtName: "Eastern District of Washington",
    feedUrl: "https://ecf.waed.uscourts.gov/cgi-bin/rss_outside.pl"
  },
  {
    id: "wawd",
    courtId: "wawd",
    courtName: "Western District of Washington",
    feedUrl: "https://ecf.wawd.uscourts.gov/cgi-bin/rss_outside.pl"
  }
];

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

function extractTag(block, tagName) {
  const match = String(block || "").match(new RegExp(`<${tagName}\\b[^>]*>([\\s\\S]*?)</${tagName}>`, "i"));
  return match ? match[1] : "";
}

function parseIsoDate(value) {
  const ms = Date.parse(String(value || "").trim());
  if (!Number.isFinite(ms)) {
    return null;
  }

  return new Date(ms).toISOString();
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

function queryValue(value, key) {
  try {
    return new URL(String(value || "")).searchParams.get(key);
  } catch {
    return null;
  }
}

function parseTitle(title) {
  const match = String(title || "").trim().match(/^((?:\d+:)?\d{2}-[a-z]{2}-\d{3,6})\s+(.+)$/i);
  if (!match) {
    return {
      docketNumber: "",
      caseName: String(title || "").trim()
    };
  }

  return {
    docketNumber: match[1].trim(),
    caseName: match[2].trim()
  };
}

function parseDescription(rawDescription, baseUrl) {
  const decoded = decodeHtml(rawDescription);
  const documentType = cleanText(decoded.match(/^\s*\[([^\]]+)\]/)?.[1] || "");
  const documentUrl = absoluteUrl(decoded.match(/href\s*=\s*["']([^"']+)["']/i)?.[1] || "", baseUrl);
  const anchorNumber = cleanText(decoded.match(/>\s*([^<]+?)\s*<\/a>/i)?.[1] || "");
  const bracketNumber = cleanText(decoded.match(/\((\d+(?:\.\d+)?)\)\s*$/)?.[1] || "");
  const documentNumber = anchorNumber || bracketNumber || "";
  const sequenceNumber = queryValue(documentUrl, "de_seq_num") || "";
  const caseId = queryValue(documentUrl, "caseid") || "";
  const descriptionText = cleanText(decoded).replace(/\(\s*\d+(?:\.\d+)?\s*\)\s*$/, "").trim();

  return {
    documentType: documentType || descriptionText || "",
    description: descriptionText || documentType || "",
    documentNumber,
    sequenceNumber,
    documentUrl,
    caseId
  };
}

function parseFeedItem(block, feed) {
  const title = cleanText(extractTag(block, "title"));
  const link = absoluteUrl(cleanText(extractTag(block, "link")), feed.feedUrl);
  const rawDescription = extractTag(block, "description");
  const parsedDescription = parseDescription(rawDescription, feed.feedUrl);
  const guid = cleanText(extractTag(block, "guid")) || [link, parsedDescription.sequenceNumber, parsedDescription.documentNumber].filter(Boolean).join("#");
  const pubDate = parseIsoDate(extractTag(block, "pubDate"));
  const titleParts = parseTitle(title);
  const reportCaseId = link ? String(link).match(/\?(\d+)(?:$|&)/)?.[1] || "" : "";

  return {
    feedId: feed.id,
    feedUrl: feed.feedUrl,
    courtId: feed.courtId,
    courtName: feed.courtName,
    title,
    docketNumber: titleParts.docketNumber,
    caseName: titleParts.caseName,
    guid,
    filedAt: pubDate,
    link,
    reportCaseId,
    caseId: parsedDescription.caseId || reportCaseId,
    documentType: parsedDescription.documentType,
    description: parsedDescription.description,
    documentNumber: parsedDescription.documentNumber,
    sequenceNumber: parsedDescription.sequenceNumber,
    documentUrl: parsedDescription.documentUrl,
    rawDescription: cleanText(rawDescription)
  };
}

export class CourtFeedClient {
  constructor(config) {
    this.enabled = Boolean(config.enabled);
    this.timeoutMs = Number(config.timeoutMs || 15000);
    this.minIntervalMs = Number(config.minIntervalMs || 1000);
    this.maxItemsPerFeed = Number(config.maxItemsPerFeed || 100);
    this.maxLookupsPerRun = Number(config.maxLookupsPerRun || 12);
    const selectedIds = new Set((config.courts || []).map((value) => String(value || "").trim().toLowerCase()).filter(Boolean));
    this.feeds = DEFAULT_COURT_FEEDS.filter((feed) => !selectedIds.size || selectedIds.has(feed.id));
    this.lastRequestAt = 0;
  }

  getStatus() {
    return {
      enabled: this.enabled,
      trackedCourts: this.feeds.length,
      feeds: this.feeds.map((feed) => ({
        id: feed.id,
        court_id: feed.courtId,
        court_name: feed.courtName
      }))
    };
  }

  listFeeds() {
    return this.feeds.map((feed) => ({ ...feed }));
  }

  async fetchFeed(feed) {
    return this.fetchFeedWithRetry(feed, 0);
  }

  async fetchFeedWithRetry(feed, attempt) {
    if (!this.enabled) {
      return {
        feed,
        lastBuildDate: null,
        items: []
      };
    }

    const waitMs = this.minIntervalMs - (Date.now() - this.lastRequestAt);
    if (waitMs > 0) {
      await wait(waitMs);
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const response = await fetch(feed.feedUrl, {
        headers: {
          accept: "application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.1",
          "user-agent": "tro-case-watch/0.1"
        },
        signal: controller.signal
      });

      const text = await response.text();
      if (!response.ok) {
        if (attempt < 2 && shouldRetryStatus(response.status)) {
          await wait(retryDelayMs(attempt));
          return this.fetchFeedWithRetry(feed, attempt + 1);
        }

        const error = new Error(`Court feed request failed: ${response.status}`);
        error.status = response.status;
        error.body = text;
        throw error;
      }

      const lastBuildDate = parseIsoDate(extractTag(text, "lastBuildDate"));
      const items = [...text.matchAll(/<item\b[^>]*>([\s\S]*?)<\/item>/gi)]
        .map((match) => parseFeedItem(match[1], feed))
        .filter((item) => item.guid && item.docketNumber)
        .slice(0, this.maxItemsPerFeed);

      return {
        feed,
        lastBuildDate,
        items
      };
    } catch (error) {
      if (attempt < 2 && shouldRetryError(error)) {
        await wait(retryDelayMs(attempt));
        return this.fetchFeedWithRetry(feed, attempt + 1);
      }

      if (error.name === "AbortError") {
        const timeoutError = new Error(`Court feed request timed out after ${this.timeoutMs}ms`);
        timeoutError.code = "ETIMEDOUT";
        throw timeoutError;
      }

      throw error;
    } finally {
      clearTimeout(timer);
      this.lastRequestAt = Date.now();
    }
  }
}

function shouldRetryStatus(status) {
  return [408, 409, 425, 429, 500, 502, 503, 504].includes(Number(status));
}

function shouldRetryError(error) {
  return error?.name === "AbortError" || error instanceof TypeError || String(error?.message || "").includes("fetch failed");
}

function retryDelayMs(attempt) {
  return 1000 * (attempt + 1);
}
