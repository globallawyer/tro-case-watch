function wait(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function cleanText(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
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

export function inferCourtSlugFromUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }

  try {
    const host = new URL(raw).hostname.toLowerCase();
    const parts = host.split(".").filter(Boolean);
    if (parts.length >= 3 && parts.at(-2) === "uscourts" && parts.at(-1) === "gov") {
      if (parts[0] === "ecf" || parts[0] === "www") {
        return String(parts[1] || "").toLowerCase();
      }
      return String(parts[0] || "").toLowerCase();
    }
  } catch {
    return "";
  }

  return "";
}

export function inferCourtSlugFromPacerCourtId(value) {
  const raw = String(value || "").trim().toUpperCase();
  if (!raw) {
    return "";
  }

  if (raw.endsWith("BK") && raw.length >= 4) {
    return `${raw.slice(0, -2).toLowerCase()}b`;
  }

  if (raw.endsWith("DC") && raw.length >= 4) {
    return `${raw.slice(0, -1).toLowerCase()}`;
  }

  if (raw.endsWith("C") && raw.length >= 4) {
    return raw.slice(0, -1).toLowerCase();
  }

  return raw.toLowerCase();
}

export function normalizeCourtDirectoryEntry(raw = {}) {
  const rssUrl = cleanText(raw.rss_url);
  const loginUrl = cleanText(raw.login_url);
  const webUrl = cleanText(raw.web_url);
  const pacerCourtId = cleanText(raw.court_id).toUpperCase();
  const slug =
    inferCourtSlugFromUrl(rssUrl) ||
    inferCourtSlugFromUrl(loginUrl) ||
    inferCourtSlugFromUrl(webUrl) ||
    inferCourtSlugFromPacerCourtId(pacerCourtId);

  return {
    slug,
    pacerCourtId,
    type: cleanText(raw.type),
    title: cleanText(raw.title),
    courtName: cleanText(raw.court_name) || cleanText(raw.title),
    rssUrl,
    loginUrl,
    webUrl
  };
}

export async function fetchPacerCourtDirectory(lookupUrl, { timeoutMs = 15000 } = {}) {
  if (!String(lookupUrl || "").trim()) {
    return [];
  }

  return fetchPacerCourtDirectoryWithRetry(lookupUrl, { timeoutMs }, 0);
}

async function fetchPacerCourtDirectoryWithRetry(lookupUrl, { timeoutMs }, attempt) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(lookupUrl, {
      headers: {
        accept: "application/json",
        "user-agent": "tro-case-watch/0.1"
      },
      signal: controller.signal
    });
    const text = await response.text();

    if (!response.ok) {
      if (attempt < 2 && shouldRetryStatus(response.status)) {
        await wait(retryDelayMs(attempt));
        return fetchPacerCourtDirectoryWithRetry(lookupUrl, { timeoutMs }, attempt + 1);
      }

      const error = new Error(`PACER court directory request failed: ${response.status}`);
      error.status = response.status;
      error.body = text;
      throw error;
    }

    const payload = JSON.parse(text);
    const rows = Array.isArray(payload?.data) ? payload.data : [];
    return rows
      .map(normalizeCourtDirectoryEntry)
      .filter((entry) => entry.slug || entry.pacerCourtId || entry.rssUrl);
  } catch (error) {
    if (attempt < 2 && shouldRetryError(error)) {
      await wait(retryDelayMs(attempt));
      return fetchPacerCourtDirectoryWithRetry(lookupUrl, { timeoutMs }, attempt + 1);
    }

    if (error?.name === "AbortError") {
      const timeoutError = new Error(`PACER court directory request timed out after ${timeoutMs}ms`);
      timeoutError.code = "ETIMEDOUT";
      throw timeoutError;
    }

    throw error;
  } finally {
    clearTimeout(timer);
  }
}

export function buildCourtDirectoryMaps(entries = []) {
  const bySlug = new Map();
  const byPacerCourtId = new Map();

  for (const entry of entries) {
    if (entry.slug && !bySlug.has(entry.slug)) {
      bySlug.set(entry.slug, entry);
    }
    if (entry.pacerCourtId && !byPacerCourtId.has(entry.pacerCourtId)) {
      byPacerCourtId.set(entry.pacerCourtId, entry);
    }
  }

  return {
    bySlug,
    byPacerCourtId
  };
}
