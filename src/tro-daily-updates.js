import fs from "node:fs";

function normalizeText(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function normalizeUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return null;
  }

  try {
    const url = new URL(raw);
    if (!["http:", "https:"].includes(url.protocol)) {
      return null;
    }
    return url.toString();
  } catch {
    return null;
  }
}

function normalizeSources(item = {}) {
  const values = Array.isArray(item.sources)
    ? item.sources
    : item.source
      ? [item.source]
      : [];

  return [...new Set(values.map(normalizeText).filter(Boolean))];
}

function parseTimeMs(value) {
  const ms = Date.parse(String(value || ""));
  return Number.isFinite(ms) ? ms : 0;
}

function normalizeItem(item = {}, index = 0) {
  const title = normalizeText(item.title || item.headline || item.summary_title || "");
  const summary = normalizeText(item.summary || item.note || item.description || "");
  const sources = normalizeSources(item);
  const url = normalizeUrl(item.url || item.link || item.href || "");
  const publishedAt = normalizeText(item.publishedAt || item.published_at || item.date || "");
  const heat = Number(item.heat ?? item.score ?? item.priority ?? 0);
  const caseRefs = Array.isArray(item.caseRefs)
    ? item.caseRefs.map(normalizeText).filter(Boolean)
    : [];

  if (!title) {
    return null;
  }

  return {
    id: normalizeText(item.id || `${publishedAt || "item"}-${index}`),
    title,
    summary,
    sources,
    url,
    publishedAt: publishedAt || null,
    heat: Number.isFinite(heat) ? heat : 0,
    caseRefs
  };
}

export function loadTroDailyUpdates(reportConfig = {}) {
  const filePath = String(reportConfig.path || "").trim();
  const maxItems = Math.max(1, Number(reportConfig.maxItems || 3));

  if (!filePath || !fs.existsSync(filePath)) {
    return {
      updatedAt: null,
      items: [],
      total: 0
    };
  }

  try {
    const rawText = fs.readFileSync(filePath, "utf-8");
    const parsed = JSON.parse(rawText);
    const rawItems = Array.isArray(parsed) ? parsed : Array.isArray(parsed.items) ? parsed.items : [];
    const normalizedItems = rawItems
      .map((item, index) => normalizeItem(item, index))
      .filter(Boolean)
      .sort((left, right) => {
        const heatDiff = Number(right.heat || 0) - Number(left.heat || 0);
        if (heatDiff !== 0) {
          return heatDiff;
        }

        return parseTimeMs(right.publishedAt) - parseTimeMs(left.publishedAt);
      });

    return {
      updatedAt: normalizeText(parsed.updatedAt || parsed.updated_at || "") || null,
      items: normalizedItems.slice(0, maxItems),
      total: normalizedItems.length
    };
  } catch (error) {
    console.error("[tro-daily-updates]", error);
    return {
      updatedAt: null,
      items: [],
      total: 0,
      error: error.message
    };
  }
}
