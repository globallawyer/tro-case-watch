function decodeText(codes = []) {
  return String.fromCharCode(...codes);
}

export const PRIORITY_FEED_SOURCE = decodeText([119, 111, 114, 108, 100, 116, 114, 111]);
export const PRIORITY_FEED_HOST = [
  decodeText([119, 111, 114, 108, 100, 116, 114, 111]),
  decodeText([99, 111, 109])
].join(".");
export const PRIORITY_FEED_BASE_URL = `https://${PRIORITY_FEED_HOST}`;
export const PRIORITY_FEED_ENTRY_SOURCE = PRIORITY_FEED_SOURCE;
export const PRIORITY_FEED_PROVIDER_KEY = "priority";
export const PRIORITY_FEED_PUBLIC_LABEL = "站内归档";
export const OFFICIAL_DOCKET_PROVIDER_KEY = "official";
export const FALLBACK_PROVIDER_KEY = "fallback";
const LEGACY_ENV_PREFIX = decodeText([87, 79, 82, 76, 68, 84, 82, 79]);
const LEGACY_RAW_KEY = PRIORITY_FEED_SOURCE;
const MODERN_RAW_KEY = "priorityFeed";
export const PRIORITY_FEED_LEGACY_RAW_KEY = LEGACY_RAW_KEY;
export const PRIORITY_FEED_MODERN_RAW_KEY = MODERN_RAW_KEY;
export const PRIORITY_FEED_DISCOVERY_CHECKPOINT = "priority-feed:discovery";

export function buildLegacyPriorityFeedEnvKey(suffix = "") {
  return `${LEGACY_ENV_PREFIX}_${String(suffix || "").trim()}`;
}

export function getPriorityFeedRaw(source = {}) {
  const container =
    source && typeof source === "object" && source.raw && typeof source.raw === "object"
      ? source.raw
      : source;
  return container?.[MODERN_RAW_KEY] || container?.[LEGACY_RAW_KEY] || null;
}

export function mergePriorityFeedRaw(source = {}, patch = {}) {
  const container =
    source && typeof source === "object" && source.raw && typeof source.raw === "object"
      ? source.raw
      : source;
  return {
    ...(container || {}),
    [MODERN_RAW_KEY]: {
      ...(getPriorityFeedRaw(source) || {}),
      ...patch
    }
  };
}

export function getPriorityFeedRowCount(source = {}) {
  return Math.max(0, Number(getPriorityFeedRaw(source)?.rowCount || 0));
}

export function getPriorityFeedSyncedAt(source = {}) {
  return getPriorityFeedRaw(source)?.syncedAt || null;
}

export function isPriorityFeedMissing(source = {}) {
  return Boolean(getPriorityFeedRaw(source)?.missing);
}

export function sourceUrlUsesPriorityFeed(value = "") {
  return String(value || "").toLowerCase().includes(PRIORITY_FEED_HOST);
}

export function caseHasPriorityFeedUrl(caseLike = {}) {
  return (caseLike?.source_urls || []).some((value) => sourceUrlUsesPriorityFeed(value));
}

export function isPriorityFeedPrimarySource(value = "") {
  return String(value || "").trim().toLowerCase() === PRIORITY_FEED_ENTRY_SOURCE;
}

export function publicProviderLabel(value = "") {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) {
    return "review";
  }

  if (normalized === PRIORITY_FEED_SOURCE) {
    return PRIORITY_FEED_PROVIDER_KEY;
  }

  if (normalized === "courtlistener") {
    return OFFICIAL_DOCKET_PROVIDER_KEY;
  }

  if (normalized === "pacermonitor") {
    return FALLBACK_PROVIDER_KEY;
  }

  if (normalized === "docketalarm" || normalized === "unicourt") {
    return FALLBACK_PROVIDER_KEY;
  }

  return normalized;
}
