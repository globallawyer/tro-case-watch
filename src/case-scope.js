import { caseHasPriorityFeedUrl, getPriorityFeedRaw } from "./priority-feed.js";

const IP_PATTERNS = [
  /\btrademark\b/i,
  /\bcopyright\b/i,
  /\bpatent\b/i,
  /\blanham act\b/i,
  /\bcounterfeit\b/i,
  /\binfringement\b/i,
  /\bdesign patent\b/i
];

const PLATFORM_PATTERNS = [
  /\bamazon\b/i,
  /\baliexpress\b/i,
  /\bdhgate\b/i,
  /\bebay\b/i,
  /\bmarketplace\b/i,
  /\bseller ids?\b/i,
  /\balibaba\b/i,
  /\bwalmart\b/i
];

const TRO_PATTERNS = [
  /\btemporary restraining order\b/i,
  /\bmotion for temporary restraining order\b/i,
  /\border granting ex parte application for entry of temporary restraining order\b/i,
  /\border on motion for temporary restraining order\b/i,
  /\bpreliminary injunction\b/i
];

const STRICT_SCHEDULE_A_PATTERNS = [
  /\bidentified on schedule a\b/i,
  /\bschedule a defendants\b/i,
  /\bunincorporated associations identified on schedule a\b/i,
  /\bpartnerships and unincorporated associations\b/i,
  /\bschedule a\b(?!\s*\/)/i
];

const NON_TARGET_PATTERNS = [
  { key: "2241", regex: /\b28:2241\b/i },
  { key: "1105a", regex: /\b8:1105\(a\)\b/i },
  { key: "habeas", regex: /\bhabeas corpus\b/i },
  { key: "alien-detainee", regex: /\balien detainee(?:s)?\b/i },
  { key: "immigration", regex: /\bimmigration\b/i },
  { key: "uscis", regex: /\bcitizenship and immigration services\b/i },
  { key: "detention-center", regex: /\bdetention center\b/i },
  { key: "detention", regex: /\bdetention\b/i },
  { key: "deportation", regex: /\bdeport(?:ation|ed)?\b/i },
  { key: "warden", regex: /\bwarden\b/i },
  { key: "ice", regex: /\bimmigration and customs enforcement\b|\bice\b/i },
  { key: "ins-detainee", regex: /\bins detainee\b/i },
  { key: "nos-463", regex: /\b463 habeas corpus\b/i },
  { key: "nos-460", regex: /\b460 deportation\b/i }
];

function normalizeText(value) {
  return String(value || "").trim().toLowerCase();
}

function uniqueStrings(values = []) {
  return [...new Set(values.filter(Boolean).map((value) => String(value).trim()).filter(Boolean))];
}

function parseTagMarker(tagsMarker = "") {
  return uniqueStrings(String(tagsMarker || "").split("|"));
}

function getTags(caseLike = {}) {
  if (Array.isArray(caseLike.tags)) {
    return uniqueStrings(caseLike.tags.map((value) => String(value || "").trim().toLowerCase()));
  }

  return parseTagMarker(caseLike.tags_marker).map((value) => value.toLowerCase());
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

export function collectScopeText(caseLike = {}) {
  const raw = caseLike.raw && typeof caseLike.raw === "object" ? caseLike.raw : {};
  const recapDocuments = asArray(raw.recap_documents);
  const entries = asArray(caseLike.entries);

  return normalizeText([
    caseLike.case_name,
    caseLike.case_name_full,
    caseLike.court_name,
    caseLike.cause,
    caseLike.nature_of_suit,
    caseLike.recent_activity_summary,
    ...asArray(caseLike.plaintiffs),
    ...asArray(caseLike.defendants),
    ...asArray(raw.party),
    ...recapDocuments.flatMap((doc) => [doc.short_description, doc.description, doc.snippet]),
    ...entries.map((entry) => entry.description)
  ].filter(Boolean).join(" | "));
}

function countRegexMatches(text = "", patterns = []) {
  return patterns.reduce((count, pattern) => count + (pattern.test(text) ? 1 : 0), 0);
}

export function hasStrictScheduleATerm(input = {}) {
  const text = typeof input === "string" ? normalizeText(input) : collectScopeText(input);
  return STRICT_SCHEDULE_A_PATTERNS.some((pattern) => pattern.test(text));
}

function hasSellerSignals(caseLike = {}) {
  const text = collectScopeText(caseLike);
  const ipHits = countRegexMatches(text, IP_PATTERNS);
  const platformHits = countRegexMatches(text, PLATFORM_PATTERNS);
  const hasTroSignal = TRO_PATTERNS.some((pattern) => pattern.test(text));
  const hasStrictScheduleA = hasStrictScheduleATerm(text);
  const tags = new Set(getTags(caseLike));

  if (caseHasPriorityFeedUrl(caseLike) || Boolean(getPriorityFeedRaw(caseLike)?.url)) {
    return true;
  }

  if (hasStrictScheduleA) {
    return true;
  }

  if (tags.has("schedule_a") && (ipHits > 0 || platformHits > 0 || hasTroSignal)) {
    return true;
  }

  if (tags.has("seller_tro") && (ipHits > 0 || platformHits > 0 || hasTroSignal)) {
    return true;
  }

  return ipHits > 0 && (platformHits > 0 || hasTroSignal);
}

export function evaluateCaseScope(caseLike = {}) {
  const text = collectScopeText(caseLike);
  const nonTargetHits = NON_TARGET_PATTERNS
    .filter(({ regex }) => regex.test(text))
    .map(({ key }) => key);
  const sellerProtected = hasSellerSignals(caseLike);
  const strictScheduleA = hasStrictScheduleATerm(text);

  return {
    text,
    nonTargetHits,
    sellerProtected,
    hasStrictScheduleA: strictScheduleA,
    isOutOfScope: nonTargetHits.length > 0 && !sellerProtected
  };
}

export function isOutOfScopeCase(caseLike = {}) {
  return evaluateCaseScope(caseLike).isOutOfScope;
}
