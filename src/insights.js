import { getPriorityFeedRaw } from "./priority-feed.js";

const IP_TERMS = [
  "trademark",
  "copyright",
  "patent",
  "lanham act",
  "counterfeit",
  "infringement",
  "design patent"
];

const PLATFORM_TERMS = [
  "amazon",
  "aliexpress",
  "dhgate",
  "ebay",
  "marketplace",
  "seller",
  "seller ids",
  "e-commerce",
  "ecommerce",
  "shop",
  "store",
  "delivery",
  "alibaba",
  "walmart"
];

const NEGATIVE_TERMS = [
  "habeas",
  "immigration",
  "detention",
  "bankruptcy",
  "prison",
  "warden",
  "social security",
  "custody",
  "deport",
  "medicaid"
];

const SCHEDULE_A_PATTERNS = [
  "identified on schedule a",
  "schedule a defendants",
  "schedule a",
  "partnerships and unincorporated associations",
  "unincorporated associations identified on schedule a"
];

const TRO_PATTERNS = [
  "temporary restraining order",
  "motion for temporary restraining order",
  "order granting ex parte application for entry of temporary restraining order",
  "order on motion for temporary restraining order"
];

const BANKRUPTCY_TERMS = [
  "schedule a/b",
  "summary of assets and liabilities",
  "declaration about individual debtors schedules",
  "chapter 7",
  "chapter 11",
  "chapter 13",
  "debtor",
  "trustee"
];

const COMPANY_SUFFIX = /\b(llc|incorporated|inc|corp|corporation|company|co|ltd|limited|gmbh|pllc|llp|lp|p\.a\.|s\.a\.|sarl|b\.v\.)\b\.?,?/gi;

function normalize(value) {
  return String(value || "").trim();
}

export function normalizeText(value) {
  return normalize(value).toLowerCase();
}

function uniqueStrings(values) {
  return [...new Set(values.filter(Boolean).map((value) => normalize(value)))];
}

function normalizedKey(value) {
  return normalizeText(value).replace(/[^\w]+/g, " ").replace(/\s+/g, " ").trim();
}

function pullPlaintiffNames(caseLike) {
  const title = normalize(caseLike.case_name);
  const titlePieces = title.split(/\s(?:v\.|vs\.)\s/i);
  const plaintiffFromCaption = titlePieces[0] || "";
  const defendantFromCaption = titlePieces.slice(1).join(" v. ").trim();
  const parties = Array.isArray(caseLike.raw?.party) ? uniqueStrings(caseLike.raw.party) : [];

  if (defendantFromCaption && parties.length) {
    const defendantKey = normalizedKey(defendantFromCaption);
    const defendantIndex = parties.findIndex((party) => normalizedKey(party) === defendantKey);
    if (defendantIndex > 0) {
      return uniqueStrings(parties.slice(0, defendantIndex));
    }
  }

  if (Array.isArray(caseLike.plaintiffs) && caseLike.plaintiffs.length) {
    return uniqueStrings(caseLike.plaintiffs);
  }

  if (title) {
    if (plaintiffFromCaption) {
      return [plaintiffFromCaption];
    }
  }

  return parties[0] ? [parties[0]] : [];
}

function pullPlaintiffName(caseLike) {
  return pullPlaintiffNames(caseLike)[0] || "";
}

function pullLawFirms(caseLike) {
  const priorityFeedFirm = normalize(getPriorityFeedRaw(caseLike.raw)?.lawFirm);
  if (priorityFeedFirm) {
    return [priorityFeedFirm];
  }

  const rawFirms = caseLike.raw?.firm;
  if (Array.isArray(rawFirms) && rawFirms.length) {
    return uniqueStrings(rawFirms);
  }

  return [];
}

function cleanBrandName(value) {
  return normalize(value)
    .replace(COMPANY_SUFFIX, " ")
    .replace(/\s*,\s*/g, " ")
    .replace(/[.]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function collectTexts(caseLike) {
  const rawDocs = Array.isArray(caseLike.raw?.recap_documents) ? caseLike.raw.recap_documents : [];
  const entries = Array.isArray(caseLike.entries) ? caseLike.entries : [];

  return [
    caseLike.case_name,
    caseLike.cause,
    caseLike.nature_of_suit,
    caseLike.recent_activity_summary,
    ...(caseLike.plaintiffs || []),
    ...(caseLike.defendants || []),
    ...rawDocs.flatMap((doc) => [doc.short_description, doc.description, doc.snippet]),
    ...entries.map((entry) => entry.description)
  ]
    .filter(Boolean)
    .join(" | ");
}

function countMatches(text, patterns) {
  return patterns.reduce((count, pattern) => count + (text.includes(pattern) ? 1 : 0), 0);
}

function includesOne(text, patterns) {
  return patterns.some((pattern) => text.includes(pattern));
}

function sanitizeDefendants(caseLike, plaintiffNames = []) {
  const plaintiffKeys = new Set(
    (Array.isArray(plaintiffNames) ? plaintiffNames : [plaintiffNames])
      .map((value) => normalizedKey(value))
      .filter(Boolean)
  );
  const unique = new Map();

  for (const rawValue of caseLike.defendants || []) {
    const value = normalize(rawValue);
    if (!value) {
      continue;
    }

    const key = normalizedKey(value);
    if (!key || plaintiffKeys.has(key)) {
      continue;
    }

    if (!unique.has(key)) {
      unique.set(key, value);
    }
  }

  const title = normalize(caseLike.case_name);
  const pieces = title.split(/\s(?:v\.|vs\.)\s/i);
  if (pieces.length >= 2) {
    const captionDefendant = normalize(pieces.slice(1).join(" v. "));
    const captionKey = normalizedKey(captionDefendant);
    if (captionKey && !plaintiffKeys.has(captionKey) && !unique.has(captionKey)) {
      unique.set(captionKey, captionDefendant);
    }
  }

  return [...unique.values()];
}

function deriveStatus(caseLike, text) {
  const dateTerminated = normalize(caseLike.date_terminated);

  if (
    dateTerminated ||
    text.includes("voluntary dismissal") ||
    text.includes("case terminated") ||
    text.includes("dismisses the instant action")
  ) {
    return {
      key: "closed",
      label: "Dismissal / Closure Seen",
      tone: "muted"
    };
  }

  if (text.includes("preliminary injunction") && (text.includes("grant") || text.includes("grants"))) {
    return {
      key: "pi",
      label: "PI Entered",
      tone: "danger"
    };
  }

  if (
    text.includes("temporary restraining order") &&
    (text.includes("grant") || text.includes("order") || text.includes("entered"))
  ) {
    return {
      key: "tro_granted",
      label: "TRO Granted",
      tone: "danger"
    };
  }

  if (text.includes("motion for temporary restraining order")) {
    return {
      key: "tro_pending",
      label: "TRO Pending",
      tone: "warn"
    };
  }

  if (text.includes("notice of settlement")) {
    return {
      key: "settlement",
      label: "Settlement Signal",
      tone: "good"
    };
  }

  if (text.includes("service") || text.includes("summons")) {
    return {
      key: "service",
      label: "Service in Progress",
      tone: "warn"
    };
  }

  return {
    key: "watch",
    label: "Monitoring",
    tone: "neutral"
  };
}

function buildHighlights(text) {
  const items = [];

  if (text.includes("order granting ex parte application for entry of temporary restraining order")) {
    items.push("TRO Order Seen");
  } else if (text.includes("motion for temporary restraining order")) {
    items.push("TRO Motion Filed");
  }

  if (text.includes("extend temporary restraining order")) {
    items.push("TRO Extension Watch");
  }

  if (text.includes("preliminary injunction") && (text.includes("grant") || text.includes("grants"))) {
    items.push("PI Entered");
  } else if (text.includes("preliminary injunction")) {
    items.push("PI Activity");
  }

  if (text.includes("certificate of service") || text.includes("service")) {
    items.push("Service in Progress");
  }

  if (text.includes("settlement")) {
    items.push("Settlement Signal");
  }

  if (text.includes("dismiss")) {
    items.push("Dismissal Signal");
  }

  return [...new Set(items)].slice(0, 4);
}

function buildActionItems(statusKey) {
  if (statusKey === "tro_granted") {
    return ["Confirm whether the TRO has been entered", "Watch the PI hearing and response deadlines", "Verify the restraint scope and defendant list"];
  }

  if (statusKey === "pi") {
    return ["Check whether the PI covers all defendants", "Watch follow-up settlement or dismissal activity", "Confirm whether account restraints are still active"];
  }

  if (statusKey === "settlement") {
    return ["Keep watching for dismissal activity", "Confirm whether your store was separately removed", "Preserve settlement or dismissal evidence"];
  }

  if (statusKey === "closed") {
    return ["Verify the court's final order", "Confirm whether marketplace restraints have been lifted", "Keep dismissal or closure records"];
  }

  if (statusKey === "service") {
    return ["Track when service is completed", "Watch whether TRO or PI activity follows immediately", "Confirm whether a defendant list has appeared"];
  }

  return ["Focus on TRO filings", "Focus on PI / Preliminary Injunction activity", "Focus on settlement or dismissal signals"];
}

function buildNarrative({ status, sellerRelevant, isScheduleACase, isTroCase, defendantCount }) {
  if (!sellerRelevant) {
    return "Continue monitoring based on the public docket.";
  }

  if (status.key === "tro_granted") {
    return "This is a classic seller-freeze stage. TRO entry, platform restraints, and service are the main risk points right now.";
  }

  if (status.key === "pi") {
    return "The case has moved into the PI stage, where restraints are usually more durable than the initial TRO.";
  }

  if (status.key === "settlement") {
    return "There are settlement signals on the docket. Watch for defendants exiting the case over time.";
  }

  if (status.key === "closed") {
    return "The case shows closure or dismissal signals. Focus next on whether platforms are releasing restraints.";
  }

  if (isScheduleACase) {
    return `This looks like a classic Schedule A seller case with ${defendantCount} visible defendant leads. Keep tracking TRO / PI / settlement developments.`;
  }

  if (isTroCase) {
    return "This appears to be a seller-related TRO case. Prioritize TRO entry, extensions, and whether the case moves into PI.";
  }

  return "The case is still in a monitoring phase. Prioritize TRO, PI, service, settlement, and dismissal milestones.";
}

export function deriveCaseInsights(caseLike) {
  const text = normalizeText(collectTexts(caseLike));
  const plaintiffNames = pullPlaintiffNames(caseLike);
  const plaintiffName = plaintiffNames[0] || "";
  const defendants = sanitizeDefendants(
    caseLike,
    plaintiffNames.length ? plaintiffNames : [plaintiffName]
  );
  const lawFirms = pullLawFirms(caseLike);
  const tags = Array.isArray(caseLike.tags) ? caseLike.tags : [];
  const ipHits = countMatches(text, IP_TERMS);
  const platformHits = countMatches(text, PLATFORM_TERMS);
  const negativeHits = countMatches(text, NEGATIVE_TERMS);
  const hasTroTerm = includesOne(text, TRO_PATTERNS);
  const hasScheduleATerm = includesOne(text, SCHEDULE_A_PATTERNS);
  const hasBankruptcyTerm =
    includesOne(text, BANKRUPTCY_TERMS) || normalizeText(caseLike.court_name).includes("bankruptcy");
  const defendantCount = defendants.length;

  const isScheduleACase =
    !hasBankruptcyTerm &&
    (tags.includes("schedule_a") ||
      text.includes("identified on schedule a") ||
      text.includes("schedule a defendants") ||
      text.includes("partnerships and unincorporated associations") ||
      (hasScheduleATerm && (ipHits >= 1 || platformHits >= 1 || hasTroTerm)));
  const isTroCase = tags.includes("tro") || hasTroTerm;

  let sellerRelevanceScore = 0;

  if (isScheduleACase) sellerRelevanceScore += 8;
  if (isTroCase) sellerRelevanceScore += 2;
  sellerRelevanceScore += ipHits >= 1 ? 3 : 0;
  sellerRelevanceScore += platformHits >= 2 ? 4 : platformHits >= 1 ? 2 : 0;
  sellerRelevanceScore += defendantCount >= 5 ? 2 : 0;
  sellerRelevanceScore -= negativeHits >= 1 && ipHits === 0 ? 6 : 0;
  sellerRelevanceScore -= hasBankruptcyTerm ? 8 : 0;

  const sellerRelevant =
    !hasBankruptcyTerm &&
    (isScheduleACase ||
      ((ipHits >= 1 || isTroCase || text.includes("temporary restraining order")) &&
        (platformHits >= 1 || defendantCount >= 5 || text.includes("seller ids") || text.includes("marketplace"))));
  const status = deriveStatus(caseLike, text);
  const brandName = cleanBrandName(getPriorityFeedRaw(caseLike.raw)?.brand || plaintiffName);
  const highlights = buildHighlights(text);
  const actionItems = buildActionItems(status.key);
  const narrative = buildNarrative({
    status,
    sellerRelevant,
    isScheduleACase,
    isTroCase,
    defendantCount
  });

  const badges = [];
  if (isScheduleACase) badges.push("Schedule A");
  if (isTroCase) badges.push("TRO");
  if (sellerRelevant) badges.push("Cross-Border Seller");
  if (lawFirms[0]) badges.push(`Plaintiff Counsel ${lawFirms[0]}`);

  return {
    plaintiff_name: plaintiffName || null,
    brand_name: brandName || null,
    lead_law_firm: lawFirms[0] || null,
    law_firms: lawFirms,
    defendant_count: defendantCount,
    defendant_preview: defendants.slice(0, 6),
    seller_relevance_score: sellerRelevanceScore,
    is_schedule_a_case: isScheduleACase,
    is_tro_case: isTroCase,
    is_seller_case: sellerRelevant,
    is_bankruptcy_case: hasBankruptcyTerm,
    status,
    highlights,
    action_items: actionItems,
    narrative,
    badges
  };
}

export function docketLooksLike(value) {
  return /\b\d{2}-cv-\d{3,6}\b/i.test(String(value || "")) || /\b\d+:\d{2}-cv-\d{3,6}\b/i.test(String(value || ""));
}

export function normalizeDocket(value) {
  return normalizeText(value).replace(/^[a-z]{1,4}[-:]/i, "").replace(/^\d+:/, "");
}
