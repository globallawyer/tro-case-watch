export const discoveryPresets = [
  {
    key: "tro_phrase",
    label: "TRO",
    query: "\"temporary restraining order\"",
    tags: ["tro"]
  },
  {
    key: "tro_motion_ip",
    label: "TRO Motion",
    query:
      "\"motion for temporary restraining order\" AND (trademark OR copyright OR patent OR seller OR marketplace OR amazon OR \"Lanham Act\")",
    tags: ["tro", "seller_tro"]
  },
  {
    key: "tro_order_ip",
    label: "TRO Order",
    query:
      "\"order granting ex parte application for entry of temporary restraining order\" OR \"order on motion for temporary restraining order\"",
    tags: ["tro", "seller_tro"]
  },
  {
    key: "schedule_a_phrase",
    label: "Schedule A",
    query:
      "\"Schedule A\" AND (trademark OR copyright OR patent OR infringement OR counterfeit OR seller OR marketplace OR amazon OR \"Lanham Act\" OR \"unincorporated associations\")",
    tags: ["schedule_a"]
  },
  {
    key: "schedule_a_defendants",
    label: "Schedule A Defendants",
    query: "\"identified on Schedule A\" OR \"Schedule A Defendants\"",
    tags: ["schedule_a"]
  }
];

const TRO_PATTERNS = [
  "temporary restraining order",
  "ex parte tro",
  "ex parte temporary restraining order",
  "motion for temporary restraining order",
  "order granting ex parte application for entry of temporary restraining order",
  "order on motion for temporary restraining order"
];

const SCHEDULE_A_STRONG_PATTERNS = [
  "identified on schedule a",
  "schedule a defendants",
  "partnerships and unincorporated associations",
  "unincorporated associations identified on schedule a"
];

const SELLER_PATTERNS = [
  "seller",
  "seller ids",
  "marketplace",
  "amazon",
  "aliexpress",
  "dhgate",
  "ebay",
  "store",
  "shop",
  "trademark",
  "copyright",
  "patent",
  "lanham act"
];

const IP_PATTERNS = [
  "trademark",
  "copyright",
  "patent",
  "counterfeit",
  "infringement",
  "lanham act"
];

const BANKRUPTCY_PATTERNS = [
  "schedule a/b",
  "summary of assets and liabilities",
  "declaration about individual debtors schedules",
  "bankruptcy",
  "debtor",
  "chapter 7",
  "chapter 11",
  "chapter 13",
  "trustee"
];

function normalize(value) {
  return String(value || "").toLowerCase();
}

function includesOne(text, patterns) {
  return patterns.some((pattern) => text.includes(pattern));
}

export function classifyCase(searchResult, presetTags = []) {
  const text = normalize([
    searchResult.caseName,
    searchResult.case_name_full,
    searchResult.court,
    searchResult.cause,
    ...(searchResult.party || []),
    ...((searchResult.recap_documents || []).map((item) => item.short_description || item.description || ""))
  ].join(" | "));

  const tags = new Set();
  const requestedTro = presetTags.includes("tro");
  const requestedScheduleA = presetTags.includes("schedule_a");
  const hasTro = includesOne(text, TRO_PATTERNS);
  const hasSeller = includesOne(text, SELLER_PATTERNS);
  const hasIp = includesOne(text, IP_PATTERNS);
  const hasBankruptcy = includesOne(text, BANKRUPTCY_PATTERNS);
  const hasStrongScheduleA = includesOne(text, SCHEDULE_A_STRONG_PATTERNS);
  const hasLooseScheduleA = text.includes("schedule a");

  if (requestedTro || hasTro) {
    tags.add("tro");
  }

  if (
    !hasBankruptcy &&
    (hasStrongScheduleA || requestedScheduleA || (hasLooseScheduleA && (hasIp || hasSeller || hasTro)))
  ) {
    tags.add("schedule_a");
  }

  if (!hasBankruptcy && (tags.has("schedule_a") || (tags.has("tro") && (hasSeller || hasIp)))) {
    tags.add("seller_tro");
  }

  return [...tags];
}

export function buildTagsMarker(tags) {
  const clean = [...new Set(tags)].filter(Boolean).sort();
  return clean.length ? `|${clean.join("|")}|` : "";
}
