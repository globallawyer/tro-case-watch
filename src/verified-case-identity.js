import { normalizeDocketIdentity } from "./insights.js";

export function plaintiffIdentity(value = "") {
  return String(value).replace(/^\s*(?:[a-z]+-)?\d+:\d{2,4}-cv-\d+\s*/i, "")
    .split(/\s+v\.?\s+/i)[0].replace(/\bet\s+al\.?/gi, "")
    .toLowerCase().replace(/[^a-z0-9]/g, "");
}

// A missing office is unknown, not office 1. Upgrade it only from a fully
// identified source with independently matching court, plaintiff and filing date.
export function verifiesMissingOffice(reference = {}, candidate = {}) {
  const short = normalizeDocketIdentity(reference.docketNumber || reference.docket_number);
  const full = normalizeDocketIdentity(candidate.docketNumber || candidate.docket_number);
  const court = String(reference.courtId || reference.court_id || "").toLowerCase();
  const candidateCourt = String(candidate.courtId || candidate.court_id || "").toLowerCase();
  const plaintiff = plaintiffIdentity(reference.plaintiff || reference.caseName || reference.case_name);
  const candidatePlaintiff = plaintiffIdentity(candidate.plaintiff || candidate.caseName || candidate.case_name);
  const filedAt = String(reference.dateFiled || reference.date_filed || "").slice(0, 10);
  const candidateFiledAt = String(candidate.dateFiled || candidate.date_filed || "").slice(0, 10);
  return Boolean(short && !/^\d+:/.test(short) && /^\d+:/.test(full) &&
    full.replace(/^\d+:/, "") === short && /^[a-z]{2}(?:nd|sd|ed|wd|md|cd|d)$/.test(court) &&
    court === candidateCourt && plaintiff.length >= 8 && plaintiff === candidatePlaintiff &&
    !/^(?:unknown|unidentified)/.test(plaintiff) && /^\d{4}-\d{2}-\d{2}$/.test(filedAt) && filedAt === candidateFiledAt);
}
