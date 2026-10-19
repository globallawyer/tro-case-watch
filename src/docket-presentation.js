import { isPriorityFeedPrimarySource } from "./priority-feed.js";
import { getRssDocketEvidence } from "./docket-rss.js";
import { analyzeDocketSequence, getIntegerDocketSequence, hasUnverifiedMirrorNumber } from "./docket-sequence.js";

export const DOCKET_PRESENTATION_VERSION = "priority-date-20261019";

export function isPrimaryDocketEntry(entry = {}) {
  // Some legacy RSS notifications were stored under the mirror provider name.
  return isPriorityFeedPrimarySource(entry.primary_source) && !getRssDocketEvidence(entry);
}

export function docketTimelineTime(entry = {}) {
  const value = Date.parse(String(entry.filed_at || ""));
  return Number.isFinite(value) ? value : -Infinity;
}

export function getDocketPresentationPolicy(entries = [], { truncated = false } = {}) {
  const sequence = analyzeDocketSequence(entries);
  const datedNumbers = entries.filter(entry => getIntegerDocketSequence(entry) && entry.filed_at)
    .map(entry => ({ number: getIntegerDocketSequence(entry), day: String(entry.filed_at).slice(0, 10) }))
    .sort((left, right) => left.number - right.number);
  let latestDay = "";
  let dateConflict = false;
  for (const entry of datedNumbers) {
    if (latestDay && entry.day < latestDay) dateConflict = true;
    latestDay = entry.day > latestDay ? entry.day : latestDay;
  }
  const unverified = entries.some(hasUnverifiedMirrorNumber);
  return {
    version: DOCKET_PRESENTATION_VERSION,
    order: "date-desc",
    hideNumbers: Boolean(truncated || unverified || dateConflict || sequence.missingCount > 0),
    reason: truncated ? "partial-timeline" : unverified || dateConflict || sequence.missingCount > 0
      ? "unreliable-numbering" : null
  };
}
