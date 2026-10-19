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
  const chronologicalNumbers = entries.map(entry => ({ number: getIntegerDocketSequence(entry), time: docketTimelineTime(entry) }))
    .filter(entry => entry.number !== null)
    .sort((left, right) => {
      const difference = right.time - left.time;
      return Number.isNaN(difference) || !difference ? right.number - left.number : difference;
    });
  let previousNumber = Infinity;
  let dateConflict = false;
  for (const entry of chronologicalNumbers) {
    if (entry.number > previousNumber) dateConflict = true;
    previousNumber = entry.number;
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
