export function syncTaskOutcome(result = {}) {
  const counts = value => Object.values(value || {}).reduce((total, count) => total + (Number(count) || 0), 0);
  const failures = Number(result.failedCases || result.emptyCases || result.refreshErrors || result.failedFeeds || result.failedSources || 0)
    + counts(result.hydrationFailures) + counts(result.refreshFailures);
  const successes = Number(result.syncedCases || result.refreshedCases || result.hydratedItems || result.successfulFeeds || result.successfulSources || result.docketEntriesUpserted || 0);
  if (result.outcome === "unresolved" || (failures > 0 && successes === 0)) return "failed";
  if (failures > 0 || result.outcome === "partial") return "partial";
  return "succeeded";
}
