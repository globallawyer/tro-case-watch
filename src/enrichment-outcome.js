export function didEnrichmentComplete(result) {
  return Boolean(result && result.enriched !== false && result.skipped !== true);
}
