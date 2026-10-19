export function didEnrichmentComplete(result) {
  return Boolean(result && result.enriched !== false && result.skipped !== true &&
    (result.enriched === true || Number(result.entries || result.docketEntriesUpserted || 0) > 0));
}

export function enrichmentFailure(error = {}, provider = null) {
  const status = Number(error.status || 0) || null;
  const code = String(error.code || error.cause?.code || error.name || "REQUEST_FAILED");
  const message = String(error.error || error.message || error.reason || "request failed").slice(0, 400);
  const blocked = status === 403 || status === 429 || /challenge|rate.?limit|captcha/i.test(`${code} ${message}`);
  const retryable = error.retryable ?? (blocked || !status || [408, 409, 425, 500, 502, 503, 504].includes(status));
  return {
    provider,
    error: message,
    code,
    status,
    retryable: Boolean(retryable),
    deferred: Boolean(error.deferred),
    retryAfterMs: Math.max(Number(error.retryAfterMs || 0), blocked ? 60 * 60 * 1000 : 0)
  };
}

export function enrichmentResultFailure(result, provider) {
  if (didEnrichmentComplete(result)) return null;
  const reason = String(result?.reason || "empty-result");
  if (["not-applicable", "filtered"].includes(reason)) return null;
  const deferred = ["fresh", "cooldown"].includes(reason);
  return enrichmentFailure({
    error: reason,
    code: deferred ? "COOLDOWN" : reason.toUpperCase().replace(/-/g, "_"),
    retryable: result?.contentAvailable !== false && !["not-found", "not_found", "identity-mismatch"].includes(reason),
    deferred,
    retryAfterMs: Number(result?.retryAfterMs || 0) || (deferred ? 2 * 60 * 60 * 1000 : 0)
  }, provider);
}

export function nextEnrichmentProviderState(previous = {}, failure, { now = Date.now() } = {}) {
  if (!failure) return { failures: 0, nextAttemptAt: null, checkedAt: new Date(now).toISOString() };
  if (!failure.retryable || failure.deferred ||
      failure.status === 404 ||
      ["EMPTY", "METADATA_ONLY", "EMPTY_RESULT", "EMPTY_CASE_SEARCH"].includes(failure.code)) return previous;
  const failures = Math.min(Number(previous.failures || 0) + 1, 12);
  const delay = Math.max(Number(failure.retryAfterMs || 0), Math.min(6 * 60 * 60 * 1000, 10 * 60 * 1000 * 2 ** (failures - 1)));
  return {
    failures,
    nextAttemptAt: new Date(now + delay).toISOString(),
    checkedAt: new Date(now).toISOString(),
    code: failure.code,
    status: failure.status
  };
}

export function planEnrichmentRetry(job, result, {
  maxAttempts = 6,
  retryDelayMs = 20 * 60 * 1000,
  memoryRetryDelayMs = 10 * 60 * 1000
} = {}) {
  const failures = result.failedProviders || [];
  const requested = result.requestedProviders || job.providers || [];
  const completed = new Set(result.completedProviders || []);
  const failed = new Map(failures.map(item => [item.provider, item]));
  const remaining = requested.filter(provider => !completed.has(provider) &&
    (failed.get(provider)?.retryable || (result.stoppedReason && !failed.has(provider))));
  const hadFailure = failures.some(item => item.retryable && !item.deferred);
  const retryAttempts = Number(job.retryAttempts || 0) + (hadFailure ? 1 : 0);
  const exhausted = hadFailure && retryAttempts >= Math.max(1, maxAttempts);
  const delay = result.stoppedReason === "memory-pressure"
    ? memoryRetryDelayMs
    : Math.max(retryDelayMs * (hadFailure ? 2 ** Math.min(Math.max(retryAttempts - 1, 0), 4) : 1),
        ...failures.filter(item => remaining.includes(item.provider)).map(item => Number(item.retryAfterMs || 0)));
  return {
    providers: exhausted ? [] : remaining,
    retryAttempts,
    requeueAfterMs: remaining.length && !exhausted ? Math.max(60 * 1000, delay) : 0,
    exhausted,
    unresolved: exhausted ? failures : failures.filter(item => !item.retryable)
  };
}
