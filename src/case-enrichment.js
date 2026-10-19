import { SIGNAL_FEED_PROVIDER_KEY } from "./providers/signal-feed.js";
import { PRIORITY_FEED_SOURCE } from "./priority-feed.js";
import { didEnrichmentComplete, enrichmentFailure, enrichmentResultFailure, nextEnrichmentProviderState } from "./enrichment-outcome.js";

export function createCaseEnrichmentRefresher({ store, syncService, wait = ms => new Promise(resolve => setTimeout(resolve, ms)) }) {
  return async function refreshCaseAcrossSources(caseId, {
  initialItem = null,
  providers = [SIGNAL_FEED_PROVIDER_KEY, "lookup:courtlistener", "recentfilings", "61tro", PRIORITY_FEED_SOURCE, "courtlistener", "pacermonitor", "docketalarm", "unicourt"],
  interProviderDelayMs = 0,
  respectProviderBackoff = false,
  shouldContinue = null
} = {}) {
  const expandedProviders = [...new Set(
    providers.flatMap((provider) => {
      if (provider === "lookup") {
        return ["lookup:courtlistener", "recentfilings", "61tro"];
      }
      if (provider === "lookup:recentfilings") {
        return ["recentfilings"];
      }
      if (provider === "lookup:61tro") {
        return ["61tro"];
      }
      return [provider];
    })
  )].sort((left, right) => Number(right === PRIORITY_FEED_SOURCE) - Number(left === PRIORITY_FEED_SOURCE));
  const completedProviders = [];
  const failedProviders = [];
  const providerResults = [];
  let lookupResult = null;
  let current = store.getCase(caseId) || initialItem || null;
  let stoppedReason = null;

  const refreshCurrent = () => {
    current = store.getCase(caseId) || current;
    return current;
  };

  const shouldRunProvider = async (provider) => {
    if (typeof shouldContinue !== "function") {
      return {
        ok: true,
        reason: null
      };
    }

    const decision = await shouldContinue({
      caseId,
      provider,
      current: refreshCurrent(),
      completedProviders: [...completedProviders],
      failedProviders: [...failedProviders],
      providerResults: [...providerResults]
    });
    if (decision === false) {
      return {
        ok: false,
        reason: "stopped"
      };
    }

    if (decision && typeof decision === "object" && decision.ok === false) {
      return {
        ok: false,
        reason: decision.reason || "stopped"
      };
    }

    return {
      ok: true,
      reason: null
    };
  };

  const runProvider = async (provider, task) => {
    const decision = await shouldRunProvider(provider);
    if (!decision.ok) {
      stoppedReason = decision.reason;
      return false;
    }

    const checkpointKey = `system:enrichment-provider:${provider}`;
    const previous = respectProviderBackoff ? store.getCheckpoint(checkpointKey) || {} : {};
    const cooldownMs = Date.parse(previous.nextAttemptAt || "") - Date.now();
    if (respectProviderBackoff && cooldownMs > 0) {
      failedProviders.push(enrichmentFailure({
        code: "PROVIDER_BACKOFF", error: "source temporarily paused after failures",
        retryable: true, deferred: true, retryAfterMs: cooldownMs
      }, provider));
      return true;
    }

    const recordFailure = (failure) => {
      failedProviders.push(failure);
      if (respectProviderBackoff) {
        store.saveCheckpoint(checkpointKey, nextEnrichmentProviderState(previous, failure));
      }
    };
    try {
      const result = await task();
      providerResults.push({
        provider,
        ...(result && typeof result === "object" ? result : { result })
      });
      if (didEnrichmentComplete(result)) {
        completedProviders.push(provider);
      }
      const failure = enrichmentResultFailure(result, provider);
      if (failure) recordFailure(failure);
      else if (respectProviderBackoff && result?.reason !== "not-applicable") {
        store.saveCheckpoint(checkpointKey, nextEnrichmentProviderState(previous, null));
      }
      refreshCurrent();
    } catch (error) {
      recordFailure(enrichmentFailure(error, provider));
      console.warn(`[sync] provider ${provider} failed for case ${caseId}: ${error?.message || error}`);
    } finally {
      if (interProviderDelayMs > 0) await wait(interProviderDelayMs);
    }
    return true;
  };

  const tasks = {
    [SIGNAL_FEED_PROVIDER_KEY]: () => syncService.enrichCaseWithSignalFeed(caseId, { force: true }),
    recentfilings: () => syncService.enrichCaseWithRecentFilings(caseId, { force: true }),
    "61tro": () => syncService.enrichCaseWithLawFirmLookup(caseId, { sourceIds: ["61tro"], force: true }),
    [PRIORITY_FEED_SOURCE]: () => syncService.enrichCaseWithPriorityFeed(caseId, { force: true }),
    courtlistener: () => syncService.enrichCaseWithCourtListener(caseId, { force: true }),
    pacermonitor: () => syncService.enrichCaseWithPacerMonitor(caseId, { force: true }),
    docketalarm: () => syncService.enrichCaseWithDocketAlarm(caseId, { force: true }),
    unicourt: () => syncService.enrichCaseWithUniCourt(caseId, { force: true })
  };
  // Preserve configured priority; a failing source must not prevent fallback sources.
  for (const provider of expandedProviders) {
    if (stoppedReason) break;
    if (provider.startsWith("lookup:")) {
      await runProvider(provider, async () => {
        const sourceKey = provider.split(":")[1];
        const lookupTerm = String(current?.docket_number || current?.case_name || "").trim();
        if (!lookupTerm) return { enriched: false, reason: "not-applicable" };
        lookupResult = await syncService.importLookup(lookupTerm, {
          courtName: current?.court_name || "", caseName: current?.case_name || "", sources: [sourceKey]
        });
        const source = lookupResult.sourceResults?.[sourceKey];
        if (source?.error) {
          throw Object.assign(new Error(source.error), { code: source.code, status: source.status });
        }
        return { enriched: false, reason: source?.matched ? "metadata-only" : "not-found", lookupResult };
      });
    } else if (tasks[provider]) {
      await runProvider(provider, tasks[provider]);
    } else {
      failedProviders.push(enrichmentFailure({ code: "UNKNOWN_PROVIDER", error: "unknown provider", retryable: false }, provider));
    }
  }

  store.refreshCaseDocketSummary(caseId);
  refreshCurrent();
  const coverage = store.getEntryCoverageForCaseIds?.([caseId])?.get(caseId) || null;

  return {
    caseId,
    coverage,
    requestedProviders: expandedProviders,
    completedProviders,
    failedProviders,
    providerResults,
    stoppedReason,
    lookupResult
  };
};
}
