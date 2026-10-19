# Durable Docket Completion

## Root Causes

- A completed HTTP attempt was treated as successful docket enrichment, even for empty objects and failed lookups. Some lookup errors were swallowed as `not-found`.
- Only a short list of HTTP statuses triggered queue retries. Fetch errors, connection resets, timeouts, empty discovery pages, and verification pages could finish a job without content or retry.
- Automatic requests ignored configured source ordering and had no shared source-outage backoff. Many cases could repeatedly hit the same unavailable source.
- A worker task finishing is not evidence of docket completeness. Title-only labels and numbered continuity require separate coverage checks.
- Historical discovery was limited to the latest source list and mirror display rows were mistaken for official numbers. The preceding historical-completion release fixes these separately.

## Changes

- Preserve configured provider priority and continue fallback providers after errors.
- Separate docket updates, metadata-only responses, not-applicable sources, definitive no-match results, transport failures, and cooldown deferrals.
- Persist per-source circuit state across worker processes. Retry network/source failures with exponential delay; honor source Retry-After. A single case's empty result or 404 does not block every case.
- Retry only unfinished, retryable providers. Limit actual failed rounds to six by default (`WEBHOOK_ENRICHMENT_MAX_ATTEMPTS`). Memory pauses and circuit deferrals do not consume that budget.
- Persist retry diagnostics and a bounded last-500 unresolved-outcome journal with a cumulative total. Failed/no-content runs are no longer labeled successful. Daily completion scans remain responsible for later re-evaluation; unresolved records are diagnostic, not proof of complete source coverage.
- Keep the existing isolated worker, process/runtime limits, queue leases, and resource guard. Limit public priority-source HTML to 4 MiB decompressed bytes.
- Include canonical content and sequence coverage in enrichment statistics. `scripts/audit-enrichment-reliability.mjs` provides a read-only recent-20-case and queue audit without credentials or raw case secrets.

## Limits

These fixes prevent silent loss and repeated resource-heavy attempts. They do not grant access to blocked sources, produce missing official filings, or prove there are no newer filings beyond the highest observed docket number. A source returning empty pages on the server while working from another network remains an external availability problem and is recorded as such. No access controls or paid sources are bypassed.

## Pre-Deployment Audit

At 2026-10-19 08:52 UTC the latest 20 seller-watch cases had 13 empty timelines; the other seven contained title-only items, and six had numbered continuity gaps. Several recent worker runs had no completed provider and no retry but were marked completed. This confirms a systemic problem, not a single-case issue. The repaired Illinois 1:26-cv-04482 remained at 60 canonical events, 59 descriptive, official observed numbers 1 through 56 with no internal numbered gap. These are observed database counts, not an independently verified current official docket total.
