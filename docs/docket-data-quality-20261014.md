# Docket data repair

## Confirmed defects

- CourtListener pagination stopped after 100 entries even when `next` existed.
- Search ingestion preferred short document titles over longer docket descriptions.
- Semantic display deduplication merged different explicit docket numbers.
- Dates and timestamps for the same filing day used different deduplication buckets.
- Mirror/API refreshes deleted earlier source rows before accepting a smaller response.
- Rendered signal entry keys could collide across cases sharing a title and date.
- Known signal cases were searched only in recent discovery instead of their saved detail URL.
- Rendered attachment extraction could include the preceding entry's attachments.
- Two commercial adapters invented docket numbers from array positions.

The repair retains historical source observations, scopes signal keys to a case,
preserves unknown numbers, rejects conflicting case identities, and retrieves all
CourtListener pages subject to explicit page, entry, and runtime budgets. Exceeding
a pagination budget raises an incomplete-fetch error; it does not report success.

## Data sources

Use structured docket-entry records with descriptions, filing dates, and published
entry numbers. CourtListener search hits and document titles are discovery hints,
not complete docket snapshots. RECAP coverage can still be incomplete even after
all API pages are fetched.

- CourtListener data model and entry pagination:
  https://wiki.free.law/c/courtlistener/help/api/rest/v4/pacer-data
- Docket Alarm structured API and cached versus live court refresh:
  https://www.docketalarm.com/api/v1/
- PACER automation and official national case index:
  https://pacer.uscourts.gov/file-case/developer-resources

Commercial sources require working credentials and suitable access permissions.
This change does not enable paid fetching or claim that unknown filings have been
recovered. Website display does not guarantee automatic gap repair.

## Rollout and verification

Preserve remote changes and take a code/configuration backup plus an online SQLite
backup before production data repair. Deploy app and worker from the same commit.
Run a bounded enrichment for known CourtListener cases, then compare the stored
entry numbers and descriptions against the upstream snapshot. Check older source
rows after a partial refresh to confirm they survive. Revisit historical signal
cases to recover rows affected by old cross-case key collisions.

On 2026-10-14, 72 local regression tests pass. The production checkout was updated
from 62489c3 to 3e96612 through the hosting console, and app and worker were rebuilt
and observed healthy. The public app.js SHA-256 matched the release exactly.
The production code/config, online 840 MB SQLite backup, and previous image are
retained under recovery/docket-quality-20261014-0235 and the rollback image tag.

Bounded, free-source re-ingestion exposed remaining upstream limitations:
CourtListener returned HTTP 429, and public signal discovery returned no case rows.
Case 2523428 had 109 archived priority entries plus complementary sources; case
5160485 had 69 title-only court-feed rows. The maximum known entry number is not
the number of complete stored entries. Neither case is verified complete.
No paid PACER fetching was enabled. Provider no-op results are now reported
separately from completed enrichment instead of treating a skipped lookup as success.
