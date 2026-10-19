# Latest-First Docket Quality

The latest-cohort exporter orders by filing date before ingestion time. Public-source comparison is sequential and records content counts and filing dates, not just HTTP success or raw row totals. Verification pages, missing public URLs and inaccessible sources are explicitly not comparable; an unknown result is never recorded as a competitor having zero entries.

## Corrections

- Resolve a short docket's existing local counterpart only when court, normalized plaintiff and filing date match exactly and no competing office matches. Both old links and background coverage use the same indexed peer resolver. Source rows remain unchanged.
- Preserve primary archive wording when it has an explanation. If it only has a title, keep that title as the heading and supplement the explanation from a reliably matched auxiliary row, recording body provenance internally.
- A single empty case search returns `EMPTY_CASE_SEARCH` and retries that case without opening a provider-wide circuit. Whole-catalog failures retain `EMPTY_DISCOVERY`; network failures, verification and rate limiting retain backoff.
- A primary catalog verification page or empty unverified listing cannot advance successful discovery. Primary HTTP requests enforce the deadline through body reading and cap decompressed HTML at 4 MiB; a challenged lookup does not probe fallback URLs.
- Standalone job outcomes distinguish failed and partial source checks from successful completion, including hydration failures. A loop finishing is not proof of content acquisition.
- Newly filed, unexamined cases from the last seven days enter the completion pool first. Within that recent group, newer filing dates precede older high-gap cases. Existing scan fairness, retry cooldowns and memory limits remain intact.

## Evidence and Scope

The first 20 recent case records included short/full duplicates, empty timelines and title-only court notices. A normal public TroHub lookup for Illinois `1:26-cv-12706` supplied seven docket records, six with explanations; both local links had no entries before repair. Other matched public examples included `12712` (16 entries) and `12687` (10 entries). Many new public 61tro detail pages exposed metadata only, not a docket history. Worldtro access failed in this comparison; DocketBird had no verified accessible public case URL. Neither can be assigned a completeness score from this run.

Import only fresh, court/docket-validated matched snapshots with actual explanatory content. Keep metadata-only results and access failures in the comparison report for later review, rather than manufacturing filing text or claiming universal completeness. Public docket descriptions are not the same as full PDF contents; sealed or paid documents cannot be declared retrieved without access.
