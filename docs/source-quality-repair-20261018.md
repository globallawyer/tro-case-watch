# Source quality repair, 2026-10-18

## Reproduced defects

- PACERMonitor date groups use `docketHeaderRow` with an explicit year. The old parser skipped them and sometimes used a date inside the legal narrative instead of the filing date. The new parser reads date cells, explicit date attributes and date-group headers. Missing years remain unknown.
- PACERMonitor search accepted an initial hit without verifying court, docket office or plaintiff. Stored BLANCHE BRIDAL case 5267284 contains ten unrelated Abdelwahed habeas rows from public case 66055570. Strict page identity checks now reject wrong hits; state-only placeholders require exact plaintiff matching in the expected state/district.
- When a source had no entry ID, storage treated its shared case-page URL as a unique document URL. This could collapse distinct numbered filings or unnumbered notices. Case-page URLs are now excluded from document URL deduplication.
- RSS internal `de_seq_num` was a fallback for official document numbers. The fallback has been removed; legacy sequence-only rows do not count toward official numbering coverage.
- Machine-prefixed RSS labels, generic notices and title-plus-attachment-name strings could be counted as descriptive text. The classifier now rejects these, and the completion SQL calls the same classifier as display rather than maintaining a divergent SQL approximation.
- Three stored CourtListener entries have year 3926. Dates are now calendar-validated at ingestion and hydration, with rejected raw values retained.
- A failed PACERMonitor attempt could advance the successful docket freshness clock. `checkedAt` now controls cooldown while `syncedAt` only advances when entries are received.

## Safety and verification

All existing tests plus source-date, identity, attachment, deduplication, cooldown and idempotent repair tests pass (99 tests). No dependency or paid-source change is included.

Production was clean at commit 1bae452 before work. The rollback directory is `/var/lib/tro-case-watch/recovery/source-quality-repair-20261018`, including protected environment/code archive, SQLite online backup and tagged old image.

`scripts/repair-source-quality.mjs` is dry-run by default. It is bounded to the audited impossible dates and known wrong-case URL. Applied repairs save complete original case/entry rows in `source_quality_repair_journal` before removing wrong rows from the live timeline. A fresh, trusted-host snapshot can fill dates only for already stored, uniquely matching entries after strict identity checks. No docket text, numbering or missing year is invented.

The accessible Albion public page verifies five filings dated 2026-10-15. Two other historical PACERMonitor pages returned HTTP 429; they were not bypassed or repeatedly retried. Unverified metadata-only identities and source access failures remain unresolved rather than being declared complete.
