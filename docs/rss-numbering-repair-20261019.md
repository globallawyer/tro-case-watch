# RSS Numbering and Cross-Source Timeline Repair

## Confirmed Defects

- A mirror's table position was mistaken for an official document number on old RSS-derived rows. In Albion (5307760), table row 5 explicitly referred to document 3, and table row 4 referred to document 1.
- Chinese RSS title wrappers counted as descriptive text even though they only repeated filing labels.
- Cross-source copies with a missing or conflicting filing date failed to merge even when they shared the same actual court document number. Coverage and timeline used different keys.
- Ingestion timestamps could incorrectly allow semantic deduplication of undated records.

## Changes and Safety

- One shared helper identifies only the explicit legacy RSS formats. Published document references are retained; synthetic RSS IDs and mirror row positions are not official docket numbers.
- New ingestion retains original row evidence in raw metadata. Legacy records are corrected at read time, without deleting or rewriting the archived rows.
- Mirror RSS event identity uses the notification marker or explicit court document reference, not the unstable table position. Legacy IDs are reconciled within the indexed case/source scope, so newly inserted mirror rows cannot overwrite unrelated notices.
- All copies of one real docket number within the same case share a display item, preferring descriptive content and retaining source provenance. Different real numbers remain distinct.
- Undated unnumbered records are not speculatively combined with other events. Filing dates are not inferred from ingestion dates or RSS publication timestamps.
- SQL coverage uses the same sequence and content functions as the timeline, in bounded case-ID batches. Materialized intermediate rows omit raw JSON and unnecessary columns.
- Public API number fields all use the resolved court sequence, preventing older clients from falling back to the bad stored number.

## Verification

Run `npm test`, then stream `scripts/audit-rss-quality.mjs` into the app container for a read-only production census and actual API checks. The audit does not trigger crawling, use paid providers, or modify the database.

This repairs identification and representation, not unavailable source content. Unnumbered RSS notices remain visibly title-only; a continuous sequence is not proof that all text or PDFs have been obtained.
