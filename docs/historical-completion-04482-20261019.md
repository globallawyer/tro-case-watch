# Historical Docket Completion: Illinois 1:26-cv-04482

## Reproduced Failures

- The case (2585078, Yiwu Baimei) had not had a detailed refresh since September 10. Recent numbers 40, 41, 44, 46, 51, 52 and 56 were empty rows or RSS titles.
- The public comparison on October 19 found 39 WorldTRO rows, 29 61TRO rows and 63 TroHub rows. TroHub included actual numbered entries 1–56 and duplicated unnumbered notices.
- TroHub lookup incorrectly searched only recent discovery. Its public GET form supports `q=2026-cv-04482`, which finds historical cases.
- WorldTRO omitted an item at real document 25, then compressed its own display numbering: source row 34 was actual document 35. A row position is not a court docket number.
- The completion selector repeatedly inspected only the newest candidate window. Excluding queued cases did not help older cases when completed or failed recent cases reoccupied that window.
- Compound CourtListener short-description labels could be misclassified as substantive content, and an empty mirror response advanced successful freshness.

## General Repairs

- Historical public search uses the full-year docket query; detail identity is verified against the docket and court. Foreign-origin links and ambiguous matches are rejected. Successful detail URLs are retained for subsequent direct refresh.
- A persistent indexed case scan ledger rotates candidate evaluation. Completed candidates are recorded as inspected too, so they cannot permanently hide older incomplete cases. Work remains bounded to the configured queue size and isolated worker.
- Mirror table positions are unverified unless independently corroborated. Unique full-text matches resolve real numbers; short generic labels additionally require the exact date, matching position and a unique same-day label. Original rows remain archived.
- Coverage statistics stream one case at a time and use the exact same canonical timeline as detail display. Cross-source duplicate rows no longer make statistics diverge from the UI. Identical long same-day unnumbered notices collapse without deleting raw rows.
- Short-description provenance is considered when choosing a richer description. Empty responses preserve the last successful count and timestamp and record a separate check time.
- The UI distinguishes numbered continuity from unnumbered notices and calls docket narration “detailed description”, not PDF full text.

## Operational Limits

Production requests to the same TroHub public URLs returned an empty list or 404 while local public requests returned populated pages. The repair does not bypass verification, rotate proxies or use paid APIs. This upstream access difference remains a real constraint on automatic TroHub updates from the VM and must not be described as working.

`scripts/import-verified-history.mjs` accepts at most four fresh, bounded HTML snapshots from the two permitted origins. It verifies docket/court identity before writing and journals the full original case and entries in a transaction. It defaults to dry-run. This permits a verified immediate repair without pretending that a failed production fetch succeeded.

An isolated replay of the real archived case plus current public snapshots produces 60 canonical items: 56 numbered filings, four unnumbered events, 59 with detailed descriptions and one cover-sheet label. Numbered entries 1–56 have no gap. This is the verified public range, not a claim that the official docket has no newer filings or that PDFs have been downloaded.
