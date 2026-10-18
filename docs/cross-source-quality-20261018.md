# Cross-source audit and global repair, 2026-10-18

## Verified scope

- The original latest 20 records were selected by created_at, not filing date or search rank.
- Before repair, 12 had no displayed entries and 8 had one RSS title each.
- TroHub public latest-15 snapshot matched 9 distinct cases in that sample. Eicher Motors 1:26-cv-12687 had 10 entries: official numbers 1-8 plus two unnumbered court notices. Its cover sheet is a title, not descriptive text. Eight other matching pages had no entries.
- Current 61TRO search matched those same 9 distinct cases (10 sample records because Monster Energy had two identities). Their public detail pages contained no timeline at the audit time. An independent historical page, 2026-cv-00651 in Illinois Northern District, contained 16 timeline entries. It is not Pennsylvania Western District 2:26-cv-00651.
- DocketBird public access returned HTTP 403. No comparison of its docket counts or authenticated content was possible. This is an access failure, not a zero-entry result.
- A read-only sample of the latest 1000 targeted 2026 database records found 247 with no entries, 621 with title-only entries, 132 with some descriptive text, 727 with some content gaps and 650 with numbering gaps. Categories overlap. Physical records are not deduplicated case counts. Classification is a heuristic, not official-document certification.

## Shared logic changes

- Preserve court and office identity; remove short-number-only cross-court fallback. A mirror omitting the office can match only one same-court case with the same plaintiff leader. Ambiguous offices remain unmatched, and the known office number is preserved.
- Support 61TRO's current search route and tl-item timeline layout alongside the legacy layout. Parse ISO filing dates and the actual court and case name. Site labels are not plaintiff law firms. Source info IDs are not docket numbers.
- Metadata-only pages update metadataCheckedAt, not the docket syncedAt clock.
- Assess descriptive content separately from numeric coverage. Generic labels, bracketed RSS titles and placeholder entry labels remain enrichment candidates even with contiguous numbers.
- Prefer richer text across matching docket entries; preserve complementary entries rather than replacing entire timelines.
- Apply cooldown/lease exclusions before the candidate LIMIT so older cases outside the latest window cannot starve indefinitely.
- Frontend distinguishes actual archived entries, descriptive entries, official numbers and unnumbered notices. No fabricated numbers or blanket completeness claims.

## Deployment and limitations

Production code, protected environment configuration, SQLite online backup and rollback image were saved before deployment. Existing API/webhook credentials were not changed. Free public sources only were used for this audit; no paid document request or access-control bypass was made.

The repair fixes shared behavior. It cannot guarantee missing upstream text becomes available immediately. Background processing must remain bounded and source failures must stay visible rather than being reported as completed dockets.

Public evidence:

- https://61tro.com/index.php/detail/11235.html
- https://61tro.com/index.php/detail/8859.html
- https://www.trohub.com/cases/99bd7e48262241c69aa2802dfaacc710-eicher-motors-limited-v-the-individuals-corporations-limited-liability-companies-partnerships-and-unincorporated-associations-identified-on-schedule-a-hereto
- https://www.docketbird.com/
