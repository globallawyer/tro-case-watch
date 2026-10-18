# Latest 20 Case Audit

The production sample is the 20 physical records ordered by created_at DESC, id DESC,
captured on 2026-10-18. It is not the latest filing dates or search-result ranking.
Before repair, 12 records had zero displayed docket entries and eight had only one
RSS title each. No sampled record had a descriptive docket entry.

## Confirmed Logic Defects

- A 61TRO HTTP-200 not-found page was accepted using the docket embedded in its URL.
- Metadata-only enrichment updated apparent success/freshness without adding entries.
- Source authority outranked actual content quality when choosing a canonical row.
- Filing dates outranked official docket numbers, allowing non-monotonic display.
- Continuity checks treated a complete sequence of titles as sufficient coverage.
- The UI conflated a maximum known number with archived entry/text quantities.
- State-only catalog records did not match a verified district and office identity.
- WorldTRO human-verification responses were recorded as ordinary missing dockets.

Regression tests cover these cases. State-only identity upgrades require a unique
catalog record, matching state/year/serial, a verified office and district, and a
strong plaintiff match. Different offices and states are not guessed or combined.
The audit/import utilities default to read-only operation unless --apply is given.

## Upstream Limits

TroHub currently exposes 15 public recent cases. Nine overlap this sample, but only
one of those details provided docket entries in the observed response (12687, ten
entries). The other eight have genuine case metadata but zero supplied entries.
Empty detail responses are not successful hydration, and cannot prove completeness.
The production host receives a list without case links while the local request
receives 15 cases. This response difference remains an upstream-access limitation.
Public snapshots can be imported after validating age, origin, and target identity;
this is not a paid API or a workaround for a verification challenge.

Never manufacture missing entry numbers, claim the 20 cases are complete, or discard
existing historical rows to make a mirror snapshot look authoritative.
