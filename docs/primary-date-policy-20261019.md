# Primary Archive and Chronological Presentation

## Contract

- Genuine Worldtro docket records take precedence over corresponding records from every auxiliary source. Legacy RSS notifications labelled as Worldtro are not genuine archive content.
- A later Worldtro correction wins even when its text is shorter or its filing date is earlier. An auxiliary refresh cannot displace an archived primary record.
- An empty primary field can be supplemented. Auxiliary-only filings remain visible; source rows and existing case links are not bulk deleted.
- Match verified document identities first. A mirror table position is not an official document number. Unnumbered records require an unambiguous content/date match; uncertain candidates remain separate rather than losing filings.
- Merge the complete case history before limiting a preview. Sort the result by filing date descending, with unknown dates last. Ingestion time is not a filing date.
- Hide docket numbers for an unreliable sequence, including unverified mirror positions, gaps or conflicting filing-date order. Keep internal numbers for reconciliation, not for fabricated display numbering.
- Apply the same policy to every year, including every 2026 case, on both case-detail links and background coverage checks. Public responses expose only generic presentation flags, not provider identity or raw source metadata.

## Operations

Existing and newly queued enrichment tasks try Worldtro first. Existing resource limits, source backoff and isolated worker execution remain enabled. A blocked source does not stop auxiliary supplementation and does not mean its archive has been fully fetched.

Run the full 2026 inventory check on a consistent SQLite backup, in an isolated container with a 256 MB Node heap and bounded memory:

```sh
node --max-old-space-size=256 scripts/audit-2026-priority-policy.mjs /backup/db.sqlite
```

The read-only audit uses indexed case-peer lookups, streams one logical case at a time, and reports date ordering, primary precedence and raw-contributor retention violations. It includes cases filed in 2026 and civil docket identities containing the 2026 year, including records with missing filing dates. It is an inventory/presentation audit, not proof that every live upstream page is currently accessible or complete.
