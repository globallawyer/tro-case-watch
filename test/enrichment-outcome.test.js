import test from "node:test";
import assert from "node:assert/strict";
import { didEnrichmentComplete } from "../src/enrichment-outcome.js";

test("empty or skipped provider results never report successful enrichment", () => {
  for (const result of [undefined, null, { enriched: false, reason: "not-found" }, { skipped: true }]) {
    assert.equal(didEnrichmentComplete(result), false);
  }
});

test("successful provider results are retained even without newly inserted rows", () => {
  assert.equal(didEnrichmentComplete({ enriched: true, entries: 125 }), true);
  assert.equal(didEnrichmentComplete({ entries: 125 }), true);
});
