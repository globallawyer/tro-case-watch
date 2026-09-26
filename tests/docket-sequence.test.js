import assert from "node:assert/strict";
import test from "node:test";

import {
  analyzeDocketSequence,
  getDocketSequenceValue,
  normalizeDocketSequenceValue
} from "../src/docket-sequence.js";

test("docket sequence prefers entry number over attachment document number", () => {
  const entry = {
    entry_number: "248.0",
    document_number: "6.0"
  };

  assert.equal(getDocketSequenceValue(entry), "248");
  assert.equal(normalizeDocketSequenceValue("0007.0"), "7");
});

test("docket sequence reports exact missing ranges without numbering unnumbered events", () => {
  const analysis = analyzeDocketSequence([
    { entry_number: "1" },
    { entry_number: "2" },
    { entry_number: "5" },
    { document_number: "7" },
    { description: "Official event without a published docket number" }
  ], { upperBound: 9 });

  assert.equal(analysis.numberedEntryCount, 4);
  assert.equal(analysis.unnumberedEntryCount, 1);
  assert.equal(analysis.missingCount, 5);
  assert.deepEqual(analysis.missingRanges, [
    { start: 3, end: 4, count: 2 },
    { start: 6, end: 6, count: 1 },
    { start: 8, end: 9, count: 2 }
  ]);
  assert.equal(analysis.isComplete, false);
});
