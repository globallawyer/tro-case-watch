import assert from "node:assert/strict";
import test from "node:test";
import { parseDocketEntries as parseAlarm } from "../src/providers/docketalarm.js";
import { parseDocketEntries as parseUniCourt } from "../src/providers/unicourt.js";

test("commercial providers preserve unknown docket numbers instead of inventing array positions", () => {
  for (const parse of [parseAlarm, parseUniCourt]) {
    const rows = parse({ docket_report: [{ description: "ORDER", date_filed: "2026-10-01" }],
      docketEntries: [{ description: "ORDER", date_filed: "2026-10-01" }] }, "https://example.test");
    assert.equal(rows.length, 1);
    assert.equal(rows[0].row_number, null);
  }
});

test("commercial providers preserve actual published numbers", () => {
  assert.equal(parseAlarm({ docket_report: [{ number: "125", contents: "ORDER" }] }, "https://example.test")[0].row_number, "125");
  assert.equal(parseUniCourt({ docketEntries: [{ entryNumber: "125", description: "ORDER" }] }, "https://example.test")[0].row_number, "125");
});
