import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { Store } from "../src/db.js";
import { CaseSyncService } from "../src/sync.js";
import { SignalFeedClient } from "../src/providers/signal-feed.js";
import { verifiesMissingOffice } from "../src/verified-case-identity.js";

const reference = { docket_number: "26-cv-12712", court_id: "ilnd", date_filed: "2026-10-16",
  case_name: "Spin Master Ltd. v. The Partnerships Identified on Schedule A" };
const verified = { docketNumber: "1:26-cv-12712", courtId: "ilnd", dateFiled: "2026-10-16",
  plaintiff: "Spin Master Ltd. et al.", caseName: "Spin Master Ltd. et al v. The Partnerships Identified on Schedule A",
  docketId: "verified", detailUrl: "https://www.trohub.com/en/cases/verified", entries: [
    { documentNumber: 1, title: "COMPLAINT filed by Spin Master Ltd. alleging trademark infringement.", filedAt: "2026-10-16" }
  ] };

function withStore(run) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "tro-verified-identity-"));
  const store = new Store(path.join(dir, "test.sqlite"));
  try { return run(store); } finally { store.db.close(); fs.rmSync(dir, { recursive: true, force: true }); }
}
function record(key, overrides = {}) {
  return { ...reference, source_case_key: key, source_case_id: key, primary_source: "61tro",
    court_name: "Northern District of Illinois", tags_marker: "|seller_tro|schedule_a|", ...overrides };
}

test("missing-office identity requires exact court, plaintiff and filing date", () => {
  assert.equal(verifiesMissingOffice(reference, verified), true);
  for (const changes of [{ courtId: "nysd" }, { courtId: "il" }, { plaintiff: "Different Brand LLC" },
    { dateFiled: "2026-10-15" }, { dateFiled: "" }, { docketNumber: "1:26-cv-12719" }]) {
    assert.equal(verifiesMissingOffice(reference, { ...verified, ...changes }), false);
  }
  assert.equal(verifiesMissingOffice({ ...reference, docket_number: "2:26-cv-12712" }, verified), false);
});

test("verified source upgrades a 61tro short record in place so its old link gains the real docket", () => withStore(store => {
  const short = store.upsertCase(record("short"));
  const full = store.upsertCase(record("full", { docket_number: verified.docketNumber, primary_source: "courtfeed" }));
  assert.equal(store.findCatalogPlaceholderForCase(verified).id, short.id);
  const service = new CaseSyncService({ store, config: { sync: {} } });
  service.ingestSignalFeedItems([verified]);
  assert.equal(store.getCase(short.id).docket_number, verified.docketNumber);
  assert.equal(store.getCase(short.id).entries.length, 1);
  assert.equal(store.getCase(full.id).entries.length, 1);
  assert.equal(store.db.prepare("SELECT COUNT(*) AS n FROM cases").get().n, 2);
}));

test("two confirmed offices with the same plaintiff and date remain ambiguous", () => withStore(store => {
  store.upsertCase(record("short"));
  store.upsertCase(record("other-office", { docket_number: "2:26-cv-12712" }));
  assert.equal(store.findCatalogPlaceholderForCase(verified), null);
}));

test("public lookup accepts a missing office only with complete identity evidence", async () => {
  const client = new SignalFeedClient({ enabled: true, publicCasesUrl: "https://example.test/en/cases/" });
  client.requestText = async () => '<tr><td><a href="/en/cases/verified">1:2026-cv-12712</a></td></tr>';
  client.fetchPublicDetail = async () => verified;
  assert.equal(await client.lookupByDocket(reference.docket_number, { courtId: "ilnd" }), null);
  assert.equal((await client.lookupByDocket(reference.docket_number, {
    courtId: "ilnd", caseName: reference.case_name, dateFiled: reference.date_filed
  })).docketNumber, verified.docketNumber);
});

test("a public search returning two possible offices never guesses the target", async () => {
  const client = new SignalFeedClient({ enabled: true, publicCasesUrl: "https://example.test/en/cases/" });
  client.requestText = async () => '<tr><td><a href="/en/cases/one">1:2026-cv-12712</a></td></tr><tr><td><a href="/en/cases/two">2:2026-cv-12712</a></td></tr>';
  client.fetchPublicDetail = async url => ({ ...verified, docketNumber: url.endsWith("two") ? "2:26-cv-12712" : verified.docketNumber });
  assert.equal(await client.lookupByDocket(reference.docket_number, {
    courtId: "ilnd", caseName: reference.case_name, dateFiled: reference.date_filed
  }), null);
});
