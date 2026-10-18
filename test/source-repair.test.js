import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";
import { Store } from "../src/db.js";

test("audited source repair archives wrong-case entries and is idempotent", () => {
  const root = fileURLToPath(new URL("..", import.meta.url));
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "tro-source-repair-"));
  const dbPath = path.join(directory, "test.sqlite");
  const store = new Store(dbPath);
  try {
    const saved = store.upsertCase({ source_case_key: "repair:wrong", source_case_id: "repair:wrong", primary_source: "worldtro", court_id: "pa", court_name: "Pennsylvania", docket_number: "26-cv-02183", case_name: "BLANCHE BRIDAL PTY. LTD.", date_filed: "2026-10-07", tags_marker: "|tro|schedule_a|", source_urls: ["https://www.pacermonitor.com/public/case/66055570/Abdelwahed_et_al_v_Lowe_et_al"], raw: { pacermonitor: { rowCount: 2, caseUrl: "https://www.pacermonitor.com/public/case/66055570/Abdelwahed_et_al_v_Lowe_et_al" } } });
    store.db.prepare("UPDATE cases SET id=5267284 WHERE id=?").run(saved.id);
    for (let n = 1; n <= 2; n++) store.upsertDocketEntry({ case_id: 5267284, source_entry_key: `foreign:${n}`, primary_source: "pacermonitor", entry_number: String(n), description: "PETITION for Writ of Habeas Corpus by an unrelated person", absolute_url: "https://www.pacermonitor.com/public/case/66055570/Abdelwahed_et_al_v_Lowe_et_al", filed_at: "2026-08-03", raw: {} });
    store.db.prepare("UPDATE docket_entries SET filed_at='3926-09-04' WHERE source_entry_key='foreign:1'").run();
    store.db.close();
    const run = apply => {
      const child = spawnSync(process.execPath, ["scripts/repair-source-quality.mjs", ...(apply ? ["--apply"] : [])], { cwd: root, env: { ...process.env, DB_PATH: dbPath }, encoding: "utf8" });
      assert.equal(child.status, 0, child.stderr);
      return JSON.parse(child.stdout);
    };
    assert.equal(run(false).quarantinedEntries.length, 2);
    const first = run(true);
    assert.equal(first.quarantinedEntries.length, 2);
    assert.equal(first.invalidDates.length, 1);
    assert.equal(run(true).quarantinedEntries.length, 0);
    const db = new DatabaseSync(dbPath, { readOnly: true });
    try {
      assert.equal(db.prepare("SELECT COUNT(*) AS n FROM docket_entries").get().n, 0);
      const archived = db.prepare("SELECT snapshot_json FROM source_quality_repair_journal WHERE repair_key='foreign-pm:2'").get();
      assert.match(archived.snapshot_json, /Habeas Corpus/);
      const repairedCase = db.prepare("SELECT docket_count,raw_json FROM cases WHERE id=5267284").get();
      assert.equal(repairedCase.docket_count, 0);
      assert.equal(JSON.parse(repairedCase.raw_json).pacermonitor.state, "identity-mismatch");
    } finally { db.close(); }
  } finally { try { store.db.close(); } catch {} fs.rmSync(directory, { recursive: true, force: true }); }
});
