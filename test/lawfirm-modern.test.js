import test from "node:test";
import assert from "node:assert/strict";
import { LawFirmClient, parse61troCasePage } from "../src/providers/lawfirm.js";

const client = new LawFirmClient({ enabled: true, sources: ["61tro"], minIntervalMs: 0 });
const source = client.listSources().find(s => s.id === "61tro");
const page = `<title>2026-cv-12687 - case detail</title>
<section class="case-hero"><h1>Eicher Motors Limited v. Schedule A Defendants</h1></section>
<div class="di-label">所属法院</div><div class="di-value">伊利诺伊州北区法院</div>
<div class="di-label">立案日期</div><div class="di-value">2026-10-16</div>
<div class="di-label">原告律所</div><div class="di-value">Example Law</div>
<section><div class="timeline"><div class="tl-item" data-info-id="8441"><div class="tl-body">
<div class="tl-title">ORDER: Defendants must respond by October 20.</div><div class="tl-date">2026-10-17</div>
<button>翻译</button><div class="tl-cn">译文</div></div></div>
<div class="tl-item" data-info-id="8442"><div class="tl-body"><div class="tl-title">Complaint</div><div class="tl-date">2026-10-16</div></div></div></div></section>`;

test("61tro current layout retains court, filing date and text without invented docket numbers", () => {
  const item = parse61troCasePage(page, "https://61tro.com/index.php/detail/11235.html", source);
  assert.equal(item.courtId, "ilnd");
  assert.equal(item.dateFiled, "2026-10-16");
  assert.equal(item.lawFirm, "Example Law");
  assert.equal(item.entries.length, 2);
  assert.equal(item.entries[0].sourceEntryId, "8441");
  assert.equal(item.entries[0].entryNumber, null);
  assert.equal(item.entries[0].filedAt, "2026-10-17");
  assert.equal(item.entries[0].description, "ORDER: Defendants must respond by October 20.");
});

test("61tro current search resolves a case before trying obsolete routes", async () => {
  const calls = [];
  const local = new LawFirmClient({ enabled: true, sources: ["61tro"], minIntervalMs: 0 });
  local.fetchText = async url => {
    calls.push(url);
    if (url.includes("/index.php/tro.html?q=")) return '<a href="/index.php/detail/11235.html">2026-cv-12687</a>';
    if (url === "https://61tro.com/index.php/detail/11235.html") return page;
    throw new Error("Unexpected legacy fetch");
  };
  const item = await local.lookup61troByDocket(source, "1:26-cv-12687", { courtName: "Northern District of Illinois" });
  assert.equal(item.entries.length, 2);
  assert.equal(calls.length, 2);
});
