import test from "node:test";
import assert from "node:assert/strict";
import { getDocketContentStatus } from "../src/docket-content.js";
import { CatalogClient } from "../src/providers/catalog.js";
import { LawFirmClient } from "../src/providers/lawfirm.js";

test("content quality distinguishes titles from substantive short docket text", () => {
  assert.equal(getDocketContentStatus({}), "missing");
  assert.equal(getDocketContentStatus({ description: "[Temporary Restraining Order]" }), "title-only");
  assert.equal(getDocketContentStatus({ description: "Report on Filing of Patent/Trademark Action", document_type: "Report on Filing of Patent/Trademark Action" }), "title-only");
  assert.equal(getDocketContentStatus({ description: "ORDER: Defendant must respond by October 20." }), "descriptive");
  assert.equal(getDocketContentStatus({ description: "CIVIL Cover Sheet", document_type: "Docket Entry" }), "title-only");
  assert.equal(getDocketContentStatus({ description: "Docket entry 125" }), "title-only");
  assert.equal(getDocketContentStatus({ description: "ATTORNEY Appearance for Plaintiff Eicher Motors Limited by Michael A. Hierl" }), "descriptive");
});

test("priority source verification pages are failures rather than empty dockets", () => {
  const client = new CatalogClient({ enabled: true });
  assert.throws(() => client.parseCasePage('<title>安全验证</title><input name="slider_token">', "https://example.test", {}), { code: "SOURCE_CHALLENGE" });
});

test("61tro soft-404 cannot create an apparent case from its URL", async () => {
  const client = new LawFirmClient({ enabled: true, sources: ["61tro"], minIntervalMs: 0 });
  client.fetchText = async () => "<title>页面未找到 - 61TRO案件查询网</title>";
  const source = client.listSources().find((item) => item.id === "61tro");
  assert.equal(await client.lookup61troByDocket(source, "26-cv-06149", { courtName: "Central District of California" }), null);
});
