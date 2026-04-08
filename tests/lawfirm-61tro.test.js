import test from "node:test";
import assert from "node:assert/strict";

import { LawFirmClient } from "../src/providers/lawfirm.js";

test("61tro docket lookup falls back to tag pages and keeps richer timeline text", async () => {
  const client = new LawFirmClient({
    enabled: true,
    sources: ["61tro"],
    timeoutMs: 1000,
    minIntervalMs: 0
  });
  const source = client.listSources().find((item) => item.id === "61tro");

  const sitemapTxt = [
    "https://61tro.com",
    "https://61tro.com/tag/XYZ%20Corporation.html"
  ].join("\n");

  const tagPageOne = `
    <html>
      <body>
        <a href="/tag/XYZ%20Corporation.html?page=2">2</a>
      </body>
    </html>
  `;

  const tagPageTwo = `
    <html>
      <body>
        <h4><a href="/detail/8739.html">XYZ Corporation v. The Partnerships Identified On Schedule A</a></h4>
      </body>
    </html>
  `;

  const detailPage = `
    <html>
      <head>
        <title>2026-cv-00011 - 案件详情 - 61TRO案件查询网</title>
      </head>
      <body>
        最近更新：2026-04-04
        <div class="post__title"><h2>2026-cv-00011</h2></div>
        <div class="post__options"><h4>XYZ Corporation v. The Partnerships Identified On Schedule A</h4></div>
        <span>法院：伊利诺伊州北区法院</span>
        <span>品牌：XYZ Corporation</span>
        <span>律所：Feldenkrais Law</span>
        <div class="layui-timeline-item">
          <div>
            <h3 class="layui-timeline-title">04/03/2026</h3>
            <p class="v-text">MINUTE entry before the Honorable Mary M. Rowland... 翻译</p>
            <p>附件：</p>
            <p>1:(Text of Proposed Order)</p>
          </div>
        </div>
        <div class="layui-timeline-item">
          <div>
            <h3 class="layui-timeline-title">04/02/2026</h3>
            <p class="v-text">ATTORNEY Appearance for Defendants asdgh by ZHAOXIN ZHANG 翻译</p>
          </div>
        </div>
      </body>
    </html>
  `;

  client.fetchText = async (url) => {
    if (url.includes("/search.html?sn=")) {
      throw new Error("LawFirm fetch failed (500)");
    }

    if (url === "https://61tro.com/sitemap.txt") {
      return sitemapTxt;
    }

    if (url === "https://61tro.com/tag/XYZ%20Corporation.html") {
      return tagPageOne;
    }

    if (url === "https://61tro.com/tag/XYZ%20Corporation.html?page=2") {
      return tagPageTwo;
    }

    if (url === "https://61tro.com/detail/8739.html") {
      return detailPage;
    }

    throw new Error(`Unexpected URL: ${url}`);
  };

  const item = await client.lookup61troByDocket(source, "26-cv-00011", {
    courtName: "District Court, N.D. Illinois",
    caseName: "XYZ Corporation v. The Partnerships Identified On Schedule A"
  });

  assert.ok(item, "expected 61tro lookup to resolve through tag fallback");
  assert.equal(item.docketNumber, "26-cv-00011");
  assert.equal(item.courtId, "ilnd");
  assert.equal(item.courtName, "Northern District of Illinois");
  assert.equal(item.entries.length, 2);
  assert.match(item.entries[0].description, /MINUTE entry/i);
  assert.match(item.entries[0].description, /附件：/);
  assert.match(item.entries[0].description, /Text of Proposed Order/);
  assert.equal(item.entries[0].filedAt, "2026-04-03");
  assert.equal(item.entries[1].filedAt, "2026-04-02");
});

test("61tro docket lookup inspects search result detail pages when titles omit the docket", async () => {
  const client = new LawFirmClient({
    enabled: true,
    sources: ["61tro"],
    timeoutMs: 1000,
    minIntervalMs: 0
  });
  const source = client.listSources().find((item) => item.id === "61tro");

  const searchPage = `
    <html>
      <body>
        <h4><a href="/detail/8739.html">Bose Corporation v. The Partnerships Identified on Schedule A</a></h4>
      </body>
    </html>
  `;

  const detailPage = `
    <html>
      <head>
        <title>2026-cv-00011 - 案件详情 - 61TRO案件查询网</title>
      </head>
      <body>
        <div class="post__title"><h2>2026-cv-00011</h2></div>
        <div class="post__options"><h4>Bose Corporation v. The Partnerships Identified on Schedule A</h4></div>
        <span>法院：伊利诺伊州北区法院</span>
        <div class="layui-timeline-item">
          <div>
            <h3 class="layui-timeline-title">04/03/2026</h3>
            <p class="v-text">Order on Motion for Extension of Time 翻译</p>
          </div>
        </div>
      </body>
    </html>
  `;

  client.fetchText = async (url) => {
    if (url === "https://61tro.com/search.html?sn=2026-cv-00011") {
      return searchPage;
    }
    if (url === "https://61tro.com/detail/8739.html") {
      return detailPage;
    }
    if (url === "https://61tro.com/search.html?sn=26-cv-00011") {
      return "";
    }
    if (url === "https://61tro.com/sitemap.txt") {
      return "";
    }
    throw new Error(`Unexpected URL: ${url}`);
  };

  const item = await client.lookup61troByDocket(source, "26-cv-00011", {
    courtName: "District Court, N.D. Illinois",
    caseName: "Bose Corporation v. The Partnerships Identified on Schedule A"
  });

  assert.ok(item, "expected search result detail inspection to find the docket");
  assert.equal(item.docketNumber, "26-cv-00011");
  assert.equal(item.entries.length, 1);
  assert.match(item.entries[0].description, /Order on Motion for Extension of Time/i);
});
