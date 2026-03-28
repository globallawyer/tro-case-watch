import fs from "node:fs";
import path from "node:path";
import nodemailer from "nodemailer";
import { loadTroDailyRoundupSources } from "./tro-daily-roundup-sources.js";

const DEFAULT_USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36";

const TRO_KEYWORDS = [
  "tro",
  "schedule a",
  "temporary restraining order",
  "临时限制令",
  "冻结",
  "冻结令",
  "应诉",
  "和解",
  "诉讼",
  "案件",
  "维权",
  "起诉",
  "被告",
  "侵权"
];

const ARTICLE_HINT_PATTERNS = [
  /\/t\/\d+/i,
  /\/help\/detail\/\d+/i,
  /\/article\//i,
  /\/news\//i,
  /\/detail\//i,
  /\/tro\//i,
  /\/case\//i
];

function pad(value) {
  return String(value).padStart(2, "0");
}

function normalizeText(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function normalizeUrl(value, baseUrl = "") {
  const raw = String(value || "").trim();
  if (!raw || raw.startsWith("javascript:") || raw.startsWith("mailto:") || raw.startsWith("tel:")) {
    return null;
  }

  try {
    const url = new URL(raw, baseUrl || undefined);
    if (!["http:", "https:"].includes(url.protocol)) {
      return null;
    }
    url.hash = "";
    return url.toString();
  } catch {
    return null;
  }
}

function decodeHtml(value) {
  return String(value || "")
    .replace(/&#(\d+);/g, (_, code) => String.fromCharCode(Number(code)))
    .replace(/&#x([0-9a-f]+);/gi, (_, code) => String.fromCharCode(Number.parseInt(code, 16)))
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'");
}

function stripTags(value) {
  return decodeHtml(String(value || "").replace(/<script\b[\s\S]*?<\/script>/gi, " ").replace(/<style\b[\s\S]*?<\/style>/gi, " ").replace(/<[^>]+>/g, " "));
}

function cleanText(value) {
  return normalizeText(stripTags(value));
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatDateKey(date = new Date(), timeZone = "Asia/Shanghai") {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  });
  return formatter.format(date);
}

function formatDateTime(date = new Date(), timeZone = "Asia/Shanghai") {
  const formatter = new Intl.DateTimeFormat("sv-SE", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  });
  return formatter.format(date);
}

function getClockParts(date = new Date(), timeZone = "Asia/Shanghai") {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    hour12: false,
    hour: "2-digit",
    minute: "2-digit"
  });
  const parts = Object.fromEntries(formatter.formatToParts(date).map((part) => [part.type, part.value]));
  return {
    hour: Number(parts.hour || 0),
    minute: Number(parts.minute || 0)
  };
}

function getOffsetMinutes(date = new Date(), timeZone = "Asia/Shanghai") {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    timeZoneName: "shortOffset"
  });
  const zone = formatter.formatToParts(date).find((part) => part.type === "timeZoneName")?.value || "GMT+0";
  const match = zone.match(/GMT([+-])(\d{1,2})(?::?(\d{2}))?/i);
  if (!match) {
    return 0;
  }

  const sign = match[1] === "-" ? -1 : 1;
  const hours = Number(match[2] || 0);
  const minutes = Number(match[3] || 0);
  return sign * (hours * 60 + minutes);
}

function zonedDateTimeToIso(dateKey, hour, minute, second = 0, timeZone = "Asia/Shanghai") {
  const [year, month, day] = String(dateKey || "").split("-").map((value) => Number(value));
  const approxUtc = new Date(Date.UTC(year, (month || 1) - 1, day || 1, hour || 0, minute || 0, second || 0));
  const offsetMinutes = getOffsetMinutes(approxUtc, timeZone);
  return new Date(approxUtc.getTime() - offsetMinutes * 60 * 1000).toISOString();
}

function buildPublicCaseUrl(caseId) {
  if (!caseId) {
    return null;
  }
  return `https://trotracker.com/case/${caseId}`;
}

function toDateKey(value, timeZone = "Asia/Shanghai") {
  const ms = Date.parse(String(value || ""));
  if (!Number.isFinite(ms)) {
    return null;
  }
  return formatDateKey(new Date(ms), timeZone);
}

function extractDateFromUrl(value = "") {
  const raw = String(value || "");
  const slashMatch = raw.match(/\/(20\d{2})[\/\-_.](\d{1,2})[\/\-_.](\d{1,2})(?:\/|$)/);
  if (slashMatch) {
    return `${slashMatch[1]}-${slashMatch[2].padStart(2, "0")}-${slashMatch[3].padStart(2, "0")}`;
  }

  const compactMatch = raw.match(/\b(20\d{2})(\d{2})(\d{2})\b/);
  if (compactMatch) {
    return `${compactMatch[1]}-${compactMatch[2]}-${compactMatch[3]}`;
  }

  return null;
}

function buildDateMarkers(localDate) {
  const [year, month, day] = String(localDate || "").split("-").map((value) => Number(value || 0));
  if (!year || !month || !day) {
    return [];
  }

  return [
    `${year}-${pad(month)}-${pad(day)}`,
    `${year}/${pad(month)}/${pad(day)}`,
    `${year}.${pad(month)}.${pad(day)}`,
    `${year}年${month}月${day}日`,
    `${year}年${pad(month)}月${pad(day)}日`
  ];
}

function textMentionsLocalDate(value, localDate) {
  const text = String(value || "");
  return buildDateMarkers(localDate).some((marker) => text.includes(marker));
}

function extractMetaContent(html, names = []) {
  const source = String(html || "");
  for (const name of names) {
    const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const patterns = [
      new RegExp(`<meta[^>]+(?:property|name)=["']${escaped}["'][^>]+content=["']([^"']+)["']`, "i"),
      new RegExp(`<meta[^>]+content=["']([^"']+)["'][^>]+(?:property|name)=["']${escaped}["']`, "i")
    ];
    for (const pattern of patterns) {
      const match = source.match(pattern);
      if (match?.[1]) {
        return cleanText(match[1]);
      }
    }
  }

  return "";
}

function extractTitle(html) {
  const preferred = extractMetaContent(html, ["og:title", "twitter:title", "title"]);
  if (preferred) {
    return preferred;
  }

  const h1Match = String(html || "").match(/<h1\b[^>]*>([\s\S]*?)<\/h1>/i);
  if (h1Match?.[1]) {
    return cleanText(h1Match[1]);
  }

  const titleMatch = String(html || "").match(/<title\b[^>]*>([\s\S]*?)<\/title>/i);
  if (titleMatch?.[1]) {
    return cleanText(titleMatch[1]);
  }

  return "";
}

function extractPublishedAt(html, url = "", fallbackText = "") {
  const candidates = [
    extractMetaContent(html, [
      "article:published_time",
      "og:published_time",
      "publish-date",
      "pubdate",
      "date",
      "article:modified_time"
    ])
  ];

  for (const match of String(html || "").matchAll(/<time\b[^>]*datetime=["']([^"']+)["']/gi)) {
    candidates.push(cleanText(match[1]));
  }

  for (const match of String(html || "").matchAll(/"datePublished"\s*:\s*"([^"]+)"/gi)) {
    candidates.push(cleanText(match[1]));
  }

  for (const match of String(html || "").matchAll(/(20\d{2}[\/\-_.]\d{1,2}[\/\-_.]\d{1,2}(?:[ T]\d{1,2}:\d{2}(?::\d{2})?(?:Z|[+-]\d{2}:?\d{2})?)?)/g)) {
    candidates.push(cleanText(match[1]));
  }

  for (const match of String(`${fallbackText} ${html}`).matchAll(/(20\d{2}年\d{1,2}月\d{1,2}日)/g)) {
    candidates.push(cleanText(match[1]));
  }

  candidates.push(extractDateFromUrl(url) || "");

  return candidates.find((value) => value) || "";
}

function extractBodyText(html) {
  const source = String(html || "");
  const blocks = [];

  const mainMatch = source.match(/<(article|main|section)\b[^>]*>([\s\S]*?)<\/\1>/i);
  if (mainMatch?.[2]) {
    blocks.push(cleanText(mainMatch[2]));
  }

  for (const pattern of [
    /<div\b[^>]+class=["'][^"']*(?:content|article|post|entry|detail|news|rich_media_content)[^"']*["'][^>]*>([\s\S]*?)<\/div>/gi,
    /<section\b[^>]+class=["'][^"']*(?:content|article|post|entry|detail|news)[^"']*["'][^>]*>([\s\S]*?)<\/section>/gi
  ]) {
    for (const match of source.matchAll(pattern)) {
      const text = cleanText(match[1]);
      if (text) {
        blocks.push(text);
      }
    }
  }

  blocks.push(cleanText(source));

  return blocks.sort((left, right) => right.length - left.length)[0] || "";
}

function extractLinksFromMarkup(markup, baseUrl) {
  const results = [];
  const source = String(markup || "");

  for (const match of source.matchAll(/<loc>(.*?)<\/loc>/gi)) {
    const url = normalizeUrl(cleanText(match[1]), baseUrl);
    if (!url) {
      continue;
    }

    results.push({
      url,
      titleHint: "",
      context: ""
    });
  }

  for (const match of source.matchAll(/<a\b[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi)) {
    const url = normalizeUrl(match[1], baseUrl);
    if (!url) {
      continue;
    }

    const titleHint = cleanText(match[2]);
    const snippet = cleanText(source.slice(Math.max(0, (match.index || 0) - 180), (match.index || 0) + 240));
    results.push({
      url,
      titleHint,
      context: snippet
    });
  }

  return results;
}

function isLikelyArticleCandidate(candidate, source) {
  const url = String(candidate.url || "");
  const titleHint = String(candidate.titleHint || "");
  const haystack = `${url} ${titleHint} ${candidate.context || ""}`.toLowerCase();

  if (!url || /\.(?:jpg|jpeg|png|gif|webp|svg|pdf|zip|rar|mp4|mp3)$/i.test(url)) {
    return false;
  }

  try {
    const host = new URL(url).hostname.toLowerCase();
    if (Array.isArray(source.allowedHosts) && source.allowedHosts.length) {
      const allowed = source.allowedHosts.some((value) => host === value || host.endsWith(`.${value}`));
      if (!allowed) {
        return false;
      }
    }
  } catch {
    return false;
  }

  if (Array.isArray(source.articleUrlPatterns) && source.articleUrlPatterns.some((value) => url.includes(value))) {
    return true;
  }

  if (ARTICLE_HINT_PATTERNS.some((pattern) => pattern.test(url))) {
    return true;
  }

  return TRO_KEYWORDS.some((keyword) => haystack.includes(keyword));
}

function scoreCandidate(candidate, source, localDate) {
  let score = 0;
  const haystack = `${candidate.url || ""} ${candidate.titleHint || ""} ${candidate.context || ""}`.toLowerCase();

  if (Array.isArray(source.articleUrlPatterns) && source.articleUrlPatterns.some((value) => String(candidate.url || "").includes(value))) {
    score += 6;
  }

  if (ARTICLE_HINT_PATTERNS.some((pattern) => pattern.test(String(candidate.url || "")))) {
    score += 3;
  }

  for (const keyword of TRO_KEYWORDS) {
    if (haystack.includes(keyword)) {
      score += 2;
    }
  }

  if (textMentionsLocalDate(candidate.context, localDate)) {
    score += 1;
  }

  return score;
}

function extractDocketNumber(value) {
  const match = String(value || "").match(/\b(?:\d+:)?\d{2}-cv-\d{3,6}\b/i);
  return match ? match[0].trim() : "";
}

function looksTroRelated(value) {
  const text = String(value || "").toLowerCase();
  if (!text) {
    return false;
  }

  if (extractDocketNumber(text)) {
    return true;
  }

  return TRO_KEYWORDS.some((keyword) => text.includes(keyword));
}

function summarizeText(value, maxLength = 120) {
  const text = normalizeText(value);
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, Math.max(0, maxLength - 1)).trim()}…`;
}

async function fetchText(url, timeoutMs) {
  const response = await fetch(url, {
    headers: {
      "user-agent": DEFAULT_USER_AGENT,
      accept: "text/html,application/xhtml+xml,application/xml;q=0.9,text/plain;q=0.8,*/*;q=0.7",
      "accept-language": "zh-CN,zh;q=0.9,en;q=0.8"
    },
    signal: AbortSignal.timeout(timeoutMs)
  });

  if (!response.ok) {
    throw new Error(`${url} responded with ${response.status}`);
  }

  return await response.text();
}

async function collectSourceArticles(source, { localDate, timeZone, timeoutMs, candidateLimit, perSourceFetchLimit }) {
  const candidates = [];
  const errors = [];

  for (const discoveryUrl of source.discoveryUrls) {
    try {
      const discoveryMarkup = await fetchText(discoveryUrl, timeoutMs);
      candidates.push(...extractLinksFromMarkup(discoveryMarkup, discoveryUrl));
    } catch (error) {
      errors.push(`${source.label} discovery failed: ${error.message || String(error)}`);
    }
  }

  const rankedCandidates = [...new Map(
    candidates
      .filter((candidate) => isLikelyArticleCandidate(candidate, source))
      .map((candidate) => [candidate.url, candidate])
  ).values()]
    .map((candidate) => ({
      ...candidate,
      score: scoreCandidate(candidate, source, localDate)
    }))
    .sort((left, right) => right.score - left.score)
    .slice(0, Math.max(1, candidateLimit));

  const articles = [];
  const seenUrls = new Set();

  for (const candidate of rankedCandidates) {
    if (seenUrls.has(candidate.url)) {
      continue;
    }
    seenUrls.add(candidate.url);

    if (articles.length >= perSourceFetchLimit) {
      break;
    }

    try {
      const html = await fetchText(candidate.url, timeoutMs);
      const title = extractTitle(html) || candidate.titleHint || source.label;
      const bodyText = extractBodyText(html);
      const publishedAt = extractPublishedAt(html, candidate.url, candidate.context);
      const derivedDateKey =
        toDateKey(publishedAt, timeZone) ||
        extractDateFromUrl(candidate.url) ||
        (textMentionsLocalDate(`${candidate.context} ${title} ${bodyText}`, localDate) ? localDate : null);

      if (derivedDateKey !== localDate) {
        continue;
      }

      if (!looksTroRelated(`${title}\n${bodyText}\n${candidate.context}`)) {
        continue;
      }

      const docketNumber = extractDocketNumber(`${title}\n${bodyText}\n${candidate.context}`);
      articles.push({
        sourceId: source.id,
        source: source.label,
        url: candidate.url,
        title,
        publishedAt: publishedAt || `${localDate}T00:00:00+08:00`,
        docketNumber: docketNumber || null,
        summary: summarizeText(bodyText || candidate.context || title, 180),
        bodyText,
        rawTitleHint: candidate.titleHint || ""
      });
    } catch (error) {
      errors.push(`${source.label} article failed: ${candidate.url} -> ${error.message || String(error)}`);
    }
  }

  return {
    source,
    articles,
    errors
  };
}

function resolveCaseReference(store, docketNumber = "", title = "") {
  const normalizedDocket = normalizeText(docketNumber);
  if (normalizedDocket) {
    const payload = store.listCases({
      startDate: "2020-01-01",
      category: "all",
      search: normalizedDocket,
      page: 1,
      pageSize: 5
    });
    const needle = normalizedDocket.toLowerCase().replace(/\s+/g, "");
    const exact = (payload.items || []).find(
      (item) => String(item.docket_number || "").toLowerCase().replace(/\s+/g, "") === needle
    );
    if (exact) {
      return {
        caseId: Number(exact.id || 0) || null,
        docketNumber: exact.docket_number || normalizedDocket,
        caseName: exact.case_name || null
      };
    }
  }

  const searchTitle = summarizeText(title, 80);
  if (searchTitle) {
    const payload = store.listCases({
      startDate: "2020-01-01",
      category: "all",
      search: searchTitle,
      page: 1,
      pageSize: 3
    });
    if (payload.items?.[0]) {
      return {
        caseId: Number(payload.items[0].id || 0) || null,
        docketNumber: payload.items[0].docket_number || normalizedDocket || null,
        caseName: payload.items[0].case_name || null
      };
    }
  }

  return {
    caseId: null,
    docketNumber: normalizedDocket || null,
    caseName: null
  };
}

function simplifyTitle(value) {
  return normalizeText(
    String(value || "")
      .replace(/\b(?:卖家支持|麦家支持|墨婷跨境|青枫TRO|SELLERAEGIS|SellerAegis|赛贝维权申诉|赛贝维权|赛贝)\b/gi, " ")
      .replace(/[|｜丨:：\-—–]+/g, " ")
  );
}

function inferFocus(group) {
  const haystack = group.articles.map((item) => `${item.title} ${item.summary} ${item.bodyText}`).join(" ").toLowerCase();
  if (/和解|settlement/i.test(haystack)) {
    return "和解与应诉进展";
  }
  if (/冻结|restraining|injunction|temporary restraining order|tro/i.test(haystack)) {
    return "冻结与 TRO 动态";
  }
  if (/起诉|立案|complaint|filed/i.test(haystack)) {
    return "立案与起诉动态";
  }
  if (/撤诉|dismiss|解除冻结|解冻/i.test(haystack)) {
    return "撤诉与解冻动向";
  }
  return "案件进展";
}

function buildGroupKey(article, reference) {
  if (reference.caseId) {
    return `case:${reference.caseId}`;
  }
  if (reference.docketNumber) {
    return `docket:${String(reference.docketNumber).toLowerCase().replace(/\s+/g, "")}`;
  }
  return `title:${simplifyTitle(article.title).toLowerCase().slice(0, 120)}`;
}

function buildRoundupTitle(group) {
  const label = group.caseName || group.docketNumber || summarizeText(group.articles[0]?.title || "TRO 案件", 42);
  if (group.sources.length > 1) {
    return `${group.sources.join("、")} 同步跟进 ${label}`;
  }
  return `${group.sources[0] || "TRO 来源"} 跟进 ${label}`;
}

function buildRoundupSummary(group) {
  const focus = inferFocus(group);
  const prefix = group.docketNumber ? `案号 ${group.docketNumber}` : group.caseName ? group.caseName : "这起案件";
  if (group.sources.length > 1) {
    return `${prefix} 今天被 ${group.sources.length} 家来源同时提到，重点围绕${focus}。`;
  }
  return `${prefix} 今天出现新的公开动态，重点围绕${focus}。`;
}

function sortArticlesByTime(items = []) {
  return [...items].sort((left, right) => Date.parse(right.publishedAt || 0) - Date.parse(left.publishedAt || 0));
}

function buildTopItems(groups = [], itemLimit = 3) {
  return [...groups]
    .map((group) => {
      const publishedAt = sortArticlesByTime(group.articles)[0]?.publishedAt || null;
      const heat =
        group.sources.length * 50 +
        group.articles.length * 10 +
        (group.docketNumber ? 12 : 0) +
        (group.caseId ? 8 : 0);

      return {
        title: buildRoundupTitle(group),
        summary: buildRoundupSummary(group),
        sources: group.sources,
        heat,
        publishedAt,
        docketNumber: group.docketNumber,
        caseName: group.caseName,
        caseId: group.caseId,
        links: sortArticlesByTime(group.articles).map((article) => ({
          source: article.source,
          title: article.title,
          url: article.url,
          publishedAt: article.publishedAt
        }))
      };
    })
    .sort((left, right) => {
      const heatDiff = Number(right.heat || 0) - Number(left.heat || 0);
      if (heatDiff !== 0) {
        return heatDiff;
      }
      return Date.parse(right.publishedAt || 0) - Date.parse(left.publishedAt || 0);
    })
    .slice(0, Math.max(1, itemLimit));
}

function buildSourceUpdates(sourceResults = []) {
  return sourceResults
    .map((result) => ({
      source: result.source.label,
      count: result.articles.length,
      articles: sortArticlesByTime(result.articles).map((article) => ({
        title: article.title,
        url: article.url,
        summary: article.summary,
        publishedAt: article.publishedAt,
        docketNumber: article.docketNumber
      }))
    }))
    .sort((left, right) => right.count - left.count);
}

function buildOverlapGroups(groups = []) {
  return groups
    .filter((group) => group.sources.length > 1)
    .map((group) => ({
      title: buildRoundupTitle(group),
      summary: buildRoundupSummary(group),
      sources: group.sources,
      docketNumber: group.docketNumber,
      caseId: group.caseId,
      caseName: group.caseName,
      linkCount: group.articles.length
    }))
    .sort((left, right) => right.sources.length - left.sources.length);
}

export async function collectTroDailyRoundup({ config, store, localDate, timeZone }) {
  const reportConfig = config.reports?.troDailyRoundup || {};
  const sources = loadTroDailyRoundupSources(config);
  const timeoutMs = Math.max(2000, Number(reportConfig.timeoutMs || 12000));
  const candidateLimit = Math.max(4, Number(reportConfig.candidateLimit || 24));
  const perSourceFetchLimit = Math.max(2, Number(reportConfig.perSourceFetchLimit || 10));
  const itemLimit = Math.max(1, Number(reportConfig.itemLimit || config.reports?.troDailyUpdates?.maxItems || 3));

  const sourceResults = [];
  for (const source of sources) {
    sourceResults.push(
      await collectSourceArticles(source, {
        localDate,
        timeZone,
        timeoutMs,
        candidateLimit,
        perSourceFetchLimit
      })
    );
  }

  const groups = new Map();
  for (const result of sourceResults) {
    for (const article of result.articles) {
      const reference = resolveCaseReference(store, article.docketNumber, article.title);
      const key = buildGroupKey(article, reference);
      if (!groups.has(key)) {
        groups.set(key, {
          caseId: reference.caseId,
          docketNumber: reference.docketNumber || article.docketNumber || null,
          caseName: reference.caseName || null,
          sources: [],
          articles: []
        });
      }

      const group = groups.get(key);
      if (reference.caseId && !group.caseId) {
        group.caseId = reference.caseId;
      }
      if (reference.caseName && !group.caseName) {
        group.caseName = reference.caseName;
      }
      if (reference.docketNumber && !group.docketNumber) {
        group.docketNumber = reference.docketNumber;
      }
      if (!group.sources.includes(article.source)) {
        group.sources.push(article.source);
      }
      group.articles.push({
        ...article,
        caseId: reference.caseId,
        caseName: reference.caseName,
        docketNumber: reference.docketNumber || article.docketNumber || null
      });
    }
  }

  const grouped = [...groups.values()];
  const items = buildTopItems(grouped, itemLimit);
  const sourceUpdates = buildSourceUpdates(sourceResults);
  const overlaps = buildOverlapGroups(grouped);
  const errors = sourceResults.flatMap((result) => result.errors || []);

  return {
    updatedAt: new Date().toISOString(),
    localDate,
    items,
    total: grouped.length,
    sourceUpdates,
    overlaps,
    errors
  };
}

function buildEmailMessage(payload = {}) {
  const itemBlocks = Array.isArray(payload.items) ? payload.items : [];
  const sourceUpdates = Array.isArray(payload.sourceUpdates) ? payload.sourceUpdates : [];
  const overlaps = Array.isArray(payload.overlaps) ? payload.overlaps : [];
  const errors = Array.isArray(payload.errors) ? payload.errors : [];
  const subject = `TRO 每日动态 - ${payload.localDate || formatDateKey(new Date(), "Asia/Shanghai")}`;

  const topText = itemBlocks.length
    ? itemBlocks.map((item, index) => {
        const links = Array.isArray(item.links)
          ? item.links.map((link) => `     - ${link.source}: ${link.title} | ${link.url}`).join("\n")
          : "     - 无";
        return [
          `${index + 1}. ${item.title}`,
          `   ${item.summary || "暂无摘要"}`,
          `   来源：${(item.sources || []).join("、") || "未知"}`,
          `   系统案件：${item.caseId ? buildPublicCaseUrl(item.caseId) : "未命中本站案件，前端将跳转到微信联系区"}`,
          `   新闻链接：`,
          links
        ].join("\n");
      })
    : ["今天没有整理出可发送的重点动态。"];

  const sourceText = sourceUpdates.length
    ? sourceUpdates.map((source) => {
        const rows = source.articles.length
          ? source.articles
              .slice(0, 6)
              .map((article) => `   - ${article.title} | ${article.url}`)
              .join("\n")
          : "   - 今日没有命中同日公开更新";
        return `${source.source}（${source.count}）\n${rows}`;
      })
    : ["今日没有抓到来源更新。"];

  const overlapText = overlaps.length
    ? overlaps.map((item) => `- ${item.title} | ${item.sources.join("、")} | ${item.docketNumber || "无案号"}`)
    : ["- 今日没有多家同时提及的同案更新。"];

  const text = [
    "TRO 每日动态",
    `日期：${payload.localDate}`,
    "",
    "今日重点：",
    ...topText,
    "",
    "多家同时提及：",
    ...overlapText,
    "",
    "按来源汇总：",
    ...sourceText,
    ...(errors.length ? ["", "抓取告警：", ...errors.map((item) => `- ${item}`)] : [])
  ].join("\n");

  const html = `
    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;line-height:1.6;">
      <h2>TRO 每日动态</h2>
      <p><strong>日期：</strong>${escapeHtml(payload.localDate)}</p>
      <h3>今日重点</h3>
      ${
        itemBlocks.length
          ? `<ol>${itemBlocks
              .map((item) => {
                const links = Array.isArray(item.links)
                  ? item.links
                      .map(
                        (link) =>
                          `<li>${escapeHtml(link.source)}：<a href="${escapeHtml(link.url)}">${escapeHtml(link.title || link.url)}</a></li>`
                      )
                      .join("")
                  : "";
                const caseHtml = item.caseId
                  ? `<a href="${escapeHtml(buildPublicCaseUrl(item.caseId))}">系统案件页</a>`
                  : "未命中本站案件，前端将跳转到微信联系区";
                return `
                  <li>
                    <strong>${escapeHtml(item.title)}</strong><br>
                    ${escapeHtml(item.summary || "暂无摘要")}<br>
                    <strong>来源：</strong>${escapeHtml((item.sources || []).join("、") || "未知")}<br>
                    <strong>系统案件：</strong>${caseHtml}<br>
                    <strong>新闻链接：</strong>
                    ${links ? `<ul>${links}</ul>` : " 无"}
                  </li>
                `;
              })
              .join("")}</ol>`
          : "<p>今天没有整理出可发送的重点动态。</p>"
      }
      <h3>多家同时提及</h3>
      ${
        overlaps.length
          ? `<ul>${overlaps
              .map(
                (item) =>
                  `<li>${escapeHtml(item.title)} | ${escapeHtml(item.sources.join("、"))} | ${escapeHtml(item.docketNumber || "无案号")}</li>`
              )
              .join("")}</ul>`
          : "<p>今日没有多家同时提及的同案更新。</p>"
      }
      <h3>按来源汇总</h3>
      ${sourceUpdates
        .map(
          (source) => `
            <h4>${escapeHtml(source.source)}（${source.count}）</h4>
            ${
              source.articles.length
                ? `<ul>${source.articles
                    .slice(0, 6)
                    .map(
                      (article) =>
                        `<li><a href="${escapeHtml(article.url)}">${escapeHtml(article.title)}</a>${article.docketNumber ? ` | ${escapeHtml(article.docketNumber)}` : ""}</li>`
                    )
                    .join("")}</ul>`
                : "<p>今日没有命中同日公开更新。</p>"
            }
          `
        )
        .join("")}
      ${
        errors.length
          ? `<h3>抓取告警</h3><ul>${errors.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>`
          : ""
      }
    </div>
  `.trim();

  return { subject, text, html };
}

function writeTroDailyUpdatesFile(filePath, payload) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf-8");
}

export class TroDailyRoundupService {
  constructor({ config, store }) {
    this.config = config;
    this.store = store;
    this.emailConfig = config.email || {};
    this.reportConfig = config.reports?.troDailyRoundup || {};
    this.outputConfig = config.reports?.troDailyUpdates || {};
    this.checkpointKey = `tro-daily-roundup:${this.reportConfig.to || this.config.reports?.dailyEmail?.to || "default"}`;
  }

  isEnabled() {
    return Boolean(this.reportConfig.enabled);
  }

  hasTransportConfig() {
    const to = this.reportConfig.to || this.config.reports?.dailyEmail?.to || this.emailConfig.user || "";
    return Boolean(
      this.emailConfig.host &&
      this.emailConfig.port &&
      this.emailConfig.user &&
      this.emailConfig.pass &&
      this.emailConfig.from &&
      to
    );
  }

  async maybeSendScheduledRoundup() {
    if (!this.isEnabled()) {
      return { sent: false, reason: "disabled" };
    }

    if (!this.hasTransportConfig()) {
      return { sent: false, reason: "missing-config" };
    }

    const now = new Date();
    const timeZone = this.reportConfig.timeZone || "Asia/Shanghai";
    const { hour, minute } = getClockParts(now, timeZone);
    if (hour !== Number(this.reportConfig.hour || 20) || minute !== Number(this.reportConfig.minute || 0)) {
      return { sent: false, reason: "not-time" };
    }

    const localDate = formatDateKey(now, timeZone);
    const checkpoint = this.store.getCheckpoint(this.checkpointKey) || {};
    if (checkpoint.localDate === localDate) {
      return { sent: false, reason: "already-sent", localDate };
    }

    return this.sendRoundup({ localDate });
  }

  async sendRoundup({ localDate = formatDateKey(new Date(), this.reportConfig.timeZone || "Asia/Shanghai"), force = false } = {}) {
    if (!this.hasTransportConfig()) {
      return { sent: false, reason: "missing-config" };
    }

    const checkpoint = this.store.getCheckpoint(this.checkpointKey) || {};
    if (!force && checkpoint.localDate === localDate) {
      return { sent: false, reason: "already-sent", localDate };
    }

    const runId = this.store.claimSyncRun("tro-daily-roundup", "report", 60);
    if (!runId) {
      return { sent: false, reason: "already-running", localDate };
    }

    try {
      const timeZone = this.reportConfig.timeZone || "Asia/Shanghai";
      const payload = await collectTroDailyRoundup({
        config: this.config,
        store: this.store,
        localDate,
        timeZone
      });

      writeTroDailyUpdatesFile(this.outputConfig.path, payload);

      const mail = buildEmailMessage(payload);
      const transport = nodemailer.createTransport({
        host: this.emailConfig.host,
        port: Number(this.emailConfig.port || 465),
        secure: Boolean(this.emailConfig.secure),
        auth: {
          user: this.emailConfig.user,
          pass: this.emailConfig.pass
        }
      });

      const to = this.reportConfig.to || this.config.reports?.dailyEmail?.to || this.emailConfig.user || "";
      const info = await transport.sendMail({
        from: this.emailConfig.from,
        to,
        subject: mail.subject,
        text: mail.text,
        html: mail.html
      });

      const checkpointPayload = {
        localDate,
        sentAt: new Date().toISOString(),
        messageId: info.messageId || null,
        itemCount: payload.items.length,
        totalGroups: payload.total,
        sourceCount: payload.sourceUpdates.length
      };
      this.store.saveCheckpoint(this.checkpointKey, checkpointPayload);
      this.store.finishSyncRun(runId, "succeeded", checkpointPayload);
      return {
        sent: true,
        ...checkpointPayload
      };
    } catch (error) {
      this.store.finishSyncRun(runId, "failed", { localDate }, error.message || String(error));
      throw error;
    }
  }
}
