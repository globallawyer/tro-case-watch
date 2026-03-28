import fs from "node:fs";

function normalizeText(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function normalizeList(values = []) {
  const list = Array.isArray(values) ? values : [values];
  return [...new Set(list.map(normalizeText).filter(Boolean))];
}

function normalizeSource(source = {}) {
  const id = normalizeText(source.id || "").toLowerCase();
  const label = normalizeText(source.label || source.name || source.id || "");
  if (!id || !label) {
    return null;
  }

  return {
    id,
    label,
    aliases: normalizeList(source.aliases || []),
    discoveryUrls: normalizeList(source.discoveryUrls || source.discovery_urls || []),
    allowedHosts: normalizeList(source.allowedHosts || source.allowed_hosts || []).map((value) => value.toLowerCase()),
    articleUrlPatterns: normalizeList(source.articleUrlPatterns || source.article_url_patterns || []),
    note: normalizeText(source.note || "")
  };
}

export const DEFAULT_TRO_DAILY_ROUNDUP_SOURCES = [
  {
    id: "maijiazhichi",
    label: "卖家支持",
    aliases: ["麦家支持"],
    discoveryUrls: ["https://maijiazhichi.com/", "https://worldtro.com/tro/"],
    allowedHosts: ["maijiazhichi.com", "www.maijiazhichi.com", "worldtro.com", "www.worldtro.com"],
    articleUrlPatterns: ["/tro/", "/article/", "/news/", "/case/", "/detail/"],
    note: "优先抓官网与 worldtro 的公开动态页"
  },
  {
    id: "moting",
    label: "墨婷跨境",
    discoveryUrls: ["https://www.amz123.com/moting"],
    allowedHosts: ["amz123.com", "www.amz123.com"],
    articleUrlPatterns: ["/t/", "/article/"],
    note: "先用 AMZ123 公开作者页作为稳定公开入口"
  },
  {
    id: "qingfeng",
    label: "青枫TRO",
    aliases: ["青枫"],
    discoveryUrls: ["https://www.123tro.com/help", "https://www.123tro.com/"],
    allowedHosts: ["123tro.com", "www.123tro.com"],
    articleUrlPatterns: ["/help/detail/", "/article/", "/news/"],
    note: "优先抓 123tro 的公开帮助与公告页"
  },
  {
    id: "selleraegis",
    label: "SELLERAEGIS",
    aliases: ["SellerAegis", "卖家守护"],
    discoveryUrls: ["https://www.selleraegis.com/"],
    allowedHosts: [
      "selleraegis.com",
      "www.selleraegis.com",
      "amztro.com",
      "www.amztro.com",
      "amzipr.com",
      "www.amzipr.com"
    ],
    articleUrlPatterns: ["/article/", "/news/", "/case/", "/blog/", ".html"],
    note: "站点结构会变，这里保留多域名兜底"
  },
  {
    id: "saibei",
    label: "赛贝维权",
    aliases: ["赛贝", "赛贝维权申诉"],
    discoveryUrls: ["https://www.saibeiip.com/", "https://tro.saibeiip.com/"],
    allowedHosts: ["saibeiip.com", "www.saibeiip.com", "tro.saibeiip.com"],
    articleUrlPatterns: ["/article/", "/news/", "/detail/", "/tro/"],
    note: "同时看主站与赛贝 TRO 查询站的公开页面"
  }
].map(normalizeSource);

export function loadTroDailyRoundupSources(config = {}) {
  const configuredPath = String(config.reports?.troDailyRoundup?.sourcesPath || "").trim();
  if (configuredPath && fs.existsSync(configuredPath)) {
    try {
      const parsed = JSON.parse(fs.readFileSync(configuredPath, "utf-8"));
      const rawSources = Array.isArray(parsed) ? parsed : Array.isArray(parsed.sources) ? parsed.sources : [];
      const normalized = rawSources.map(normalizeSource).filter(Boolean);
      if (normalized.length) {
        return normalized;
      }
    } catch (error) {
      console.error("[tro-daily-roundup] failed to load custom sources", error);
    }
  }

  return DEFAULT_TRO_DAILY_ROUNDUP_SOURCES;
}
