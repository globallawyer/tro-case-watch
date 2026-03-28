const state = {
  page: 1,
  pageSize: 15,
  category: "watchlist",
  court: "",
  search: "",
  selectedCaseId: null,
  pageCount: 1
};

const detailCache = new Map();
const detailCacheTtlMs = 2 * 60 * 1000;
let currentCasesPayload = null;
let detailRequestToken = 0;
let casesRequestToken = 0;
let detailRefreshTimer = null;
const inflightDetailRequests = new Map();
const caseRoutePattern = /^\/case\/(\d+)\/?$/;
let latestStatusPayload = null;

const caseList = document.querySelector("#case-list");
const detailPanel = document.querySelector("#detail-panel");
const heroStats = document.querySelector("#hero-stats");
const casesSummary = document.querySelector("#cases-summary");
const pageIndicator = document.querySelector("#page-indicator");
const courtFilter = document.querySelector("#court-filter");
const lookupForm = document.querySelector("#lookup-form");
const lookupInput = document.querySelector("#lookup-input");
const prevPageButton = document.querySelector("#prev-page");
const nextPageButton = document.querySelector("#next-page");
const refreshButton = document.querySelector("#refresh-button");
const contentGrid = document.querySelector(".content-grid");
const copyWechatButton = document.querySelector("#copy-wechat-button");
const statusPollMs = 5 * 60 * 1000;
const apiBase = "";

const VALUE_TERM_REPLACEMENTS = [
  ["专利", "Patent"],
  ["商标", "Trademark"],
  ["版权", "Copyright"],
  ["和解", "Settlement"],
  ["撤案", "Dismissal"],
  ["结案", "Closed"],
  ["送达", "Service"],
  ["持续观察", "Monitoring"],
  ["原告", "Plaintiff"],
  ["品牌", "Brand"],
  ["被告", "Defendant"],
  ["律所", "Counsel"]
];

function translateValue(value) {
  let text = String(value ?? "").trim();
  for (const [source, target] of VALUE_TERM_REPLACEMENTS) {
    text = text.replaceAll(source, target);
  }
  return text;
}

function getRouteCaseId() {
  const match = window.location.pathname.match(caseRoutePattern);
  if (!match) {
    return null;
  }

  const value = Number(match[1]);
  return Number.isFinite(value) ? value : null;
}

function setCaseRoute(caseId, { replace = false } = {}) {
  if (!window.history?.pushState) {
    return;
  }

  const url = new URL(window.location.href);
  url.pathname = caseId ? `/case/${caseId}` : "/";
  url.hash = caseId ? "detail-panel" : "";
  const method = replace ? "replaceState" : "pushState";
  window.history[method](null, "", url);
}

function formatDate(value) {
  if (!value) {
    return "Unknown";
  }

  return new Date(value).toLocaleDateString("en-US", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  });
}

function request(path, options) {
  return fetch(`${apiBase}${path}`, options).then(async (response) => {
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    return response.json();
  });
}

function toneClass(status) {
  return status?.tone || "neutral";
}

function heroCard(label, value) {
  return `
    <article class="stat-card">
      <span>${label}</span>
      <strong>${value}</strong>
    </article>
  `;
}

function renderHero(status) {
  latestStatusPayload = status;
  const totals = status.dashboard?.totals || {};
  const recentSync = status.dashboard?.recentSync;
  const cards = [
    heroCard("Total Cases / Watchlist", `${totals.total_cases || 0} / ${totals.watchlist_cases || 0}`),
    heroCard("TRO / Schedule A", `${totals.tro_cases || 0} / ${totals.schedule_a_cases || 0}`),
    heroCard("Added Today", totals.today_added_watchlist || 0),
    heroCard(
      "Last Sync",
      recentSync?.finished_at
        ? new Date(recentSync.finished_at).toLocaleTimeString("en-US", { hour12: false })
        : recentSync?.status === "running"
          ? "Running"
          : "Idle"
    )
  ];

  heroStats.innerHTML = cards.join("");
}

function isDetailPanelVisible() {
  const rect = detailPanel.getBoundingClientRect();
  return rect.top < window.innerHeight * 0.8 && rect.bottom > window.innerHeight * 0.2;
}

function jumpToDetailPanel() {
  if (!detailPanel) {
    return;
  }

  if (!isDetailPanelVisible() || window.innerWidth < 1180) {
    detailPanel.scrollIntoView({
      behavior: "smooth",
      block: "start"
    });
  }

  if (window.location.hash !== "#detail-panel" && window.history?.replaceState) {
    const url = new URL(window.location.href);
    url.hash = "detail-panel";
    window.history.replaceState(null, "", url);
  }
}

function revealResultsIfNeeded() {
  if (!contentGrid) {
    return;
  }

  const top = contentGrid.getBoundingClientRect().top;
  if (top > window.innerHeight * 0.68) {
    contentGrid.scrollIntoView({
      behavior: "smooth",
      block: "start"
    });
  }
}

function renderCourtOptions(courts) {
  const selected = state.court;
  courtFilter.innerHTML = [`<option value="">All Courts</option>`]
    .concat(
      courts.map(
        (court) =>
          `<option value="${court.court_id || ""}" ${court.court_id === selected ? "selected" : ""}>${court.court_name} (${court.total})</option>`
      )
    )
    .join("");
}

function renderCasesLoading(message = "Loading cases...") {
  casesSummary.textContent = message;
  caseList.innerHTML = `
    <article class="case-row">
      <h3>Loading case list</h3>
      <p>Your request was received. Syncing on-site results now.</p>
    </article>
  `;
}

function caseStatusBadge(item) {
  const status = item.insights?.status || {};
  return `<span class="status-pill ${toneClass(status)}">${translateValue(status.label || "Monitoring")}</span>`;
}

function tagPills(item) {
  const values = [...new Set(item.insights?.badges || [])];
  return values.map((value) => `<span class="tag-pill">${translateValue(value)}</span>`).join("");
}

function highlightPills(item) {
  const values = item.insights?.highlights || [];
  return values.map((value) => `<span class="highlight-pill">${translateValue(value)}</span>`).join("");
}

function displayEntryType(entry) {
  const type = String(entry.document_type || "").trim();
  if (!type) {
    return "Docket Entry";
  }

  if (/pacer document/i.test(type)) {
    return "Docket Document";
  }

  return type;
}

function renderCaseRow(item) {
  const insights = item.insights || {};
  const plaintiff = translateValue(insights.brand_name || insights.plaintiff_name || item.case_name || "Unnamed Case");
  const lawFirm = translateValue(insights.lead_law_firm || "Unknown");
  const summary =
    translateValue(insights.narrative || item.recent_activity_summary || "No docket summary is available yet.");

  return `
    <button class="case-row ${item.id === state.selectedCaseId ? "is-active" : ""}" type="button" data-case-id="${item.id}">
      <div class="case-row-top">
        <div>
          <span class="case-docket">${item.docket_number || "No docket number"}</span>
          <h3>${plaintiff}</h3>
          <div class="case-meta">
            <span>${item.court_name || "Unknown Court"}</span>
            <span>Filed ${formatDate(item.date_filed)}</span>
            <span>Plaintiff Counsel ${lawFirm}</span>
          </div>
        </div>
        <div class="status-row">
          ${caseStatusBadge(item)}
        </div>
      </div>
      <div class="tag-row">${tagPills(item)}</div>
      <div class="tag-row highlights-row">${highlightPills(item)}</div>
      <p class="case-summary">${summary}</p>
      <div class="case-foot">
        <span>Plaintiff / Brand ${plaintiff}</span>
        <span>Defendants ${insights.defendant_count || 0}</span>
        <span>Latest Activity ${formatDate(item.latest_docket_filed_at || item.date_filed)}</span>
      </div>
    </button>
  `;
}

function renderCases(payload) {
  currentCasesPayload = payload;
  state.pageCount = payload.pageCount || 1;
  renderCourtOptions(payload.courts || []);

  const messages = [];
  if (!state.search) {
    const totalCases = Number(latestStatusPayload?.dashboard?.totals?.total_cases || 0);
    messages.push(`${payload.total} cases in the active watchlist`);
    if (totalCases > 0) {
      messages.push(`${totalCases} total cases in the database`);
    }
    messages.push(`Page ${payload.page} / ${payload.pageCount}`);
  } else {
    messages.push(`${payload.total} matching cases, page ${payload.page} / ${payload.pageCount}`);
  }
  if (payload.categoryRelaxed) {
    const relaxedLabel = {
      all: "All Cases",
      watchlist: "Watchlist",
      seller_watch: "Seller Watch",
      tro: "TRO",
      schedule_a: "Schedule A"
    }[payload.relaxedCategory] || "broader scope";
    messages.push(`No direct hit in the current category. Widened to ${relaxedLabel}.`);
  }
  if (payload.liveImported?.imported) {
    messages.push(`Live-imported ${payload.liveImported.imported} matching cases`);
  }
  if (payload.lookupError) {
    messages.push(`Live import failed: ${payload.lookupError}`);
  }

  casesSummary.textContent = messages.join(" · ");
  pageIndicator.textContent = `Page ${payload.page} / ${payload.pageCount}`;

  if (!payload.items.length) {
    caseList.innerHTML = `
      <article class="case-row">
        <h3>No matching cases</h3>
        <p>Try a full docket number and the system will attempt a live public lookup.</p>
      </article>
    `;
    detailPanel.innerHTML = `
      <div class="panel-head">
        <h2>Docket View</h2>
        <p>No matching result for the current search.</p>
      </div>
    `;
    return;
  }

  const routeCaseId = getRouteCaseId();
  const selectedItem = payload.items.find((item) => item.id === state.selectedCaseId) || null;
  const fallbackItem = payload.items[0] || null;

  if ((!state.selectedCaseId || !selectedItem) && fallbackItem && !routeCaseId) {
    state.selectedCaseId = fallbackItem.id;
    loadCaseDetail(state.selectedCaseId, {
      summaryItem: fallbackItem,
      updateRoute: true
    }).catch(console.error);
  }

  caseList.innerHTML = payload.items.map(renderCaseRow).join("");
  updateActiveCaseRow();
  caseList.querySelectorAll("[data-case-id]").forEach((button) => {
    const caseId = Number(button.dataset.caseId);
    button.addEventListener("mouseenter", () => {
      prefetchCaseDetail(caseId);
    });
    button.addEventListener("focus", () => {
      prefetchCaseDetail(caseId);
    });
    button.addEventListener("click", () => {
      state.selectedCaseId = caseId;
      const summaryItem = payload.items.find((item) => item.id === state.selectedCaseId) || null;
      updateActiveCaseRow();
      loadCaseDetail(state.selectedCaseId, {
        summaryItem,
        focus: true,
        updateRoute: true
      }).catch(console.error);
    });
  });
}

function updateActiveCaseRow() {
  caseList.querySelectorAll("[data-case-id]").forEach((button) => {
    button.classList.toggle("is-active", Number(button.dataset.caseId) === state.selectedCaseId);
  });
}

function adviceByStatus(statusKey) {
  if (statusKey === "tro_granted") {
    return "The TRO appears to have been entered. Focus on service, marketplace restraints, and the PI hearing schedule.";
  }

  if (statusKey === "pi") {
    return "The case is already in the Preliminary Injunction stage, where restraints are usually more durable than the initial TRO.";
  }

  if (statusKey === "settlement") {
    return "Recent docket activity suggests settlement or dismissal. Keep watching for defendants exiting the case.";
  }

  if (statusKey === "closed") {
    return "The docket shows closure, dismissal, or large-scale voluntary dismissals.";
  }

  if (statusKey === "service") {
    return "This looks more like the service and response-prep stage. Watch defendant lists, proofs of service, and the next TRO / PI step.";
  }

  return "The case is still in a monitoring phase. Keep an eye on TRO, PI, settlement, and dismissal milestones.";
}

function timelineSourceLabel(entry) {
  return translateValue(entry.timeline_label || "Docket Timeline");
}

function timelineSignals(entry) {
  const text = `${entry.document_type || ""} ${entry.description || ""}`.toLowerCase();
  const markers = [];

  if (/(temporary restraining order|\btro\b)/i.test(text)) {
    markers.push("TRO");
  }
  if (/(preliminary injunction|\bpi\b)/i.test(text)) {
    markers.push("PI");
  }
  if (/(settlement|stipulation)/i.test(text)) {
    markers.push("Settlement");
  }
  if (/(dismiss|terminated|closing)/i.test(text)) {
    markers.push("Closed");
  }
  if (/(service|serve|summons)/i.test(text)) {
    markers.push("Service");
  }

  return [...new Set(markers)];
}

function timelineSourceSummary(entries) {
  const summary = new Map();

  entries.forEach((entry) => {
    const label = timelineSourceLabel(entry);
    summary.set(label, (summary.get(label) || 0) + 1);
  });

  return [...summary.entries()]
    .map(([label, count]) => `${translateValue(label)} ${count} entries`)
    .join(" · ");
}

function renderDetail(item) {
  const insights = item.insights || {};
  const entries = item.entries || [];
  const hydrationPending = item.hydration_pending?.pending;
  const timelineSummary = entries.length ? timelineSourceSummary(entries) : "Case-level summary only";
  const summaryCards = [
    { label: "Plaintiff / Brand", value: translateValue(insights.brand_name || insights.plaintiff_name || "Unknown") },
    { label: "Plaintiff Counsel", value: translateValue(insights.lead_law_firm || "Unknown") },
    { label: "Case Stage", value: translateValue(insights.status?.label || "Monitoring") },
    { label: "Defendants", value: insights.defendant_count || 0 },
    { label: "On-site Docket", value: entries.length ? `${entries.length} entries` : "Case summary only" }
  ];
  const defendantPreview = insights.defendant_preview?.length
    ? insights.defendant_preview.join(" / ")
    : "No defendant preview is available yet.";
  const highlights = (insights.highlights || [])
    .map((value) => `<span class="highlight-pill">${translateValue(value)}</span>`)
    .join("");

  detailPanel.innerHTML = `
    <div class="panel-head detail-header">
      <span class="case-docket">${item.docket_number || "No docket number"}</span>
      <h3>${translateValue(item.case_name || insights.brand_name || "Unnamed Case")}</h3>
      <div class="status-row">
        ${caseStatusBadge(item)}
        ${tagPills(item)}
      </div>
      <div class="detail-meta">
        <span>${item.court_name || "Unknown Court"}</span>
        <span>Filed ${formatDate(item.date_filed)}</span>
        <span>Latest Activity ${formatDate(item.latest_docket_filed_at || item.date_filed)}</span>
      </div>
      <div class="summary-grid">
        ${summaryCards
          .map(
            (card) => `
              <article class="summary-card">
                <span>${card.label}</span>
                <strong>${card.value}</strong>
              </article>
            `
          )
          .join("")}
      </div>
      <div class="timeline-toolbar">
        <div class="timeline-toolbar-copy">
          <strong>View the case timeline on-site</strong>
          <p>The docket below is archived directly on this site, so you usually do not need to jump out to external sources.</p>
          ${hydrationPending ? '<p class="focus-text">Background enrichment is still running. The page will refresh as new docket entries arrive.</p>' : ""}
        </div>
        <div class="timeline-toolbar-stats">
          <span class="tag-pill">Latest Activity ${formatDate(item.latest_docket_filed_at || item.date_filed)}</span>
          <span class="tag-pill">${timelineSummary}</span>
        </div>
      </div>
    </div>

    <section class="detail-note">
      <p>${adviceByStatus(insights.status?.key)}</p>
      <p>${translateValue(insights.narrative || "Monitoring continues.")} Defendant preview: ${translateValue(defendantPreview)}</p>
      <div class="tag-row detail-highlight-row">${highlights || '<span class="tag-pill">No clear procedural milestone detected yet</span>'}</div>
    </section>

    <section class="timeline">
      <div class="timeline-head">
        <div>
          <h3>On-site Docket Timeline</h3>
          <p>Docket text is archived here in reverse chronological order so you can quickly judge TRO, PI, settlement, or closure signals.</p>
        </div>
      </div>
      ${
        entries.length
          ? entries
              .map(
                (entry) => {
                  const signals = timelineSignals(entry)
                    .map((value) => `<span class="highlight-pill">${value}</span>`)
                    .join("");

                  return `
                  <article class="timeline-item">
                    <div class="timeline-item-head">
                      <time>${formatDate(entry.filed_at)}</time>
                      <span class="status-pill neutral">${timelineSourceLabel(entry)}</span>
                    </div>
                    <h3>${entry.document_number || entry.entry_number || "No.?"} · ${displayEntryType(entry)}</h3>
                    ${signals ? `<div class="tag-row timeline-tags">${signals}</div>` : ""}
                    <p>${entry.description || "No displayable text"}</p>
                    ${entry.description_zh ? `<p class="timeline-zh">${entry.description_zh}</p>` : ""}
                    <p class="timeline-source-note">This source is archived on-site. Use your browser translator if you want an alternate language view.</p>
                  </article>
                `;
                }
              )
              .join("")
          : `
            <article class="timeline-item">
              <time>${formatDate(item.latest_docket_filed_at || item.date_filed)}</time>
              <h3>Latest Activity</h3>
              <p>${translateValue(item.recent_activity_summary || "Only case-level metadata is available right now; line-by-line docket entries have not been synced yet.")}
              </p>
              <p class="focus-text">Public sources currently provide only a case-level summary. Once full docket entries arrive, the on-site timeline will appear here.</p>
              ${item.recent_activity_summary_zh ? `<p class="timeline-zh">${item.recent_activity_summary_zh}</p>` : ""}
            </article>
          `
      }
    </section>
  `;
}

function renderDetailLoading(item = {}) {
  const docket = item.docket_number || "No docket number";
  const title = translateValue(item.case_name || item.insights?.brand_name || "Loading case details");
  const court = item.court_name || "Loading court information";
  const filedAt = formatDate(item.date_filed);

  detailPanel.innerHTML = `
    <div class="panel-head detail-header">
      <span class="case-docket">${docket}</span>
      <h3>${title}</h3>
      <div class="detail-meta">
        <span>${court}</span>
        <span>Filed ${filedAt}</span>
      </div>
    </div>
    <div class="detail-empty is-loading">
      <h3>Opening docket details</h3>
      <p>Your selection was received. Loading the on-site timeline now.</p>
    </div>
  `;
}

function getCachedDetail(caseId) {
  const cached = detailCache.get(caseId);
  if (!cached) {
    return null;
  }

  if (Date.now() - cached.cachedAt > detailCacheTtlMs) {
    detailCache.delete(caseId);
    return null;
  }

  return cached.item;
}

function shouldCacheDetail(item) {
  const entriesCount = item.entries?.length || 0;
  return !(item.insights?.badges || []).includes("Cross-Border Seller") || entriesCount >= 12;
}

function shouldRefreshIncompleteDetail(item) {
  return Boolean(item?.hydration_pending?.pending);
}

function clearDetailRefreshTimer() {
  if (detailRefreshTimer) {
    window.clearTimeout(detailRefreshTimer);
    detailRefreshTimer = null;
  }
}

function cacheDetailItem(caseId, item) {
  if (shouldCacheDetail(item)) {
    detailCache.set(caseId, {
      item,
      cachedAt: Date.now()
    });
    return;
  }

  detailCache.delete(caseId);
}

async function fetchCaseDetail(caseId, { force = false } = {}) {
  const cached = force ? null : getCachedDetail(caseId);
  if (cached) {
    return cached;
  }

  if (inflightDetailRequests.has(caseId)) {
    return inflightDetailRequests.get(caseId);
  }

  const requestPromise = request(`/api/cases/${caseId}`)
    .then((item) => {
      cacheDetailItem(caseId, item);
      return item;
    })
    .finally(() => {
      inflightDetailRequests.delete(caseId);
    });

  inflightDetailRequests.set(caseId, requestPromise);
  return requestPromise;
}

function prefetchCaseDetail(caseId) {
  if (!caseId || getCachedDetail(caseId) || inflightDetailRequests.has(caseId)) {
    return;
  }

  fetchCaseDetail(caseId).catch(() => {});
}

function scheduleIncompleteDetailRefresh(caseId, requestToken, attempt = 0) {
  const delays = [2500, 6000, 12000];
  if (attempt >= delays.length) {
    return;
  }

  clearDetailRefreshTimer();
  detailRefreshTimer = window.setTimeout(async () => {
    if (requestToken !== detailRequestToken || state.selectedCaseId !== caseId) {
      return;
    }

    try {
      detailCache.delete(caseId);
      const item = await fetchCaseDetail(caseId, { force: true });
      if (requestToken !== detailRequestToken || state.selectedCaseId !== caseId) {
        return;
      }

      renderDetail(item);
      if (shouldRefreshIncompleteDetail(item)) {
        scheduleIncompleteDetailRefresh(caseId, requestToken, attempt + 1);
      }
    } catch {
      scheduleIncompleteDetailRefresh(caseId, requestToken, attempt + 1);
    }
  }, delays[attempt]);
}

function prefetchVisibleCaseDetails(items = []) {
  items.slice(0, 3).forEach((item) => prefetchCaseDetail(item.id));
}

async function loadStatus() {
  const status = await request("/api/sync/status");
  renderHero(status);
}

async function loadCases({ autoSelectFirst = false, preserveSelection = true } = {}) {
  const requestToken = ++casesRequestToken;
  lookupInput.value = state.search;
  renderCasesLoading(state.search ? `Searching ${state.search} ...` : "Refreshing case list...");

  const params = new URLSearchParams({
    category: state.category,
    court: state.court,
    search: state.search,
    page: String(state.page),
    pageSize: String(state.pageSize)
  });

  const payload = await request(`/api/cases?${params.toString()}`);
  if (requestToken !== casesRequestToken) {
    return null;
  }

  if (autoSelectFirst || !preserveSelection) {
    state.selectedCaseId = null;
  }

  renderCases(payload);
  prefetchVisibleCaseDetails(payload.items || []);
  return payload;
}

async function loadCaseDetail(caseId, { summaryItem = null, focus = false, updateRoute = false } = {}) {
  const requestToken = ++detailRequestToken;
  clearDetailRefreshTimer();
  const cached = getCachedDetail(caseId);

  if (summaryItem) {
    renderDetailLoading(summaryItem);
  }

  if (focus) {
    jumpToDetailPanel();
  }

  if (updateRoute) {
    setCaseRoute(caseId);
  }

  if (cached) {
    if (requestToken === detailRequestToken) {
      renderDetail(cached);
    }
    return;
  }

  const item = await fetchCaseDetail(caseId);
  if (requestToken !== detailRequestToken) {
    return;
  }

  renderDetail(item);
  if (shouldRefreshIncompleteDetail(item)) {
    scheduleIncompleteDetailRefresh(caseId, requestToken);
  }
}

lookupForm.addEventListener("submit", (event) => {
  event.preventDefault();
  state.search = lookupInput.value.trim();
  state.page = 1;
  loadCases({
    autoSelectFirst: true,
    preserveSelection: false
  }).then((payload) => {
    if (payload) {
      revealResultsIfNeeded();
    }
  }).catch(console.error);
});

courtFilter.addEventListener("change", (event) => {
  state.court = String(event.target.value || "");
  state.page = 1;
  loadCases({
    autoSelectFirst: true,
    preserveSelection: false
  }).then((payload) => {
    if (payload) {
      revealResultsIfNeeded();
    }
  }).catch(console.error);
});

prevPageButton.addEventListener("click", () => {
  if (state.page <= 1) {
    return;
  }
  state.page -= 1;
  state.selectedCaseId = null;
  loadCases({
    autoSelectFirst: true,
    preserveSelection: false
  }).catch(console.error);
});

nextPageButton.addEventListener("click", () => {
  if (state.page >= state.pageCount) {
    return;
  }
  state.page += 1;
  state.selectedCaseId = null;
  loadCases({
    autoSelectFirst: true,
    preserveSelection: false
  }).catch(console.error);
});

if (refreshButton) {
  refreshButton.addEventListener("click", async () => {
    refreshButton.disabled = true;
    refreshButton.textContent = "Syncing...";
    try {
      await request("/api/admin/sync", {
        method: "POST",
        headers: {
          "content-type": "application/json"
        },
        body: JSON.stringify({ mode: "recent" })
      });
      await Promise.all([
        loadStatus(),
        loadCases({
          preserveSelection: true
        })
      ]);
    } finally {
      refreshButton.disabled = false;
      refreshButton.textContent = "Refresh Now";
    }
  });
}

if (copyWechatButton) {
  copyWechatButton.addEventListener("click", async () => {
    const defaultLabel = "Add WeChat for Group Settlement";

    try {
      await navigator.clipboard.writeText("mylearnedfriend");
      copyWechatButton.textContent = "WeChat ID Copied";
    } catch (error) {
      copyWechatButton.textContent = "WeChat: mylearnedfriend";
    }

    window.setTimeout(() => {
      copyWechatButton.textContent = defaultLabel;
    }, 1800);
  });
}

async function boot() {
  const routeCaseId = getRouteCaseId();
  if (routeCaseId) {
    state.selectedCaseId = routeCaseId;
    renderDetailLoading({
      docket_number: `Case #${routeCaseId}`,
      case_name: "Loading case details"
    });
  }

  await loadCases({
    autoSelectFirst: !routeCaseId,
    preserveSelection: Boolean(routeCaseId)
  });
  loadStatus().catch(console.error);

  if (routeCaseId) {
    const summaryItem = currentCasesPayload?.items?.find((item) => item.id === routeCaseId) || null;
    loadCaseDetail(routeCaseId, {
      summaryItem,
      focus: true
    }).catch(console.error);
  }

  window.setInterval(() => {
    loadStatus().catch(() => {});
  }, statusPollMs);
}

window.addEventListener("popstate", () => {
  const routeCaseId = getRouteCaseId();
  if (routeCaseId) {
    state.selectedCaseId = routeCaseId;
    updateActiveCaseRow();
    const summaryItem = currentCasesPayload?.items?.find((item) => item.id === routeCaseId) || null;
    loadCaseDetail(routeCaseId, {
      summaryItem,
      focus: true
    }).catch(console.error);
    return;
  }

  if (currentCasesPayload?.items?.length) {
    state.selectedCaseId = currentCasesPayload.items[0].id;
    updateActiveCaseRow();
    loadCaseDetail(state.selectedCaseId, {
      summaryItem: currentCasesPayload.items[0],
      focus: true
    }).catch(console.error);
  }
});

boot().catch((error) => {
  console.error(error);
  caseList.innerHTML = `<article class="case-row"><h3>Load Failed</h3><p>${error.message}</p></article>`;
});
