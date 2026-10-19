import { DatabaseSync } from "node:sqlite";
import { Store } from "../src/db.js";
import { getWebhookEnrichmentQueueStats } from "../src/webhook-enrichment.js";

const db = new DatabaseSync(process.env.DB_PATH || "/app/data/tro-watch.sqlite", { readOnly: true });
const store = Object.create(Store.prototype);
store.db = db;
try {
  const cases = db.prepare(`SELECT id, docket_number, court_id, date_filed FROM cases
    WHERE is_seller_watch=1 AND date_filed <= date('now')
    ORDER BY date_filed DESC, id DESC LIMIT 20`).all();
  const reference = db.prepare("SELECT id,docket_number,court_id,date_filed FROM cases WHERE id=2585078").get();
  const coverage = store.getEntryCoverageForCaseIds([...cases.map(row => row.id), reference?.id].filter(Boolean));
  const queue = store.getCheckpoint("system:webhook-enrichment-queue") || {};
  const failureCodes = {};
  for (const job of [...(queue.pending || []), ...(queue.leased || [])]) {
    for (const failure of job.lastFailures || []) {
      const key = `${failure.provider}:${failure.code}`;
      failureCodes[key] = Number(failureCodes[key] || 0) + 1;
    }
  }
  const runs = db.prepare(`SELECT id,status,started_at,finished_at,stats_json FROM sync_runs
    WHERE mode='webhook-enrichment' ORDER BY id DESC LIMIT 8`).all().map(row => {
    const stats = JSON.parse(row.stats_json || "{}");
    return { id: row.id, status: row.status, startedAt: row.started_at, finishedAt: row.finished_at,
      caseId: stats.caseId, outcome: stats.outcome, requeued: stats.requeued,
      completedProviders: stats.completedProviders, retryAttempts: stats.retryAttempts,
      failures: (stats.failedProviders || []).map(({ provider, code, status, retryable, deferred }) => ({ provider, code, status, retryable, deferred })) };
  });
  const circuits = db.prepare("SELECT checkpoint_key,value_json FROM checkpoints WHERE checkpoint_key LIKE 'system:enrichment-provider:%'")
    .all().map(row => ({ provider: row.checkpoint_key.split(":").slice(2).join(":"), ...JSON.parse(row.value_json || "{}") }));
  console.log(JSON.stringify({ at: new Date().toISOString(),
    queue: getWebhookEnrichmentQueueStats(queue), failureCodes, circuits, runs,
    recentCases: cases.map(row => ({ ...row, coverage: coverage.get(row.id) })),
    reference: reference ? { ...reference, coverage: coverage.get(reference.id) } : null
  }, null, 2));
} finally { db.close(); }
