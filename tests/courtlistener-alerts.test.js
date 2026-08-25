import test from "node:test";
import assert from "node:assert/strict";

import { CourtListenerClient, extractCourtListenerWebhookDocketId } from "../src/providers/courtlistener.js";
import { CaseSyncService } from "../src/sync.js";

test("extractCourtListenerWebhookDocketId accepts integer docket ids from webhook payloads", () => {
  assert.equal(extractCourtListenerWebhookDocketId({ docket: 4214664 }), "4214664");
  assert.equal(extractCourtListenerWebhookDocketId({ docket_id: 4214664 }), "4214664");
  assert.equal(
    extractCourtListenerWebhookDocketId({ docket: "https://www.courtlistener.com/api/rest/v4/dockets/4214664/" }),
    "4214664"
  );
  assert.equal(extractCourtListenerWebhookDocketId({ docket: null }), null);
});

test("CourtListenerClient can create and re-up docket alerts via the official API", async () => {
  const client = new CourtListenerClient({
    baseUrl: "https://www.courtlistener.com/api/rest/v4",
    apiToken: "token",
    enableDocketSync: true,
    enableDocketAlerts: true,
    recapFetchEnabled: false
  });

  const calls = [];
  client.fetchJson = async (url, options = {}) => {
    calls.push({
      url,
      method: options.method,
      body: options.body ? options.body.toString() : null,
      contentType: options.contentType
    });

    if (options.method === "POST") {
      return { id: 9, docket: 4214664, alert_type: 1 };
    }

    if (options.method === "PATCH") {
      return { id: 9, docket: 4214664, alert_type: 1 };
    }

    throw new Error("Unexpected request");
  };

  const created = await client.createDocketAlert(4214664);
  const reupped = await client.updateDocketAlert(9, { alertType: 1 });

  assert.equal(created.id, 9);
  assert.equal(reupped.id, 9);
  assert.deepEqual(calls, [
    {
      url: "https://www.courtlistener.com/api/rest/v4/docket-alerts/",
      method: "POST",
      body: "docket=4214664",
      contentType: "application/x-www-form-urlencoded; charset=utf-8"
    },
    {
      url: "https://www.courtlistener.com/api/rest/v4/docket-alerts/9/",
      method: "PATCH",
      body: "alert_type=1",
      contentType: "application/x-www-form-urlencoded; charset=utf-8"
    }
  ]);
});

test("docket alert subscription stops the batch after the first rate limit", async () => {
  const rows = [1, 2, 3].map((id) => ({
    id,
    courtlistener_docket_id: 4200000 + id,
    raw: {}
  }));
  let calls = 0;
  const service = new CaseSyncService({
    config: {
      sync: { startDate: "2026-01-01" },
      courtListener: {
        docketAlertSyncMaxCasesPerRun: 3,
        docketAlertSyncStaleAfterHours: 24
      }
    },
    store: {
      getCasesNeedingCourtListenerAlertSync() {
        return rows;
      }
    },
    courtListener: {
      hasDocketAlertAccess() {
        return true;
      },
      async getDocketAlertsByDocket() {
        calls += 1;
        const error = new Error("CourtListener rate limit cooldown active");
        error.status = 429;
        throw error;
      }
    }
  });

  const result = await service.syncCourtListenerAlertSubscriptions();

  assert.equal(calls, 1);
  assert.equal(result.failedCases, 1);
  assert.equal(result.deferredCases, 2);
  assert.equal(result.rateLimited, true);
});
