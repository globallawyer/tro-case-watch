import crypto from "node:crypto";

function extractJson(text) {
  const trimmed = String(text || "").trim();
  const fenced = trimmed.match(/```(?:json)?\s*([\s\S]*?)```/i);
  return fenced ? fenced[1].trim() : trimmed;
}

function makeCacheKey(provider, text) {
  return crypto.createHash("sha256").update(`${provider}:${text}`).digest("hex");
}

export class TranslationService {
  constructor(config, store) {
    this.provider = config.provider;
    this.baseUrl = config.baseUrl.replace(/\/$/, "");
    this.apiKey = config.apiKey;
    this.model = config.model;
    this.batchLimit = Math.max(1, config.batchLimit || 20);
    this.store = store;
  }

  isEnabled() {
    return this.provider === "openai" && Boolean(this.apiKey);
  }

  async translatePending() {
    if (!this.isEnabled()) {
      return {
        enabled: false,
        translated: 0
      };
    }

    const caseRows = this.store.getPendingCaseTranslations(Math.max(2, Math.floor(this.batchLimit / 2)));
    const entryRows = this.store.getPendingEntryTranslations(this.batchLimit);
    const items = [];

    for (const row of caseRows) {
      if (row.case_name && !row.case_name_zh) {
        items.push({ kind: "case_name", id: row.id, text: row.case_name });
      }

      if (row.recent_activity_summary && !row.recent_activity_summary_zh) {
        items.push({ kind: "summary", id: row.id, text: row.recent_activity_summary });
      }
    }

    for (const row of entryRows) {
      items.push({ kind: "docket_entry", id: row.id, text: row.description });
    }

    let translated = 0;
    const unresolved = [];

    for (const item of items) {
      const cacheKey = makeCacheKey(this.provider, item.text);
      const cached = this.store.getTranslation(cacheKey);

      if (cached) {
        this.applyItemTranslation(item, cached);
        translated += 1;
      } else {
        unresolved.push({ ...item, cacheKey });
      }
    }

    for (let index = 0; index < unresolved.length; index += 8) {
      const batch = unresolved.slice(index, index + 8);
      const results = await this.translateBatch(batch);

      for (const result of results) {
        const item = batch.find((candidate) => candidate.clientId === result.id || candidate.cacheKey === result.id);
        const batchItem = item || batch.find((candidate) => candidate.text === result.sourceText);
        if (!batchItem || !result.translation) {
          continue;
        }

        this.store.saveTranslation(batchItem.cacheKey, this.provider, batchItem.text, result.translation);
        this.applyItemTranslation(batchItem, result.translation);
        translated += 1;
      }
    }

    return {
      enabled: true,
      translated
    };
  }

  applyItemTranslation(item, translation) {
    if (item.kind === "case_name") {
      this.store.updateCaseTranslations(item.id, { case_name_zh: translation });
      return;
    }

    if (item.kind === "summary") {
      this.store.updateCaseTranslations(item.id, { recent_activity_summary_zh: translation });
      return;
    }

    if (item.kind === "docket_entry") {
      this.store.updateEntryTranslation(item.id, translation);
    }
  }

  async translateBatch(batch) {
    const payload = batch.map((item) => ({
      id: item.cacheKey,
      text: item.text
    }));

    const response = await fetch(`${this.baseUrl}/chat/completions`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${this.apiKey}`
      },
      body: JSON.stringify({
        model: this.model,
        temperature: 0,
        response_format: { type: "json_object" },
        messages: [
          {
            role: "system",
            content:
              "Translate U.S. federal case captions and docket text into concise Simplified Chinese. Preserve case numbers, party names, dates, money amounts, exhibit numbers, and document titles. Return JSON only with the shape {\"translations\":[{\"id\":\"...\",\"translation\":\"...\"}]}."
          },
          {
            role: "user",
            content: JSON.stringify(payload)
          }
        ]
      })
    });

    const bodyText = await response.text();
    if (!response.ok) {
      const error = new Error(`Translation failed: ${response.status}`);
      error.body = bodyText;
      throw error;
    }

    const json = JSON.parse(extractJson(JSON.parse(bodyText).choices?.[0]?.message?.content || "{}"));
    return Array.isArray(json.translations) ? json.translations : [];
  }
}
