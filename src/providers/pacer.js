export class PacerAdapter {
  constructor(config, store) {
    this.config = config;
    this.store = store;
  }

  getStatus() {
    const usage = this.store.getProviderUsage("pacer");
    const remaining = Math.max(0, this.config.dailyBudgetUsd - usage.estimated_cost_usd);

    if (!this.config.enabled) {
      return {
        enabled: false,
        state: "disabled",
        remainingBudgetUsd: remaining,
        note: "默认关闭。直接从 PACER 批量拉全国 docket 很容易产生费用。"
      };
    }

    return {
      enabled: true,
      state: "manual-only",
      remainingBudgetUsd: remaining,
      note: "当前只实现了预算闸门和状态位，未自动化登录法院 CM/ECF。"
    };
  }

  async syncRecent() {
    return {
      provider: "pacer",
      ...this.getStatus(),
      syncedCases: 0
    };
  }
}
