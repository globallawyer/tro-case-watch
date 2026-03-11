export class PacerMonitorAdapter {
  constructor(config) {
    this.enabled = Boolean(config.enabled);
    this.baseUrl = config.baseUrl || "";
    this.apiKey = config.apiKey || "";
  }

  getStatus() {
    if (!this.enabled) {
      return {
        enabled: false,
        state: "disabled",
        note: "默认关闭，避免在没有企业 API 资料时误抓取。"
      };
    }

    if (!this.baseUrl || !this.apiKey) {
      return {
        enabled: true,
        state: "blocked",
        note: "已打开开关，但缺少 PACERMonitor API base URL 或 API key。"
      };
    }

    return {
      enabled: true,
      state: "blocked",
      note: "适配器已预留，但需要你提供实际 API 文档或样例响应后才能安全接入。"
    };
  }

  async syncRecent() {
    return {
      provider: "pacermonitor",
      ...this.getStatus(),
      syncedCases: 0
    };
  }
}
