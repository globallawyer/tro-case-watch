# TRO Case Watch

一个面向跨境电商 TRO / Schedule A 场景的案件监控站点原型。当前版本采用双源策略：

- `CourtListener` 负责全国发现、回填和低成本实时更新
- `WorldTRO` 公开页面负责补品牌、原告律所和更完整的卖家时间线

当前版本已经实现：

- 以 `CourtListener search` 为主数据源，检索 `2025-01-01` 以来的 TRO / Schedule A 案件
- 以 `WorldTRO` 公开页面为补充数据源，补品牌、原告律所、公开 docket 时间线
- 本地 `SQLite` 去重、缓存、分页查询
- 前端案件列表、详情页、docket 时间线
- 中文翻译缓存，避免重复翻译同一段案件标题或 docket 文本
- `PACER` 预算闸门和 `PACERMonitor` / `PACER` 适配器占位
- 启动即同步，并支持每 10 分钟轮询
- 支持后台持续 `backfill`，逐步补齐 2025 年以来历史案件
- 支持 `WorldTRO` 单案懒加载补源，以及批量 `worldtro backfill`

## 当前实测状态

- 已接入一个可用的 `CourtListener API token`
- 该 token 当前实测结果：
  - `search` 可用
  - `dockets` 可用
  - `docket-entries` 当前返回 `403`
- 这意味着：
  - 可以稳定发现新案
  - 可以刷新案件级元数据，例如 `date_last_filing`
  - 但还不能从 CourtListener 直接拉到完整逐条 docket entries
- 当前本地库已经回填出一批 `2025-01-01` 以来的 TRO / Schedule A 案件，后续可继续跑 `backfill`

## 本地启动

```bash
cd /Users/serendipitypku/Documents/Playground/tro-case-watch
cp .env.example .env
npm start
```

打开 `http://localhost:4127`。

## Oracle Cloud Always Free 部署

这个项目依赖常驻 Node 进程、定时同步和本地 `SQLite`，不适合部署到会休眠或临时磁盘的免费 PaaS。当前仓库已经带好一套适合 `Oracle Cloud Always Free VM` 的部署文件：

- `Dockerfile`
- `deploy/oci/compose.yml`
- `deploy/oci/Caddyfile`
- `deploy/oci/install-docker.sh`
- `deploy/oci/deploy.sh`

### 1. 准备一台 Oracle 免费虚机

- 建议选择 `Ubuntu` 镜像
- 公网开放 `22`、`80`、`443`
- 把这整个项目目录传到服务器，例如 `/opt/tro-case-watch`

示例：

```bash
rsync -avz /Users/serendipitypku/Documents/Playground/tro-case-watch/ ubuntu@YOUR_VM_IP:/opt/tro-case-watch/
```

### 2. 准备生产环境变量

登录服务器后：

```bash
cd /opt/tro-case-watch
cp .env.example .env
```

至少填这些值：

- `COURTLISTENER_API_TOKEN`
- `ADMIN_TOKEN`

如果你暂时不做翻译，可把：

- `OPENAI_API_KEY=`

保持为空。

### 3. 安装 Docker

```bash
cd /opt/tro-case-watch
bash deploy/oci/install-docker.sh
```

脚本执行后重新登录一次服务器，让 `docker` 用户组生效。

### 4. 启动站点

```bash
cd /opt/tro-case-watch
bash deploy/oci/deploy.sh
```

这会启动两部分：

- `app`
  - Node 服务，监听容器内 `4127`
- `caddy`
  - 对外监听 `80/443`
  - 自动申请 HTTPS 证书
  - 把 `trotracker.com` 重定向到 `www.trotracker.com`

### 5. 配置域名解析

当前 `Caddyfile` 已经按以下域名写死：

- `www.trotracker.com`
- `trotracker.com`

DNS 建议这样配：

- `A` 记录：`trotracker.com -> YOUR_VM_PUBLIC_IP`
- `A` 记录：`www.trotracker.com -> YOUR_VM_PUBLIC_IP`

如果你的 DNS 面板更习惯 `CNAME`，也可以：

- `A` 记录：`trotracker.com -> YOUR_VM_PUBLIC_IP`
- `CNAME` 记录：`www -> trotracker.com`

### 6. 验证

```bash
curl http://YOUR_VM_PUBLIC_IP/api/health
curl https://www.trotracker.com/api/health
docker compose -f deploy/oci/compose.yml ps
```

看日志：

```bash
docker compose -f deploy/oci/compose.yml logs -f app
docker compose -f deploy/oci/compose.yml logs -f caddy
```

## 环境变量

### 必填或强烈建议

- `COURTLISTENER_API_TOKEN`
  - 没有 token 时，仍可抓 `search` 结果。
  - 但如果你要抓完整 docket / docket-entries，这个 token 基本是必需的。
- `OPENAI_API_KEY`
  - 用于把案件标题和 docket 文本翻译成中文。
  - 代码里做了翻译缓存，相同文本不会重复调用。

### 可选

- `COURTLISTENER_ENABLE_DOCKET_SYNC=true`
  - 只有在你确认 token 有 `dockets` / `docket-entries` 访问权限时再打开。
- `WORLDTRO_ENABLED=true`
  - 默认已打开，使用公开页面补源。
- `WORLDTRO_MIN_INTERVAL_MS=1500`
  - 两次公开页面请求的最小间隔，避免对第三方站点造成太大压力。
- `WORLDTRO_TIMEOUT_MS=15000`
  - 单次公开页面请求超时，避免批量补源卡死。
- `WORLDTRO_MAX_CASES_PER_RUN=3`
- `WORLDTRO_BACKFILL_MAX_CASES_PER_RUN=12`
  - 控制每轮公开补源的案件数。
- `PACERMONITOR_ENABLED=true`
- `PACERMONITOR_API_BASE_URL=...`
- `PACERMONITOR_API_KEY=...`
  - 这部分我先留了适配器接口。你给我真实 API 文档或样例响应后，我就能继续接。
- `PACER_ENABLED=true`
  - 当前只建议在小范围、明确预算、按案号定向抓取的模式下启用。

## 已知边界

- `CourtListener search` 是当前最稳的全国发现入口。
- `WorldTRO` 不是官方 API，而是公开 HTML 页面补源。
- `CourtListener dockets` / `docket-entries` 不是所有账户都能直接拿到。
- `PACERMonitor` 没有公开稳定文档时，不建议直接猜接口。
- 直接从 `PACER / CM-ECF` 自动化全国轮询，费用和维护风险都很高，所以这版默认不打开。
- `WorldTRO` 如果改版 HTML 结构，解析器需要跟着更新。

## 常用命令

```bash
npm start
npm run sync:recent
npm run sync:backfill
npm run sync:worldtro
```

- `sync:worldtro`
  - 单独跑一轮 `WorldTRO` 历史补源，不动 PACER。

## 我建议你准备的资源

- `CourtListener` 账号和 API token
- 一个中文翻译 provider 的 API key
  - 当前代码默认接 `OpenAI`
- 如果你坚持接 `PACERMonitor`
  - 企业 API 文档
  - base URL
  - key 或账号认证方式
- 如果你坚持直接接 `PACER`
  - PACER 账户
  - client code
  - 是否启用 MFA
  - 允许接入的法院范围
  - 你的日预算上限

## 成本建议

- 主链路用 `CourtListener`，不要用 `PACER` 做全国 10 分钟轮询。
- `PACER` 只对以下情况补抓：
  - CourtListener 没抓到的新案
  - 某些重点法院的重点案件
  - 律师手动点名要追的案号
- 中文翻译只翻新文本，并做缓存。
- 大规模历史回填先跑 `CourtListener`，再挑高价值案件补 PACER。

## PACER 接入策略

- 如果后续真的需要从 PACER 拉数据，优先考虑 `CourtListener recap-fetch`
  - 好处是不用自己维护脆弱的法院站点自动化流程
  - 风险是它仍然会用你的 PACER 凭据去购买数据，所以 PACER 账单照样会产生
- 只有在以下条件同时满足时，才建议启用：
  - 你已经给出日预算或单次预算
  - 只抓指定案号、指定法院或指定日期范围
  - 默认关闭 `parties and counsel`
  - 只取自上次检查后的增量 docket
- 开发和联调用 `PACER QA`，不要先在生产 PACER 上试脚本
