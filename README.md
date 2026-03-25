# forevex

独立 Rust 服务：从 Polymarket 官方 HTTP API 拉取**用户资料 / 指标 / 持仓 / 市场 activity**，写入 **PostgreSQL**，并提供 **CLI** 与 **HTTP API**。位于本仓库子目录 **`account-analyzer/forevex/`**（相对 monorepo 根：`Forevex/account-analyzer/forevex`），**保留独立 `.git`**（嵌套仓库）。

> 上游为非保证稳定的公开接口；字段以实时 JSON 为准。Rust 官方客户端见 [Polymarket Clients & SDKs](https://docs.polymarket.com/api-reference/clients-sdks)。

## 快速开始

```bash
cp .env.example .env
docker compose up -d postgres
# 等待 healthy 后：
export DATABASE_URL=postgres://forevex:forevex@127.0.0.1:5433/forevex
cargo run -- serve
```

另一终端：

```bash
curl -s http://127.0.0.1:3000/health
# 创建/同步用户（201 Created）
curl -s -X POST http://127.0.0.1:3000/api/v1/users -H 'content-type: application/json' \
  -d '{"input":"@YatSen"}'   # 或 0x… 地址
curl -s http://127.0.0.1:3000/api/v1/users/0x你的proxy
curl -s -X POST http://127.0.0.1:3000/api/v1/users/0x…/positions/sync
curl -s 'http://127.0.0.1:3000/api/v1/users/0x…/positions?state=open'
# 同步某市场 activity（JSON body）
curl -s -X POST http://127.0.0.1:3000/api/v1/users/0x…/activity \
  -H 'content-type: application/json' -d '{"market":"<condition_id>"}'
# 读取缓存
curl -s 'http://127.0.0.1:3000/api/v1/users/0x…/activity?market=<condition_id>'
# 持仓聚合分析（需已 sync 持仓）
curl -s 'http://127.0.0.1:3000/api/v1/users/0x…/analytics/positions'
```

### HTTP API（REST，`/api/v1`）

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/health` | 健康检查 |
| `GET` | `/api/v1/meta` | 元信息 |
| `POST` | `/api/v1/users` | Body: `{"input":"@slug或0x…"}`，上游拉取并写入 DB → **201** |
| `GET` | `/api/v1/users/{proxy}` | 用户快照 JSON（含 **`positionsSyncedAt`**：至少成功做过一次 `POST …/positions/sync` 后有 ISO 时间，否则 `null`） |
| `GET` | `/api/v1/users/{proxy}/positions` | Query: `state=open` / `closed` |
| `POST` | `/api/v1/users/{proxy}/positions/sync` | 从 Data API 刷新持仓 |
| `GET` | `/api/v1/users/{proxy}/activity` | Query: `market=<condition_id>`，读缓存 |
| `POST` | `/api/v1/users/{proxy}/activity` | Body: `{"market":"<condition_id>"}`，同步 activity |
| `GET` | `/api/v1/users/{proxy}/analytics/positions` | 仅基于 **已缓存 open+closed 持仓** 的聚合（无 `/trades`）：已平仓胜率、按 `slug` 规则分类的胜率与金额/条数分布、**均价**价位桶、Yes/No 持仓条数比；需先 `POST …/positions/sync` |

错误响应：`{ "error": "…" }`，HTTP 状态码区分 400 / 404 / 502 等。

全栈 Compose（含 `forevex` 镜像）：

```bash
docker compose up --build
```

默认映射：**Postgres `5433`**（避免与本机其他 PG 冲突）、**API `3000`**。

## CLI

```bash
forevex serve
forevex sync user '@YatSen'
forevex sync user 0x…
forevex sync positions 0x…
forevex sync activity 0x… --market <condition_id>
```

## 配置

| 变量 | 说明 |
|------|------|
| `DATABASE_URL` / `FOREVEX_DATABASE_URL` | Postgres 连接串 |
| `FOREVEX_BIND` | 监听地址，默认 `0.0.0.0:3000` |
| `FOREVEX_PUBLIC_BASE_URL` | 对外域名/基址（元信息、文档用） |
| `FOREVEX_*_ORIGIN` / `FOREVEX_USER_STATS_URL` / `FOREVEX_USER_PNL_URL` | 上游 base |

启动时会自动执行 `migrations/`。v0.1 **无鉴权**；生产环境建议反代 + 后续再加 API Key/JWT。

## 数据模型（分层）

1. **`wallet_user_snapshot`**：`proxy` 主键；Gamma `public-profile`、Data `value` / `traded`、`v1/user-stats`、`user-pnl` 系列 JSON；**`positions_synced_at`** 在每次成功 `positions/sync` 后更新，用于客户端区分「从未拉过持仓」与「已拉过但 0 仓」。
2. **`positions`**：`open` / `closed`，每行 `position_key` + `raw` JSON。
3. **`market_activity_cache`**：按 `(proxy, market_condition_id)` 存 activity 数组。

`@slug` 解析：请求 Gamma `GET /public-profile?username=`（无 `@` 前缀），读取 `proxyWallet` 作为后续 Data API 的 `user`。

## 与 polymarket-account-analyzer 的区别

| 服务 | 典型 HTTP 路由 | 用途 |
|------|----------------|------|
| **forevex**（本目录） | `/health`、`/api/v1/users/...` 等 | 官方 API 同步进 Postgres，REST 读库 |
| **polymarket-account-analyzer** | `GET /analyze/:wallet`、`GET /position-activity/:wallet?market=` 等 | 深度分析报告、KPI、策略推断、PG 报告缓存 |

前端建议：**只连 forevex** 时**不要设置** **`NEXT_PUBLIC_API_BASE_URL`**（则不请求 `GET /analyze`），并配置 **`NEXT_PUBLIC_FOREVEX_URL`** 或 **`NEXT_PUBLIC_FOREVEX_USE_PROXY` + `FOREVEX_UPSTREAM_URL`**。若与分析器并行部署，再设置 **`NEXT_PUBLIC_API_BASE_URL`** 指向分析器；**`NEXT_PUBLIC_SKIP_ANALYZE=1`** 在已配基址时仅跳过拉报告。把 API 基址误指到 forevex 会得到 **`GET /analyze` 404**。

### 部署后仍见 `POST /api/v1/users` → 404？

**最常见原因：公网 `:3000` 上跑的不是 forevex，而是 polymarket-account-analyzer**（两者 compose 默认都映射 `3000`）。分析器没有 `/api/v1/...`，故统一 **404**。

在**服务器本机**执行（把 `127.0.0.1` 换成你实际监听地址）：

```bash
curl -sS http://127.0.0.1:3000/health
```

- **forevex**：响应为 **JSON**，形如 `{"ok":true,"service":"forevex"}`；且 `curl -sS http://127.0.0.1:3000/api/v1/meta` 应返回带 `apiVersion` 的 JSON。
- **polymarket-account-analyzer**：`/health` 为纯文本 **`ok`**（无 JSON）；无 `/api/v1/meta`。

处理：二选一占用 `3000`，或把 forevex 改绑 **`3001`**（改 `FOREVEX_BIND` + compose `ports` + 前端 `NEXT_PUBLIC_FOREVEX_URL`）。更新镜像后建议 **`docker compose build --no-cache`** 再 **`up -d`**，避免旧层缓存。

## 存储：`jsonb` vs 强类型列

当前实现以 **`jsonb` + 少量键列**（`proxy`、`state`、`position_key`、`market`）为主。

- **仅 jsonb**：上线快，上游加字段自动保留；查询/索引要靠 PostgreSQL JSON 算子，Rust 侧少结构保证。
- **强类型列**：每个字段对应 SQL 类型，迁移与编译期校验强，上游一变就要改代码和 migration。
- **推荐折中**（后续迭代）：高频筛选字段（如 `condition_id`、`asset`）抽列 + 全量 `payload jsonb` 保留原文。

## License

MIT OR Apache-2.0
