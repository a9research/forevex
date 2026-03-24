# forevex

独立 Rust 服务：从 Polymarket 官方 HTTP API 拉取**用户资料 / 指标 / 持仓 / 市场 activity**，写入 **PostgreSQL**，并提供 **CLI** 与 **HTTP API**。与仓库 [`account-analyzer`](../account-analyzer) 平级，**单独 git 仓库**。

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
curl -s http://127.0.0.1:8080/health
curl -s -X POST http://127.0.0.1:8080/v1/users/sync -H 'content-type: application/json' \
  -d '{"input":"@YatSen"}'   # 或 0x… 地址
curl -s http://127.0.0.1:8080/v1/users/0x你的proxy
curl -s -X POST http://127.0.0.1:8080/v1/wallets/0x…/positions/sync
curl -s 'http://127.0.0.1:8080/v1/wallets/0x…/positions?state=open'
curl -s -X POST 'http://127.0.0.1:8080/v1/wallets/0x…/activity?market=条件ID'
curl -s 'http://127.0.0.1:8080/v1/wallets/0x…/activity?market=条件ID'
```

全栈 Compose（含 `forevex` 镜像）：

```bash
docker compose up --build
```

默认映射：**Postgres `5433`**（避免与本机其他 PG 冲突）、**API `8088`**。

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
| `FOREVEX_BIND` | 监听地址，默认 `0.0.0.0:8080` |
| `FOREVEX_PUBLIC_BASE_URL` | 对外域名/基址（元信息、文档用） |
| `FOREVEX_*_ORIGIN` / `FOREVEX_USER_STATS_URL` / `FOREVEX_USER_PNL_URL` | 上游 base |

启动时会自动执行 `migrations/`。v0.1 **无鉴权**；生产环境建议反代 + 后续再加 API Key/JWT。

## 数据模型（分层）

1. **`wallet_user_snapshot`**：`proxy` 主键；Gamma `public-profile`、Data `value` / `traded`、`v1/user-stats`、`user-pnl` 系列 JSON。
2. **`positions`**：`open` / `closed`，每行 `position_key` + `raw` JSON。
3. **`market_activity_cache`**：按 `(proxy, market_condition_id)` 存 activity 数组。

`@slug` 解析：请求 Gamma `GET /public-profile?username=`（无 `@` 前缀），读取 `proxyWallet` 作为后续 Data API 的 `user`。

## 存储：`jsonb` vs 强类型列

当前实现以 **`jsonb` + 少量键列**（`proxy`、`state`、`position_key`、`market`）为主。

- **仅 jsonb**：上线快，上游加字段自动保留；查询/索引要靠 PostgreSQL JSON 算子，Rust 侧少结构保证。
- **强类型列**：每个字段对应 SQL 类型，迁移与编译期校验强，上游一变就要改代码和 migration。
- **推荐折中**（后续迭代）：高频筛选字段（如 `condition_id`、`asset`）抽列 + 全量 `payload jsonb` 保留原文。

## License

MIT OR Apache-2.0
