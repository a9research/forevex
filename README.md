# polymarket-pipeline

Rust 数据管线（**仅 Postgres**；Parquet 导出未实现，见绿场文档 P4）：**Gamma markets** → **类目补全** → **Goldsky `orderFilledEvents`（含 sticky `timestamp` + `id_gt` 分页）** → **加工写入 `fact_trades`** → **钱包维表** → **Data API activity（可选）** → **官方 API 快照（可选）** → **聚合表**，表结构与流程对齐 [poly_data](https://github.com/warproxxx/poly_data) 与文档：

- [`docs/polymarket-data-platform-unified.md`](../docs/polymarket-data-platform-unified.md)
- [`docs/polymarket-analytics-mvp-data-bus.md`](../docs/polymarket-analytics-mvp-data-bus.md)
- [`docs/polymarket-pipeline-rust-greenfield-plan.md`](../docs/polymarket-pipeline-rust-greenfield-plan.md)

## 依赖

- Rust 2021
- PostgreSQL（`DATABASE_URL`）

## 配置

复制 `.env.example` 为 `.env`，设置 `DATABASE_URL`。

| 变量 | 说明 |
|------|------|
| `DATABASE_URL` / `PIPELINE_DATABASE_URL` | Postgres 连接串 |
| `PIPELINE_GAMMA_ORIGIN` | 默认 `https://gamma-api.polymarket.com` |
| `PIPELINE_DATA_API_ORIGIN` | 默认 `https://data-api.polymarket.com` |
| `PIPELINE_GOLDSKY_GRAPHQL_URL` | Goldsky GraphQL 端点（与 poly_data 默认一致） |
| `PIPELINE_MARKETS_BATCH_SIZE` | 默认 `500` |
| `PIPELINE_HTTP_TIMEOUT_SEC` | 默认 `120` |
| `PIPELINE_RATE_LIMIT_MS` | Gamma enrich / snapshot / activity 间隔（毫秒），默认 `120` |
| `PIPELINE_ACTIVITY_PROXIES` | 逗号分隔 `0x…`，`ingest-activities` 拉 `/activity` |
| `PIPELINE_SNAPSHOT_PROXIES` | 逗号分隔；为空则对 `dim_wallets` 最近活跃取 `PIPELINE_SNAPSHOT_MAX_WALLETS` 条 |
| `PIPELINE_SNAPSHOT_MAX_WALLETS` | 默认 `200` |
| `PIPELINE_USER_STATS_URL` | 默认 `https://data-api.polymarket.com/v1/user-stats` |
| `PIPELINE_USER_PNL_URL` | 默认 `https://user-pnl-api.polymarket.com/user-pnl` |
| `PIPELINE_GAMMA_PUBLIC_PROFILE_URL` | 默认 `https://gamma-api.polymarket.com/public-profile` |
| `PIPELINE_HTTP_BIND` | `serve` 子命令监听地址，默认 `0.0.0.0:8080` |

## 本地开发：Docker 只跑 Postgres，Rust 用 `cargo` 调试

适合：**数据库用 Compose 固定版本/隔离**，**程序在本机 `cargo build` / `cargo run`**，改代码立刻重跑，不必每次构建镜像。

1. **只启动 Postgres**（不启动 `pipeline` 容器）：

```bash
cd forevex
docker compose up -d postgres
```

`docker compose` 会按服务名 **只拉起 `postgres`**，`pipeline` 不会启动。

2. **连接串**与 `docker-compose.yml` 里一致：映射 **`127.0.0.1:5433` → 容器 5432**，用户/密码/库 **`postgres` / `postgres` / `polymarket_pipeline`**。在项目根复制环境变量：

```bash
cp .env.example .env
# 确认 .env 中 DATABASE_URL 为：
# postgres://postgres:postgres@127.0.0.1:5433/polymarket_pipeline
```

`dotenvy` 会在 `cargo run` 时自动加载当前目录下的 `.env`（在 `forevex/` 下执行命令即可）。

3. **本机跑迁移与子命令**（示例）：

```bash
cargo run -- migrate
cargo run -- serve
# 调试日志
RUST_LOG=debug,polymarket_pipeline=debug cargo run -- serve
```

4. **停库**（数据仍在 volume 里）：

```bash
docker compose stop postgres
```

5. **连库工具**（任选）：`psql postgres://postgres:postgres@127.0.0.1:5433/polymarket_pipeline`，或 DBeaver / TablePlus 等填同一连接串。

**注意**：若本机 **5433 已被占用**，改 `docker-compose.yml` 里 `ports` 左侧端口，并同步改 `.env` 里的 `DATABASE_URL`。不要用 `host.docker.internal` 连接——那是容器内访问宿主机；本机 Rust 进程连 **`127.0.0.1:5433`** 即可。

## CLI

```bash
cargo run -- migrate
cargo run -- ingest-markets    # dim_markets，checkpoint: ingest_markets.offset
cargo run -- enrich-gamma       # category_raw / topic_primary
cargo run -- ingest-goldsky     # stg_order_filled（sticky 游标）
cargo run -- process-trades     # fact_trades
cargo run -- refresh-wallets    # dim_wallets
cargo run -- ingest-activities # fact_account_activities（需 PIPELINE_ACTIVITY_PROXIES）
cargo run -- snapshot-wallets   # wallet_api_snapshot
cargo run -- aggregate          # agg_wallet_topic + agg_global_daily
cargo run -- import-order-filled-snapshot /path/to/orderFilled_complete.csv.xz  # poly_data 历史快照 → stg_order_filled
cargo run -- run-all            # migrate + 上述顺序（不含 serve）
cargo run -- serve              # GET /health、GET /stats（需 DB）
```

## 验证

```bash
cd forevex && cargo build && cargo test
```

端到端需有效 `DATABASE_URL` 与外网；`run-all` 会调用官方与 Goldsky HTTP。

## poly_data 历史快照（Quick Download）

poly_data 提供 **完整历史 `orderFilled` CSV**（xz 压缩），可与本管线 **`stg_order_filled`** 对齐：

- **下载**（示例）：[orderFilled_complete.csv.xz](https://polydata-archive.s3.us-east-1.amazonaws.com/orderFilled_complete.csv.xz)  
  文件名：`orderFilled_complete.csv.xz`

**导入**（写入 `stg_order_filled`，与 Goldsky 增量游标共用唯一约束；重复行自动跳过）：

```bash
# 本地：直接指向下载文件（支持 .csv 或 .csv.xz）
cargo run --release -- import-order-filled-snapshot /path/to/orderFilled_complete.csv.xz
```

导入结束后，默认会把 **`etl_checkpoint`** 里 **`goldsky_order_filled`** 的游标设为当前表中 **最大 `timestamp_i64`**，这样后续 **`ingest-goldsky`** 只从该时间戳之后增量拉取。若不想改游标（仅做离线装载测试）：

```bash
cargo run --release -- import-order-filled-snapshot /path/to/orderFilled_complete.csv.xz --no-update-checkpoint
```

**已解压的 CSV** 或 **管道**（stdin 仅支持**明文 CSV**，不支持 xz 流）：

```bash
xzcat orderFilled_complete.csv.xz | cargo run --release -- import-order-filled-snapshot -
```

导入后请按需继续：**`process-trades`**（→ `fact_trades`）及下游 **`refresh-wallets` / `aggregate`** 等（与 `run-all` 中顺序一致）。

**Docker**（把文件放到例如 `./data/orderFilled_complete.csv.xz`）：

```bash
docker compose run --rm -v "$(pwd)/data:/data:ro" pipeline import-order-filled-snapshot /data/orderFilled_complete.csv.xz
```

## Docker Compose

在 **`forevex/`** 目录（本 crate 根）使用 **`docker-compose.yml`**：

```bash
cd forevex
docker compose up -d --build
```

- **Postgres**：宿主机 **`127.0.0.1:5433`** → 容器内 `5432`，库名 **`polymarket_pipeline`**（用户/密码见 compose 文件，生产请修改）。
- **pipeline 默认命令**：镜像入口会先执行 **`migrate`**，再启动 **`serve`**，HTTP **`127.0.0.1:8080`**（`GET /health`、`GET /stats`）。
- **一次性全量同步**（需外网，耗时可能较长）：

```bash
docker compose run --rm pipeline run-all
```

- **仅执行迁移**：`docker compose run --rm pipeline migrate`（与启动时的 migrate 等价，可单独调试）。

可在 `docker-compose.yml` 的 `pipeline.environment` 中追加 `PIPELINE_*`（或挂载 env 文件），与 `.env.example` 一致。

## 与 poly_data 的差异（已知）

- **Parquet / 文件导出**：未实现；数据仅存 Postgres。
- **ingest-markets 游标**：使用 `etl_checkpoint` 的 `offset`，与 poly_data「从 CSV 尾部恢复」等价语义不同，但可断点续跑。
