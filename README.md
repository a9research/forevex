# polymarket-pipeline

Rust 数据管线（**仅 Postgres**；Parquet 导出未实现，见 [`docs/polymarket-data-foundation-pma-core.md`](../docs/polymarket-data-foundation-pma-core.md) §12）。

**文档级目标（① 层 raw）**：以 **[jon-becker/prediction-market-analysis](https://github.com/jon-becker/prediction-market-analysis)（PMA）** 的 **Gamma + 链上 `OrderFilled` Parquet**（OSS 持久化）为 **唯一** raw 来源——**不**再使用 Goldsky / poly_data CSV 等旁路导入。流程：**Gamma / PMA markets** → **类目补全** → **`stg_order_filled`（PMA Parquet）** → **加工 `fact_trades`** → **钱包维表** → **Data API activity（可选）** → **官方 API 快照（可选）** → **聚合表**。

**本仓库实现**：链上成交 raw **仅 PMA Parquet**；**对象存储（阿里云 OSS 等 S3 兼容）为唯一持久 raw**，本地 `polymarket/**` 仅为解压或增量暂存，上传后会删除。

- [`docs/polymarket-data-foundation-pma-core.md`](../docs/polymarket-data-foundation-pma-core.md)（数据规格唯一来源）

## 依赖

- Rust 2021
- PostgreSQL（`DATABASE_URL`）

## 配置

复制 `.env.example` 为 `.env`，设置 `DATABASE_URL`。

| 变量 | 说明 |
|------|------|
| `DATABASE_URL` / `PIPELINE_DATABASE_URL` | Postgres 连接串 |
| `PIPELINE_DB_MAX_CONNECTIONS` | sqlx 池最大连接数，默认 `16`（曾硬编码为 5，大导入易 `pool timed out`） |
| `PIPELINE_DB_ACQUIRE_TIMEOUT_SEC` | 等待池里空闲连接的最长时间（秒），默认 `120` |
| `PIPELINE_GAMMA_ORIGIN` | 默认 `https://gamma-api.polymarket.com` |
| `PIPELINE_DATA_API_ORIGIN` | 默认 `https://data-api.polymarket.com` |
| `PIPELINE_PMA_DATA_DIR` | 解压/增量暂存根目录（含 `polymarket/…`），默认 `data`；**非权威**，以 OSS 为准 |
| `PIPELINE_PMA_ALLOW_LOCAL_ONLY` | 设为 `1` 时允许**不配 OSS**、仅从本地 Parquet 入库（仅开发） |
| `PIPELINE_BOOTSTRAP_DOWNLOAD_URL` | `bootstrap-data` 的 `data.tar.zst` 地址，默认 `https://s3.jbecker.dev/data.tar.zst` |
| `PIPELINE_OSS_*` / `PIPELINE_S3_*` | 见 `.env.example`：bucket、endpoint、AccessKey；配置后 **`ingest-pma` / `sync` 先上传本地 parquet 再删盘，再从 OSS 入库** |
| `PIPELINE_MARKETS_BATCH_SIZE` | 默认 `500` |
| `PIPELINE_DIM_MARKETS_SOURCE` | `gamma`（仅 Gamma HTTP）\|`pma_parquet`（仅 OSS `polymarket/markets/*.parquet`）\|`auto`（默认：OSS 上存在 markets Parquet 则走 PMA，否则 Gamma） |
| `PIPELINE_HTTP_TIMEOUT_SEC` | 默认 `120` |
| `PIPELINE_RATE_LIMIT_MS` | Gamma enrich / snapshot / activity 间隔（毫秒），默认 `120` |
| `PIPELINE_ACTIVITY_PROXIES` | 逗号分隔 `0x…`，`ingest-activities` 拉 `/activity` |
| `PIPELINE_SNAPSHOT_PROXIES` | 逗号分隔；为空则对 `dim_wallets` 最近活跃取 `PIPELINE_SNAPSHOT_MAX_WALLETS` 条 |
| `PIPELINE_SNAPSHOT_MAX_WALLETS` | 默认 `200` |
| `PIPELINE_USER_STATS_URL` | 默认 `https://data-api.polymarket.com/v1/user-stats` |
| `PIPELINE_USER_PNL_URL` | 默认 `https://user-pnl-api.polymarket.com/user-pnl` |
| `PIPELINE_GAMMA_PUBLIC_PROFILE_URL` | 默认 `https://gamma-api.polymarket.com/public-profile` |
| `PIPELINE_HTTP_BIND` | `serve` 子命令监听地址，默认 `0.0.0.0:8080` |

### 阿里云 OSS（S3 兼容）

在 RAM 创建 AccessKey，Region 与 Endpoint 与 Bucket 所在地域一致，例如华东 2（上海）：

- `PIPELINE_OSS_REGION=oss-cn-shanghai`
- `PIPELINE_OSS_ENDPOINT=https://oss-cn-shanghai.aliyuncs.com`
- `PIPELINE_OSS_BUCKET=<你的 Bucket 名>`
- `PIPELINE_OSS_ACCESS_KEY_ID` / `PIPELINE_OSS_ACCESS_KEY_SECRET`

若 SDK/网关要求 **虚拟主机样式**（`bucket.oss-…`），设 `PIPELINE_OSS_VIRTUAL_HOSTED=1`。键前缀可用 `PIPELINE_S3_PREFIX`（如 `prod`）。

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

**`max_connections`（Compose）**：`docker-compose.yml` 里 `postgres` 服务使用 `command: ["postgres", "-c", "max_connections=150"]` 覆盖服务端上限（官方镜像**没有**对应环境变量）。改数字后执行 `docker compose up -d postgres` 使配置生效。独立安装的 PostgreSQL 请在 `postgresql.conf` 或 `ALTER SYSTEM` 中设置。

## CLI

```bash
cargo run -- migrate
cargo run -- ingest-markets    # dim_markets（auto：OSS 有 markets Parquet 则增量文件级 checkpoint `ingest_markets_pma`；否则 Gamma offset `ingest_markets`）
cargo run -- status             # JSON：OSS 上 trades/blocks/markets 文件数、pma 待处理 trades 文件数、全部 etl_checkpoint
cargo run -- enrich-gamma       # category_raw / topic_primary
cargo run -- ingest-pma         # stg_order_filled（OSS 为 raw；本地 parquet 会先上传再删）
cargo run -- bootstrap-data      # 下载并解压；若已配 OSS bucket 则上传并删除本地 polymarket/
cargo run -- process-trades     # fact_trades
cargo run -- refresh-wallets    # dim_wallets
cargo run -- ingest-activities # fact_account_activities（需 PIPELINE_ACTIVITY_PROXIES）
cargo run -- snapshot-wallets   # wallet_api_snapshot
cargo run -- aggregate          # agg_wallet_topic + agg_global_daily
cargo run -- sync               # 增量拉取 + 加工 + 聚合（与 run-all 同顺序，但不含 migrate）
cargo run -- run-all            # migrate + 与 sync 相同步骤（不含 serve）
cargo run -- serve              # GET /health、GET /stats、GET /pipeline-status（与 `status` 子命令同源）
```

### `run-all` vs `sync`

| 命令 | migrate | 后续管线 |
|------|---------|----------|
| **`run-all`** | ✅ 执行 | ingest-markets → enrich-gamma → **ingest-pma** → process-trades → refresh-wallets → ingest-activities → snapshot-wallets → aggregate |
| **`sync`** | ❌ 不执行 | **同上**（首次请已跑过 `migrate` 且表已就绪） |

初始化或改表结构后：先 **`migrate`**（或 **`run-all`** 一次）。之后日常/定时「拉最新 + 跑完全部下游」用一条：

```bash
cargo run -- sync
```

生产环境须配置 OSS（见上文「阿里云 OSS」与 `.env.example`）。**`bootstrap-data`** 在已配 bucket 时解压后会自动上传并删除本地 `polymarket/`；对象存储内无法直接解压 tar，须先本机解压再上传。

## 数据处理流程与产出

### 流程总览（`sync` / `run-all` 中 migrate 之后的顺序）

| 步骤 | 子命令 | 输入来源 | 写入 / 更新 |
|------|--------|----------|-------------|
| 1 | `ingest-markets` | Gamma `GET /markets`（分页） | **`dim_markets`** |
| 2 | `enrich-gamma` | Gamma `GET /markets/{id}` | **`dim_markets`**（`category_raw`、`topic_primary`） |
| 3 | `ingest-pma` | **OSS** 上 `polymarket/**/*.parquet`（本地仅暂存上传） | **`stg_order_filled`** |
| 4 | `process-trades` | `stg_order_filled` + `dim_markets` | **`fact_trades`** |
| 5 | `refresh-wallets` | `fact_trades` | **`dim_wallets`**（`INSERT … ON CONFLICT` 合并首末次时间） |
| 6 | `ingest-activities` | Data API `/activity`（需 `PIPELINE_ACTIVITY_PROXIES`） | **`fact_account_activities`** |
| 7 | `snapshot-wallets` | user-stats / user-pnl / public-profile | **`wallet_api_snapshot`** |
| 8 | `aggregate` | `fact_trades` + `dim_markets` | **`agg_wallet_topic`**、**`agg_global_daily`**（全量重算） |

**初始化 raw（Parquet）**：`bootstrap-data` 下载 `data.tar.zst` 并解压到 `PIPELINE_PMA_DATA_DIR`；若已配 OSS，会把 **`polymarket/`** 上传后删除本地目录——**入库仍由 `ingest-pma` 在 `sync` 中完成**。

### 产出物

| 类型 | 说明 |
|------|------|
| **对象存储** | 权威链上 raw：`[PREFIX/]polymarket/blocks/*.parquet`、`…/trades/*.parquet`。 |
| **PostgreSQL** | 业务与分析表：`dim_markets`、`stg_order_filled`、`fact_trades`、`dim_wallets`、`fact_account_activities`、`wallet_api_snapshot`、`agg_*`；断点表 **`etl_checkpoint`**。 |
| **HTTP（`serve`）** | `GET /health`；`GET /stats` 返回上述核心表的**行数** JSON（观测用，非明细导出）。 |
| **Parquet 导出** | **未实现**；分析结果不落盘为 Parquet 文件（见绿场文档 P4）。 |

```text
Gamma HTTP ──────────────► dim_markets ──► enrich-gamma
OSS Parquet (PMA) ───────► stg_order_filled ──► fact_trades ──► dim_wallets / agg_*
Data API / 官方 API（可选）► fact_account_activities / wallet_api_snapshot
```

## 增量拉取与断点说明

各步骤如何「只处理新增 / 未处理部分」如下；断点均记在 **`etl_checkpoint`**（`pipeline` + `cursor_json`）。

| 步骤 | 增量方式 |
|------|----------|
| **`ingest-markets`** | 游标键 **`ingest_markets`**，`cursor_json.offset` 为 Gamma 列表的 **offset**。每批拉取后 `offset += len(batch)` 并保存；下次从该 offset 继续拉新市场。遇 429 会退避重试。 |
| **`enrich-gamma`** | 无单独 offset：每次查询 **`dim_markets`** 中 **`category_raw` 或 `topic_primary` 为空** 的 `market_id`，逐条调 `GET /markets/{id}` 补全；已补全的行不会再次选中。 |
| **`ingest-pma`** | 游标键 **`pma_order_filled`**，`cursor_json.last_completed_file` 为**已完整处理完的**最后一个 Parquet 的**对象键**（OSS）或相对路径（仅本地模式）。按路径排序后，跳过直到该文件为止，**从下一个文件起**继续写入 `stg_order_filled`。运行前若配置了 OSS：先把 **`PIPELINE_PMA_DATA_DIR` 下** 本地 **`polymarket/**/*.parquet` 全部上传 OSS** 并 **删除本地 `polymarket/`**，再仅按 OSS 列表入库——**增量 Parquet** 的典型做法：把新文件放到本地目录（或解压增量包）后跑 **`sync`**，会先合并进 OSS 再按文件断点续跑。 |
| **`process-trades`** | 游标键 **`process_trades`**，`cursor_json.last_stg_id` 为已处理到的 **`stg_order_filled.id` 上界**。每次只处理 **`id > last_stg_id`** 的行（每轮最多 5 万行），处理完后把游标更新为本轮最大 `id`。 |
| **`refresh-wallets`** | 无断点：对 **`fact_trades`** 中 maker/taker 做聚合 **`INSERT … ON CONFLICT DO UPDATE`**，合并 `first_seen_at` / `last_seen_at`，属于**幂等增量合并**。 |
| **`ingest-activities`** | 无时间游标：对每个代理拉 Data API **最近一页**（`limit=500`），插入前用 `(proxy, type, occurred_at, tx_hash)` 查重，**已存在则跳过**——重复跑不会大量重复插入。 |
| **`snapshot-wallets`** | 无断点：按配置代理或 `dim_wallets` 最近活跃钱包，**每次运行追加一批** `wallet_api_snapshot` 行（带 `fetched_at`）。 |
| **`aggregate`** | **全量重算**：先 **`DELETE`** 再 **`INSERT`** `agg_wallet_topic` / `agg_global_daily`，数据完全来自当前 **`fact_trades` + `dim_markets`**，无增量游标。 |

**小结**：`sync` 重复执行时，markets / PMA / process-trades 靠 **`etl_checkpoint`** 推进；enrich 靠「列为空」筛选；钱包维表靠 SQL 合并；活动与快照靠查重或追加；聚合表每次 **`sync` 末尾整表重算**，与最新 `fact_trades` 一致。

## 检查库内数据

**1）HTTP（需先起 `serve`）**

```bash
cargo run -- serve   # 另开终端
curl -s http://127.0.0.1:8080/stats | jq .
```

返回各核心表的 **行数** 摘要（`dim_markets`、`stg_order_filled`、`fact_trades` 等）。

**2）`psql` 看行数与样例**（连接串与 `.env` 一致）：

```bash
psql "postgres://postgres:postgres@127.0.0.1:5433/polymarket_pipeline" -c "
SELECT 'dim_markets' AS t, COUNT(*)::bigint AS n FROM dim_markets
UNION ALL SELECT 'stg_order_filled', COUNT(*) FROM stg_order_filled
UNION ALL SELECT 'fact_trades', COUNT(*) FROM fact_trades
UNION ALL SELECT 'dim_wallets', COUNT(*) FROM dim_wallets
UNION ALL SELECT 'agg_wallet_topic', COUNT(*) FROM agg_wallet_topic
ORDER BY 1;
"
```

查看最近几条成交：

```bash
psql "postgres://postgres:postgres@127.0.0.1:5433/polymarket_pipeline" -c "
SELECT ts, market_id, maker, usd_amount FROM fact_trades ORDER BY ts DESC LIMIT 5;
"
```

**3）图形客户端**：DBeaver / TablePlus / DataGrip 等，填 **`127.0.0.1`、端口 `5433`**、库 **`polymarket_pipeline`**、用户 **`postgres`**。

## 验证

```bash
cd forevex && cargo build && cargo test
```

端到端需有效 `DATABASE_URL` 与外网；`run-all` 会调用 Gamma / Data API 等 HTTP。

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

## 实现说明（与旧脚本对照）

- **Parquet 文件导出**：未实现；业务数据仅存 Postgres，Parquet 仅作 OSS raw。
- 增量与断点细节见上文 **「增量拉取与断点说明」**。
