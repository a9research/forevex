# polymarket-pipeline

Rust 数据管线（加工结果落 **Postgres**；raw 层 Parquet 落 **OSS**。PG → Parquet 的“导出”仍未实现，见 [`docs/polymarket-data-foundation-pma-core.md`](../docs/polymarket-data-foundation-pma-core.md) §12）。

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
| `PIPELINE_SKIP_ENRICH_GAMMA` | `1` 时跳过 `enrich-gamma`（不逐条调 Gamma `/markets/{id}` 补 `category_raw/topic_primary`）；默认 `0` |
| `PIPELINE_ENRICH_GAMMA_BATCH_SIZE` | enrich-gamma 每次最多处理多少个 market（默认 `1000`；用于 `sync/run-all` 增量推进） |
| `PIPELINE_ENRICH_GAMMA_FAIL_BACKOFF_SEC` | enrich-gamma 失败退避基准秒（默认 `300`，按 attempts 指数增长，用于 429/网络错误重试） |
| `PIPELINE_POLYGON_RPC_URLS` | **PMA 对齐 indexer** 使用：逗号分隔多个 Polygon JSON-RPC URL（失败/限流会切换 + 退避重试） |
| `PIPELINE_POLYGON_RPC_MAX_RETRIES` | JSON-RPC 每次请求最大重试次数（含切换节点），默认 `8` |
| `PIPELINE_POLYGON_RPC_BACKOFF_MS` | JSON-RPC 退避基准毫秒数（指数放大），默认 `250` |
| `PIPELINE_POLYMARKET_EXCHANGE_ADDRESS` | trades 同步用：`eth_getLogs` 的合约地址过滤（逗号分隔多个 `0x...`；PMA 默认两份：CTF + NegRisk） |
| `PIPELINE_POLYMARKET_ORDER_FILLED_TOPIC0` | trades 同步用：`OrderFilled` 事件 topic0（PMA：`0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6`） |
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
cargo run -- oss-apple-double   # 报告 OSS 上 macOS `._*.parquet` 旁路是否与真实文件成对（不需 Postgres）
cargo run -- oss-apple-double --delete   # 删除所有 `._*.parquet`（建议先跑上一行看 JSON）
cargo run -- enrich-gamma       # category_raw / topic_primary
cargo run -- ingest-pma         # stg_order_filled（OSS 为 raw；本地 parquet 会先上传再删）
cargo run -- bootstrap-data      # 下载并解压；若已配 OSS bucket 则上传并删除本地 polymarket/
cargo run -- indexer-oss         # Polygon RPC → OSS Parquet（blocks/trades）+ OSS cursor（不需 Postgres）
cargo run -- indexer-oss --blocks   # 只同步 blocks（写 polymarket/blocks/*.parquet）
cargo run -- indexer-oss --trades   # 只同步 trades（写 polymarket/trades/*.parquet；需 EXCHANGE_ADDRESS + TOPIC0）
cargo run -- sync               # 增量跑通：会把 fact_trades 落 OSS Parquet（不写 PG fact_trades）
cargo run -- process-trades     # fact_trades
cargo run -- refresh-wallets    # dim_wallets
cargo run -- ingest-activities # fact_account_activities（需 PIPELINE_ACTIVITY_PROXIES）
cargo run -- snapshot-wallets   # wallet_api_snapshot
cargo run -- aggregate          # agg_wallet_topic + agg_global_daily
cargo run -- sync               # 增量拉取 + 加工 + 聚合（与 run-all 同顺序，但不含 migrate；fact_trades 写 OSS）
cargo run -- run-all            # migrate + 与 sync 相同步骤（不含 serve）
cargo run -- serve              # GET /health、GET /stats、GET /pipeline-status（与 `status` 子命令同源）
cargo run -- cleanup-stg        # TRUNCATE stg_order_filled（释放磁盘；确认已切到 OSS 直读或下游已完成后再用）
```

## 完整执行步骤（从初始化到增量）

以下步骤假设：你已经把 PMA 初始数据上传到 OSS（`polymarket/{trades,blocks,markets}/` 至少包含 `trades` + `blocks`）。

### 0. 清空 PostgreSQL（按你的需求二选一）

方案 A（最干净，删掉 volume）：
```bash
cd forevex
docker compose down -v
docker compose up -d postgres
```

方案 B（保留 volume 但重建库）：
> 按你自己的数据库管理方式删除 `polymarket_pipeline` 内现有表数据/重建库。
>
> 目标是：跑 `cargo run -- migrate` 后应处于“空库/无旧 checkpoint”状态。

### 1. 配置环境变量

在 `forevex/.env` 中确保：
1) `DATABASE_URL` 指向你当前清空后的 Postgres 库  
2) `PIPELINE_OSS_BUCKET` / `PIPELINE_OSS_REGION` / `PIPELINE_OSS_ENDPOINT` / AK/SK 已配置好  
3) 选择 markets 来源（影响 `ingest-markets`）：
```text
PIPELINE_DIM_MARKETS_SOURCE=auto
# auto: 若 OSS 存在 polymarket/markets/*.parquet（忽略 ._ 旁路），则用 OSS；否则用 Gamma HTTP
```

4) 选择 trades 处理策略（默认推荐 `pma_oss`，可显著降低 PG 磁盘占用）：
```text
PIPELINE_TRADES_PROCESSOR=pma_oss
# pma_oss: OSS PMA Parquet → PG fact_trades（跳过 PG stg_order_filled）
# pg_stg : OSS PMA Parquet → PG stg_order_filled → PG fact_trades（旧路径，耗磁盘）
```

5) （可选）跳过 enrich-gamma（当你不关心 `category_raw/topic_primary` 或 Gamma 太慢/限流时）：
```text
PIPELINE_SKIP_ENRICH_GAMMA=1
```

### 2. （可选）清理 OSS 上 macOS `._*.parquet` 旁路文件

先报告（建议你确认已完全成对后再删）：
```bash
cargo run -- oss-apple-double
```

确认 `apple_double_without_counterpart` 为空或你已确认可删后执行：
```bash
cargo run -- oss-apple-double --delete
```

### 3. 首次初始化：建表 + 全量跑通

```bash
cd forevex

cargo run -- migrate
cargo run -- run-all
```

首次执行会写入：
- `dim_markets`（由 `ingest-markets` 写入：OSS Parquet 或 Gamma HTTP）
- `stg_order_filled`（仅在 `PIPELINE_TRADES_PROCESSOR=pg_stg` 时写入；`pma_oss` 默认不写）
- `fact_trades` / `dim_wallets` / `fact_account_activities`（按配置可有/可空）
- `wallet_api_snapshot` / `agg_*`
- `etl_checkpoint`（用于后续 `sync` 的增量续跑）

### 6. 释放磁盘（仅当需要）

当你已确认 `fact_trades` 从 OSS 正常生成、且不再需要 PG staging 时，可以清空 `stg_order_filled`：

```bash
cargo run -- cleanup-stg
```

验证（应为 0）：

```bash
psql \"postgres://postgres:postgres@127.0.0.1:5433/polymarket_pipeline\" -c \"SELECT COUNT(*) FROM stg_order_filled;\"
```

### 4. 判断你这台机器上 `ingest-markets` 会走 OSS 还是 Gamma（可直接照抄）

跑一次：
```bash
cargo run -- status
```

看输出里的 `pma.oss_markets_parquet_files`：
- 若 `pma.oss_markets_parquet_files > 0`，且 `PIPELINE_DIM_MARKETS_SOURCE=auto`：`ingest-markets` 会从 **OSS `polymarket/markets/*.parquet`** 增量灌库
- 若 `pma.oss_markets_parquet_files == 0`：`ingest-markets` 会回退到 **Gamma HTTP**（checkpoint：`ingest_markets`）

### 5. 日常增量：不重建表，直接续跑

```bash
cd forevex
cargo run -- sync
```

`sync` 会按固定顺序执行（不含 `migrate`）：
`ingest-markets → enrich-gamma → ingest-pma → process-trades → refresh-wallets → ingest-activities → snapshot-wallets → aggregate`

## 你机器上可直接照抄的执行顺序（结合你当前假设：OSS 已有初始数据）

你可以把下面当作“从零到可用”的最短路径（把 OSS 环境变量写好）：

1. 清空 PG（先删旧数据，确保是新库）：
```bash
cd forevex
docker compose down -v
docker compose up -d postgres
```

2. 首次全量：
```bash
cargo run -- migrate
cargo run -- run-all
```

3. 看是否都跑到你期望的进度：
```bash
cargo run -- status
```

4. 后续每天/每次追加 OSS 新的 PMA 增量包后：
```bash
cargo run -- sync
```

下面把整个流程按你这个项目的实现，串成「初始化 → 增量拉取 → 处理加工 → 存储落库」的完整步骤清单（每一步都包含：步骤/数据源/存储方式/操作命令）。

0. 前置条件（一次性）
OSS 上有 PMA 初始数据（你已完成）

目录按规格：polymarket/trades/、polymarket/blocks/（以及可选 polymarket/markets/、polymarket/legacy_trades/）
旁路文件 ._*.parquet 会被代码忽略（且你之前验证 canonical_counterpart_exists: true，基本可删）。
配置 forevex/.env

DATABASE_URL：Postgres 连接串
PIPELINE_OSS_BUCKET / PIPELINE_OSS_REGION / PIPELINE_OSS_ENDPOINT / AK/SK
可选：PIPELINE_S3_PREFIX（对象键有前缀时）
PIPELINE_DIM_MARKETS_SOURCE=auto（默认）：OSS 有 polymarket/markets/*.parquet 就用 Parquet，否则用 Gamma HTTP。
（可选）如果你要清理 ._ 旁路文件

先报告：cargo run -- oss-apple-double
再删除：cargo run -- oss-apple-double --delete

1. 初始化（清空 PG → 首次全量跑通）
满足你要求的“先清空 PG + OSS 已有初始 PMA”。

清空 PostgreSQL（确保是空库）

最稳：docker compose down -v 后再 docker compose up -d postgres
或你自定义的 drop/recreate 数据库方式（但要确保表/数据都没了）。
建表（migrations）

在 forevex/：
cargo run -- migrate
跑通全量管线（从 OSS 读 raw → 入库 → 加工 → 聚合）

推荐一次性跑完：
cargo run -- run-all
这会依次执行：
ingest-markets → enrich-gamma → ingest-pma → process-trades → refresh-wallets → ingest-activities → snapshot-wallets → aggregate
初始化后快速检查

cargo run -- status
或 cargo run -- serve 后看 GET /pipeline-status

2. 增量拉取与续跑（日常/定时）
之后你的日常更新只需要：

增量同步（不再执行 migrate）

cargo run -- sync
增量续跑点（关键：etl_checkpoint）

ingest-markets
checkpoint key：ingest_markets
增量依据：Gamma markets 的 offset（当你选择 Gamma HTTP 来源时）
如果走 OSS polymarket/markets/*.parquet：会用文件级 last_completed_file（pipeline key：ingest_markets_pma）
ingest-pma（PMA trades/blocks）
checkpoint key：pma_order_filled
增量依据：OSS 的 trades Parquet 按字典序文件键，记录 cursor_json.last_completed_file，只处理更后面的文件
同时忽略 ._*.parquet
process-trades
checkpoint key：process_trades
增量依据：last_stg_id，只处理新增的 stg_order_filled 行

其余步骤的幂等/重算方式

process-trades / fact_trades：都有 ON CONFLICT DO NOTHING，可安全重复跑。
refresh-wallets：以 fact_trades 重建维表范围（upsert min/max），可重复跑。
ingest-activities：对每个 proxy 的非 TRADE 活动做“是否已存在”检查（避免重复插入）。
snapshot-wallets：每次会往 wallet_api_snapshot 追加新行（带 fetched_at），不做去重（一般这是你想要的“快照历史”）。
aggregate：会 DELETE 后从 fact_trades 重建聚合表（因此安全，但要接受全量重算成本）。

3. 数据源 → 处理 → 存储方式（你关心的“整个过程”）
按链路理解即可：

数据源（Raw）

PMA：OSS 上的 Parquet
polymarket/trades/*.parquet → 原始成交行
polymarket/blocks/*.parquet → 由 block_number → timestamp 映射
polymarket/markets/*.parquet（可选）→ 市场维表来源
polymarket/legacy_trades/*.parquet（可选，代码列举/清理已一致，默认 ETL 仍主要走 trades/blocks）

Gamma HTTP / Data API（非 Parquet 部分）
ingest-markets（当没 markets Parquet 时）
enrich-gamma：给 dim_markets 填 category_raw / topic_primary
ingest-activities：Data API activity 拉取非 TRADE
snapshot-wallets：user-stats / user-pnl / public-profile 快照

处理（Staging / Fact / Enrich / Aggregate）

ingest-pma：把 PMA trades 灌入 stg_order_filled
process-trades：把 stg_order_filled + dim_markets 加工为 fact_trades（对齐 process_live 语义，方向、price、金额 ÷10^6 等在代码里实现）
enrich-gamma：补 dim_markets.category_raw/topic_primary
refresh-wallets：从 fact_trades 重建 dim_wallets
aggregate：从 fact_trades 重建 agg_wallet_topic + agg_global_daily

存储（落库到哪里）

原始数据（raw Parquet）：OSS（权威），本地 polymarket/** 只用于解压/暂存（有 bootstrap 时会上传并删本地）
加工结果与服务表（Postgres）：
dim_markets（市场维表）
stg_order_filled（PMA 原始成交 staging）
fact_trades（加工后的事实成交）
dim_wallets（钱包维表）
fact_account_activities（账户活动）
wallet_api_snapshot（API 快照）
agg_wallet_topic / agg_global_daily（聚合结果）
etl_checkpoint（各步骤增量游标）

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
