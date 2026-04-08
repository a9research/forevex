use std::path::PathBuf;
use std::time::Duration;

/// `dim_markets` 数据来源（规格 §4：主路径为 PMA `markets` Parquet；Gamma HTTP 为旁路）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DimMarketsSource {
    /// `GET /markets` 分页（poly_data 风格）
    GammaHttp,
    /// OSS `polymarket/markets/*.parquet`（须配置 bucket）
    PmaParquetOss,
    /// 若 OSS 上存在 `polymarket/markets/*.parquet` 则用 Parquet，否则 Gamma HTTP
    Auto,
}

/// Trades processing pipeline strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradesProcessor {
    /// Legacy: PMA Parquet → PG `stg_order_filled` → PG `fact_trades`.
    PgStg,
    /// Preferred: OSS PMA Parquet → PG `fact_trades` directly (skip PG staging).
    PmaOssDirect,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    /// sqlx pool size (must stay below Postgres `max_connections` minus other clients).
    pub db_max_connections: u32,
    /// How long to wait for a free pool slot before erroring (`pool timed out`).
    pub db_acquire_timeout: Duration,
    pub gamma_origin: String,
    pub data_api_origin: String,
    pub http_timeout: Duration,
    pub markets_batch_size: u32,
    /// Delay between outbound HTTP calls (Gamma enrich, snapshots, activity).
    pub rate_limit_ms: u64,
    /// Comma-separated proxy addresses for `ingest-activities`.
    pub activity_proxies: Vec<String>,
    /// Comma-separated proxies for `snapshot-wallets`; if empty, use first N from `dim_wallets`.
    pub snapshot_proxies: Vec<String>,
    pub snapshot_max_wallets: usize,
    pub user_stats_path: String,
    pub user_pnl_origin: String,
    pub gamma_profile_path: String,
    /// `polymarket-pipeline serve` bind address.
    pub http_bind: String,

    /// 含 `polymarket/{trades,blocks,markets,legacy_trades}` 的目录（**仅作解压/增量暂存**，以对象存储为准）。
    pub pma_data_dir: PathBuf,
    /// `bootstrap-data` 默认下载地址（PMA 预置包）。
    pub bootstrap_download_url: String,
    /// 未配置对象存储时，允许仅从本地 Parquet 入库（开发用）。
    pub pma_allow_local_only: bool,

    /// S3 兼容对象存储（阿里云 OSS / MinIO / R2 等）。配置后 **PMA 以 OSS 为唯一 raw 源**。
    pub s3_bucket: Option<String>,
    pub s3_region: Option<String>,
    pub s3_endpoint: Option<String>,
    /// 对象键前缀（如 `prod`，则键为 `prod/polymarket/trades/...`）。
    pub s3_prefix: Option<String>,
    pub s3_access_key_id: Option<String>,
    pub s3_secret_access_key: Option<String>,
    /// 虚拟主机样式（如 `https://bucket.oss-cn-hangzhou.aliyuncs.com`）。阿里云部分场景需 `true`。
    pub s3_virtual_hosted: bool,

    /// `ingest-markets`：`gamma` | `pma_parquet` | `auto`（默认 `auto`）。
    pub dim_markets_source: DimMarketsSource,

    /// Trades processor: `pma_oss`（默认）| `pg_stg`。
    pub trades_processor: TradesProcessor,

    /// Skip `enrich-gamma` step in `sync/run-all` (useful when Gamma is slow/limited).
    pub skip_enrich_gamma: bool,

    /// enrich-gamma: max markets per run (sync/run-all step).
    pub enrich_gamma_batch_size: u32,
    /// enrich-gamma: base backoff seconds after a failure (doubles per attempt).
    pub enrich_gamma_fail_backoff_sec: u64,

    /// Polygon JSON-RPC endpoints (comma-separated). Used by PMA-aligned indexer (OSS Parquet writer).
    pub polygon_rpc_urls: Vec<String>,
    /// Max retry attempts per RPC request (with backoff + endpoint rotation).
    pub polygon_rpc_max_retries: u32,
    /// Base backoff in milliseconds for RPC retries.
    pub polygon_rpc_backoff_ms: u64,

    /// Optional: contract address to filter `eth_getLogs` for OrderFilled-like events.
    /// If empty, indexer will fail when running trades sync.
    pub polymarket_exchange_addresses: Vec<String>,
    /// Optional: topic0 for OrderFilled event (0x... 32-byte hash).
    pub polymarket_order_filled_topic0: Option<String>,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let _ = dotenvy::dotenv();

        // Defaults extracted from prediction-market-analysis (PMA) `src/indexers/polymarket/blockchain.py`.
        const PMA_CTF_EXCHANGE: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";
        const PMA_NEGRISK_CTF_EXCHANGE: &str = "0xC5d563A36AE78145C45a50134d48A1215220f80a";
        const PMA_ORDER_FILLED_TOPIC0: &str =
            "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6";

        let database_url = std::env::var("DATABASE_URL")
            .or_else(|_| std::env::var("PIPELINE_DATABASE_URL"))
            .map_err(|_| anyhow::anyhow!("DATABASE_URL or PIPELINE_DATABASE_URL required"))?;
        let db_max_connections: u32 = std::env::var("PIPELINE_DB_MAX_CONNECTIONS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(16)
            .clamp(1, 256);
        let db_acquire_timeout_sec: u64 = std::env::var("PIPELINE_DB_ACQUIRE_TIMEOUT_SEC")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(120)
            .max(1);
        let gamma_origin = std::env::var("PIPELINE_GAMMA_ORIGIN")
            .unwrap_or_else(|_| "https://gamma-api.polymarket.com".to_string())
            .trim_end_matches('/')
            .to_string();
        let data_api_origin = std::env::var("PIPELINE_DATA_API_ORIGIN")
            .unwrap_or_else(|_| "https://data-api.polymarket.com".to_string())
            .trim_end_matches('/')
            .to_string();
        let timeout_sec: u64 = std::env::var("PIPELINE_HTTP_TIMEOUT_SEC")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(120);
        let markets_batch_size: u32 = std::env::var("PIPELINE_MARKETS_BATCH_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(500);
        let rate_limit_ms: u64 = std::env::var("PIPELINE_RATE_LIMIT_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(120);

        let activity_proxies =
            parse_csv_list(std::env::var("PIPELINE_ACTIVITY_PROXIES").unwrap_or_default());
        let snapshot_proxies =
            parse_csv_list(std::env::var("PIPELINE_SNAPSHOT_PROXIES").unwrap_or_default());
        let snapshot_max_wallets: usize = std::env::var("PIPELINE_SNAPSHOT_MAX_WALLETS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(200);

        let user_stats_path = std::env::var("PIPELINE_USER_STATS_URL")
            .unwrap_or_else(|_| "https://data-api.polymarket.com/v1/user-stats".to_string());
        let user_pnl_origin = std::env::var("PIPELINE_USER_PNL_URL")
            .unwrap_or_else(|_| "https://user-pnl-api.polymarket.com/user-pnl".to_string())
            .trim_end_matches('/')
            .to_string();
        let gamma_profile_path = std::env::var("PIPELINE_GAMMA_PUBLIC_PROFILE_URL")
            .unwrap_or_else(|_| "https://gamma-api.polymarket.com/public-profile".to_string());

        let http_bind =
            std::env::var("PIPELINE_HTTP_BIND").unwrap_or_else(|_| "0.0.0.0:8080".to_string());

        let pma_data_dir = std::env::var("PIPELINE_PMA_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("data"));

        let bootstrap_download_url = std::env::var("PIPELINE_BOOTSTRAP_DOWNLOAD_URL")
            .unwrap_or_else(|_| "https://s3.jbecker.dev/data.tar.zst".to_string());

        let pma_allow_local_only = env_truthy(std::env::var("PIPELINE_PMA_ALLOW_LOCAL_ONLY").ok());

        // 阿里云 OSS：可用 PIPELINE_OSS_*；与 PIPELINE_S3_* 等价（后者为通用别名）
        let s3_bucket = nonempty_opt(
            std::env::var("PIPELINE_OSS_BUCKET")
                .ok()
                .or_else(|| std::env::var("PIPELINE_S3_BUCKET").ok()),
        );
        let s3_region = nonempty_opt(
            std::env::var("PIPELINE_OSS_REGION")
                .ok()
                .or_else(|| std::env::var("PIPELINE_S3_REGION").ok()),
        );
        let s3_endpoint = nonempty_opt(
            std::env::var("PIPELINE_OSS_ENDPOINT")
                .ok()
                .or_else(|| std::env::var("PIPELINE_S3_ENDPOINT").ok()),
        );
        let s3_prefix = nonempty_opt(std::env::var("PIPELINE_S3_PREFIX").ok());
        let s3_access_key_id = nonempty_opt(
            std::env::var("PIPELINE_OSS_ACCESS_KEY_ID")
                .ok()
                .or_else(|| std::env::var("PIPELINE_S3_ACCESS_KEY_ID").ok()),
        );
        let s3_secret_access_key = nonempty_opt(
            std::env::var("PIPELINE_OSS_ACCESS_KEY_SECRET")
                .ok()
                .or_else(|| std::env::var("PIPELINE_OSS_SECRET_ACCESS_KEY").ok())
                .or_else(|| std::env::var("PIPELINE_S3_SECRET_ACCESS_KEY").ok()),
        );
        let s3_virtual_hosted = env_truthy(
            std::env::var("PIPELINE_OSS_VIRTUAL_HOSTED")
                .ok()
                .or_else(|| std::env::var("PIPELINE_S3_VIRTUAL_HOSTED").ok()),
        );

        let dim_markets_source = parse_dim_markets_source(
            std::env::var("PIPELINE_DIM_MARKETS_SOURCE").unwrap_or_else(|_| "auto".to_string()),
        )?;

        let trades_processor = parse_trades_processor(
            std::env::var("PIPELINE_TRADES_PROCESSOR").unwrap_or_else(|_| "pma_oss".to_string()),
        )?;

        let skip_enrich_gamma = env_truthy(std::env::var("PIPELINE_SKIP_ENRICH_GAMMA").ok());

        let enrich_gamma_batch_size: u32 = std::env::var("PIPELINE_ENRICH_GAMMA_BATCH_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1_000)
            .clamp(1, 200_000);
        let enrich_gamma_fail_backoff_sec: u64 =
            std::env::var("PIPELINE_ENRICH_GAMMA_FAIL_BACKOFF_SEC")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(300)
                .clamp(1, 86_400);

        let mut polygon_rpc_urls = parse_csv_list(
            std::env::var("PIPELINE_POLYGON_RPC_URLS")
                .or_else(|_| std::env::var("PIPELINE_POLYGON_RPC_URL").map(|s| s))
                .or_else(|_| std::env::var("POLYGON_RPC_URLS").map(|s| s))
                .or_else(|_| std::env::var("POLYGON_RPC_URL").map(|s| s))
                .unwrap_or_default(),
        );
        // 与 `.env.example` 建议一致；未配置时让 `indexer-oss` 可直接跑通。公共节点易限流，生产请自备 RPC。
        if polygon_rpc_urls.is_empty() {
            polygon_rpc_urls = vec![
                "https://polygon-rpc.com".to_string(),
                "https://rpc.ankr.com/polygon".to_string(),
            ];
        }
        let polygon_rpc_max_retries: u32 = std::env::var("PIPELINE_POLYGON_RPC_MAX_RETRIES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8)
            .clamp(0, 100);
        let polygon_rpc_backoff_ms: u64 = std::env::var("PIPELINE_POLYGON_RPC_BACKOFF_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(250)
            .clamp(0, 60_000);

        let polymarket_exchange_addresses = {
            let s = std::env::var("PIPELINE_POLYMARKET_EXCHANGE_ADDRESS")
                .ok()
                .or_else(|| std::env::var("PIPELINE_POLYMARKET_EXCHANGE_ADDRESSES").ok())
                .or_else(|| std::env::var("PIPELINE_PM_EXCHANGE_ADDRESS").ok())
                .or_else(|| std::env::var("PIPELINE_PM_EXCHANGE_ADDRESSES").ok())
                .unwrap_or_default();
            let v = parse_csv_list(s);
            if v.is_empty() {
                vec![
                    PMA_CTF_EXCHANGE.to_string(),
                    PMA_NEGRISK_CTF_EXCHANGE.to_string(),
                ]
            } else {
                v
            }
        };
        let polymarket_order_filled_topic0 = nonempty_opt(
            std::env::var("PIPELINE_POLYMARKET_ORDER_FILLED_TOPIC0")
                .ok()
                .or_else(|| std::env::var("PIPELINE_PM_ORDER_FILLED_TOPIC0").ok()),
        );
        let polymarket_order_filled_topic0 =
            polymarket_order_filled_topic0.or_else(|| Some(PMA_ORDER_FILLED_TOPIC0.to_string()));

        Ok(Self {
            database_url,
            db_max_connections,
            db_acquire_timeout: Duration::from_secs(db_acquire_timeout_sec),
            gamma_origin,
            data_api_origin,
            http_timeout: Duration::from_secs(timeout_sec.max(10)),
            markets_batch_size: markets_batch_size.max(1),
            rate_limit_ms,
            activity_proxies,
            snapshot_proxies,
            snapshot_max_wallets: snapshot_max_wallets.max(1),
            user_stats_path,
            user_pnl_origin,
            gamma_profile_path,
            http_bind,
            pma_data_dir,
            bootstrap_download_url,
            pma_allow_local_only,
            s3_bucket,
            s3_region,
            s3_endpoint,
            s3_prefix,
            s3_access_key_id,
            s3_secret_access_key,
            s3_virtual_hosted,
            dim_markets_source,
            trades_processor,
            skip_enrich_gamma,
            enrich_gamma_batch_size,
            enrich_gamma_fail_backoff_sec,

            polygon_rpc_urls,
            polygon_rpc_max_retries,
            polygon_rpc_backoff_ms,
            polymarket_exchange_addresses,
            polymarket_order_filled_topic0,
        })
    }
}

fn parse_dim_markets_source(s: String) -> anyhow::Result<DimMarketsSource> {
    match s.trim().to_ascii_lowercase().as_str() {
        "gamma" | "gamma_http" | "http" => Ok(DimMarketsSource::GammaHttp),
        "pma_parquet" | "parquet" | "oss" | "markets_parquet" => {
            Ok(DimMarketsSource::PmaParquetOss)
        }
        "auto" => Ok(DimMarketsSource::Auto),
        _ => anyhow::bail!(
            "PIPELINE_DIM_MARKETS_SOURCE: expected gamma | pma_parquet | auto, got {s:?}"
        ),
    }
}

fn parse_trades_processor(s: String) -> anyhow::Result<TradesProcessor> {
    match s.trim().to_ascii_lowercase().as_str() {
        "pma_oss" | "pma" | "oss" | "direct" => Ok(TradesProcessor::PmaOssDirect),
        "pg_stg" | "stg" | "postgres" => Ok(TradesProcessor::PgStg),
        _ => anyhow::bail!("PIPELINE_TRADES_PROCESSOR: expected pma_oss | pg_stg, got {s:?}"),
    }
}

fn env_truthy(s: Option<String>) -> bool {
    match s {
        None => false,
        Some(v) => {
            let v = v.trim().to_ascii_lowercase();
            matches!(v.as_str(), "1" | "true" | "yes" | "on")
        }
    }
}

fn nonempty_opt(s: Option<String>) -> Option<String> {
    s.filter(|x| !x.trim().is_empty())
}

fn parse_csv_list(s: String) -> Vec<String> {
    s.split(',')
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect()
}
