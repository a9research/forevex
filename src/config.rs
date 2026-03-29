use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    /// sqlx pool size (must stay below Postgres `max_connections` minus other clients).
    pub db_max_connections: u32,
    /// How long to wait for a free pool slot before erroring (`pool timed out`).
    pub db_acquire_timeout: Duration,
    pub gamma_origin: String,
    pub data_api_origin: String,
    pub goldsky_graphql_url: String,
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
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let _ = dotenvy::dotenv();
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
        let goldsky_graphql_url = std::env::var("PIPELINE_GOLDSKY_GRAPHQL_URL").unwrap_or_else(|_| {
            "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"
                .to_string()
        });
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

        let activity_proxies = parse_csv_list(
            std::env::var("PIPELINE_ACTIVITY_PROXIES").unwrap_or_default(),
        );
        let snapshot_proxies = parse_csv_list(
            std::env::var("PIPELINE_SNAPSHOT_PROXIES").unwrap_or_default(),
        );
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

        let http_bind = std::env::var("PIPELINE_HTTP_BIND").unwrap_or_else(|_| "0.0.0.0:8080".to_string());

        Ok(Self {
            database_url,
            db_max_connections,
            db_acquire_timeout: Duration::from_secs(db_acquire_timeout_sec),
            gamma_origin,
            data_api_origin,
            goldsky_graphql_url,
            http_timeout: Duration::from_secs(timeout_sec.max(10)),
            markets_batch_size: markets_batch_size.max(1),
            rate_limit_ms: rate_limit_ms.max(0),
            activity_proxies,
            snapshot_proxies,
            snapshot_max_wallets: snapshot_max_wallets.max(1),
            user_stats_path,
            user_pnl_origin,
            gamma_profile_path,
            http_bind,
        })
    }
}

fn parse_csv_list(s: String) -> Vec<String> {
    s.split(',')
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect()
}
