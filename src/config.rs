use anyhow::Context;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    /// Public URL for docs / CORS allow (optional).
    pub public_base_url: Option<String>,
    pub bind: String,
    pub gamma_origin: String,
    pub data_api_origin: String,
    pub user_stats_path: String,
    pub user_pnl_origin: String,
    pub http_timeout: Duration,
    pub rate_limit_ms: u64,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let _ = dotenvy::dotenv();
        let database_url = std::env::var("DATABASE_URL")
            .or_else(|_| std::env::var("FOREVEX_DATABASE_URL"))
            .context("DATABASE_URL or FOREVEX_DATABASE_URL must be set")?;
        let public_base_url = std::env::var("FOREVEX_PUBLIC_BASE_URL").ok().filter(|s| !s.is_empty());
        let bind = std::env::var("FOREVEX_BIND").unwrap_or_else(|_| "0.0.0.0:3000".to_string());
        let gamma_origin = std::env::var("FOREVEX_GAMMA_ORIGIN")
            .unwrap_or_else(|_| "https://gamma-api.polymarket.com".to_string())
            .trim_end_matches('/')
            .to_string();
        let data_api_origin = std::env::var("FOREVEX_DATA_API_ORIGIN")
            .unwrap_or_else(|_| "https://data-api.polymarket.com".to_string())
            .trim_end_matches('/')
            .to_string();
        let user_stats_path = std::env::var("FOREVEX_USER_STATS_URL")
            .unwrap_or_else(|_| "https://data-api.polymarket.com/v1/user-stats".to_string())
            .trim_end_matches('/')
            .to_string();
        let user_pnl_origin = std::env::var("FOREVEX_USER_PNL_URL")
            .unwrap_or_else(|_| "https://user-pnl-api.polymarket.com/user-pnl".to_string())
            .trim_end_matches('/')
            .to_string();
        let timeout_sec: u64 = std::env::var("FOREVEX_HTTP_TIMEOUT_SEC")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(45);
        let rate_limit_ms: u64 = std::env::var("FOREVEX_RATE_LIMIT_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(120);
        Ok(Self {
            database_url,
            public_base_url,
            bind,
            gamma_origin,
            data_api_origin,
            user_stats_path,
            user_pnl_origin,
            http_timeout: Duration::from_secs(timeout_sec.max(5)),
            rate_limit_ms: rate_limit_ms.max(0),
        })
    }
}
