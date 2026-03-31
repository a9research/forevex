//! Batch fetch Data API user-stats + user-pnl + Gamma public-profile → `wallet_api_snapshot`.

use crate::config::Config;
use reqwest::Client;
use serde_json::Value;
use sqlx::PgPool;

pub async fn run(pool: &PgPool, cfg: &Config) -> anyhow::Result<usize> {
    let client = Client::builder().timeout(cfg.http_timeout).build()?;

    let proxies: Vec<String> = if !cfg.snapshot_proxies.is_empty() {
        cfg.snapshot_proxies.clone()
    } else {
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT proxy_address FROM dim_wallets
            ORDER BY last_seen_at DESC
            LIMIT $1
            "#,
        )
        .bind(cfg.snapshot_max_wallets as i64)
        .fetch_all(pool)
        .await?
    };

    if proxies.is_empty() {
        tracing::info!("snapshot-wallets: no wallets, skip");
        return Ok(0);
    }

    let stats_base = cfg.user_stats_path.trim_end_matches('/').to_string();
    let pnl_base = cfg.user_pnl_origin.trim_end_matches('/').to_string();
    let profile_base = cfg.gamma_profile_path.trim_end_matches('/').to_string();

    let mut n = 0usize;
    for proxy in proxies {
        let addr = proxy.trim().to_lowercase();
        if !addr.starts_with("0x") || addr.len() != 42 {
            tracing::warn!(%proxy, "skip invalid proxy");
            continue;
        }

        let stats_url = format!("{}?proxyAddress={}", stats_base, urlencoding::encode(&addr));
        let pnl_url = format!(
            "{}?user_address={}&interval=all&fidelity=12h",
            pnl_base,
            urlencoding::encode(&addr)
        );
        let profile_url = format!("{}?address={}", profile_base, urlencoding::encode(&addr));

        let user_stats_json = fetch_json_opt(&client, &stats_url).await;
        let user_pnl_json = fetch_json_opt(&client, &pnl_url).await;
        let gamma_profile_json = fetch_json_opt(&client, &profile_url).await;

        sqlx::query(
            r#"
            INSERT INTO wallet_api_snapshot (
                proxy_address, fetched_at, user_stats_json, user_pnl_json, gamma_profile_json
            )
            VALUES ($1, NOW(), $2, $3, $4)
            "#,
        )
        .bind(&addr)
        .bind(&user_stats_json)
        .bind(&user_pnl_json)
        .bind(&gamma_profile_json)
        .execute(pool)
        .await?;

        n += 1;
        tokio::time::sleep(std::time::Duration::from_millis(cfg.rate_limit_ms)).await;
    }

    tracing::info!(n, "snapshot-wallets rows inserted");
    Ok(n)
}

async fn fetch_json_opt(client: &Client, url: &str) -> Option<Value> {
    match client.get(url).send().await {
        Ok(resp) => {
            if !resp.status().is_success() {
                tracing::warn!(%url, status = %resp.status(), "snapshot HTTP error");
                return None;
            }
            match resp.json::<Value>().await {
                Ok(v) => Some(v),
                Err(e) => {
                    tracing::warn!(%url, error = %e, "snapshot JSON");
                    None
                }
            }
        }
        Err(e) => {
            tracing::warn!(%url, error = %e, "snapshot request failed");
            None
        }
    }
}
