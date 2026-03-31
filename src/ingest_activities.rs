//! Pull Data API activity for configured proxies → `fact_account_activities` (non-TRADE types preferred).

use crate::config::Config;
use chrono::{DateTime, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::PgPool;

pub async fn run(pool: &PgPool, cfg: &Config) -> anyhow::Result<usize> {
    if cfg.activity_proxies.is_empty() {
        tracing::info!("ingest-activities: PIPELINE_ACTIVITY_PROXIES empty, skip");
        return Ok(0);
    }

    let client = Client::builder().timeout(cfg.http_timeout).build()?;

    let mut n = 0usize;
    for proxy in &cfg.activity_proxies {
        let proxy = proxy.trim();
        if !proxy.starts_with("0x") || proxy.len() != 42 {
            tracing::warn!(%proxy, "skip invalid proxy");
            continue;
        }
        let url = format!(
            "{}/activity?user={}&limit=500",
            cfg.data_api_origin,
            urlencoding::encode(proxy)
        );
        let resp = client.get(&url).send().await;
        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(%proxy, error = %e, "activity request failed");
                continue;
            }
        };
        if !resp.status().is_success() {
            tracing::warn!(%proxy, status = %resp.status(), "activity HTTP error");
            tokio::time::sleep(std::time::Duration::from_millis(cfg.rate_limit_ms)).await;
            continue;
        }
        let body: Value = match resp.json().await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(%proxy, error = %e, "activity JSON");
                continue;
            }
        };

        let arr = body
            .as_array()
            .cloned()
            .or_else(|| body.get("data").and_then(|v| v.as_array()).cloned())
            .unwrap_or_default();

        for ev in arr {
            let type_s = ev
                .get("type")
                .or_else(|| ev.get("action"))
                .and_then(|v| v.as_str())
                .unwrap_or("UNKNOWN")
                .to_uppercase();

            if type_s == "TRADE" {
                continue;
            }

            let occurred_at = parse_time(&ev).unwrap_or_else(Utc::now);
            let tx_hash = ev
                .get("transactionHash")
                .or_else(|| ev.get("transaction_hash"))
                .and_then(|v| v.as_str())
                .map(String::from);
            let condition_id = ev
                .get("conditionId")
                .or_else(|| ev.get("condition_id"))
                .and_then(|v| v.as_str())
                .map(String::from);
            let amount = parse_amount(&ev);

            let exists: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS(
                    SELECT 1 FROM fact_account_activities
                    WHERE proxy_address = $1
                      AND activity_type = $2
                      AND occurred_at = $3
                      AND tx_hash IS NOT DISTINCT FROM $4
                )
                "#,
            )
            .bind(proxy.to_lowercase())
            .bind(&type_s)
            .bind(occurred_at)
            .bind(&tx_hash)
            .fetch_one(pool)
            .await?;

            if exists {
                continue;
            }

            sqlx::query(
                r#"
                INSERT INTO fact_account_activities (
                    occurred_at, block_number, tx_hash, proxy_address, activity_type,
                    condition_id, amount_usd, payload_json, source
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'data_api')
                "#,
            )
            .bind(occurred_at)
            .bind(
                ev.get("blockNumber")
                    .and_then(|v| v.as_i64())
                    .or_else(|| ev.get("block_number").and_then(|v| v.as_i64())),
            )
            .bind(&tx_hash)
            .bind(proxy.to_lowercase())
            .bind(&type_s)
            .bind(&condition_id)
            .bind(amount)
            .bind(&ev)
            .execute(pool)
            .await?;

            n += 1;
        }

        tokio::time::sleep(std::time::Duration::from_millis(cfg.rate_limit_ms)).await;
    }

    tracing::info!(n, "ingest-activities rows inserted");
    Ok(n)
}

fn parse_time(ev: &Value) -> Option<DateTime<Utc>> {
    let s = ev
        .get("timestamp")
        .or_else(|| ev.get("createdAt"))
        .and_then(|v| v.as_str())?;
    if let Ok(t) = DateTime::parse_from_rfc3339(s) {
        return Some(t.with_timezone(&Utc));
    }
    if let Ok(u) = s.parse::<i64>() {
        return DateTime::from_timestamp(u, 0);
    }
    None
}

fn parse_amount(ev: &Value) -> Option<Decimal> {
    let v = ev.get("usdcSize").or_else(|| ev.get("size"))?;
    if let Some(f) = v.as_f64() {
        return Decimal::from_f64_retain(f);
    }
    v.as_str().and_then(|s| s.parse().ok())
}
