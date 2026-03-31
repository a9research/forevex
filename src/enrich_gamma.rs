//! Fill `dim_markets.category_raw` and `topic_primary` via Gamma `GET /markets/{id}`.

use crate::checkpoint;
use crate::config::Config;
use crate::taxonomy::rollup_category_to_topic;
use reqwest::Client;
use serde_json::Value;
use sqlx::PgPool;
use std::time::Duration;

const PIPELINE_KEY: &str = "enrich_gamma";

pub async fn run(pool: &PgPool, cfg: &Config) -> anyhow::Result<usize> {
    let client = Client::builder().timeout(cfg.http_timeout).build()?;

    let cursor = checkpoint::load(pool, PIPELINE_KEY).await?;
    let mut last_market_id = cursor
        .as_ref()
        .and_then(|j| j.get("last_market_id"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let mut n = 0usize;
    let mut wrapped = false;
    loop {
        let batch = select_next_batch(pool, cfg, last_market_id.as_deref()).await?;
        if batch.is_empty() && !wrapped {
            // Wrap once to catch low IDs that were skipped previously due to backoff.
            wrapped = true;
            last_market_id = None;
            continue;
        }
        if batch.is_empty() {
            // Done for now; clear cursor.
            checkpoint::save(
                pool,
                PIPELINE_KEY,
                serde_json::json!({ "last_market_id": "" }),
            )
            .await?;
            break;
        }

        for market_id in batch {
            let url = format!(
                "{}/markets/{}",
                cfg.gamma_origin,
                urlencoding::encode(&market_id)
            );

            let resp = client.get(&url).send().await;
            match resp {
                Ok(resp) => {
                    let status = resp.status();
                    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                        mark_failure(pool, cfg, &market_id, "429 too many requests").await?;
                    } else if !status.is_success() {
                        mark_failure(pool, cfg, &market_id, &format!("http status {status}"))
                            .await?;
                    } else {
                        let market: Value = resp.json().await.unwrap_or(Value::Null);
                        // Treat any 200 response as "fetched" (even if category missing) so we don't refetch.
                        let category = market
                            .get("category")
                            .and_then(|v| v.as_str())
                            .map(String::from);
                        let topic = rollup_category_to_topic(category.as_deref());
                        mark_success(pool, &market_id, category, topic).await?;
                        n += 1;
                    }
                }
                Err(e) => {
                    mark_failure(pool, cfg, &market_id, &format!("request error: {e}")).await?;
                }
            }

            last_market_id = Some(market_id.clone());
            checkpoint::save(
                pool,
                PIPELINE_KEY,
                serde_json::json!({ "last_market_id": last_market_id.as_deref().unwrap_or("") }),
            )
            .await?;

            tokio::time::sleep(Duration::from_millis(cfg.rate_limit_ms)).await;
        }
    }

    tracing::info!(n, "enrich-gamma finished");
    Ok(n)
}

async fn select_next_batch(
    pool: &PgPool,
    cfg: &Config,
    last_market_id: Option<&str>,
) -> anyhow::Result<Vec<String>> {
    // We only pick rows that have never had a successful 200 enrich response.
    // Failures are retried with exponential backoff based on attempts.
    let backoff_base = cfg.enrich_gamma_fail_backoff_sec as i64;
    let limit = cfg.enrich_gamma_batch_size as i64;
    let last = last_market_id.unwrap_or("");

    let q = r#"
        SELECT market_id
        FROM dim_markets
        WHERE enrich_gamma_fetched_at IS NULL
          AND market_id > $1
          AND (
            enrich_gamma_failed_at IS NULL
            OR enrich_gamma_failed_at < NOW() - (
                ($2::bigint) * (1::bigint << LEAST(enrich_gamma_attempts, 10))
            ) * INTERVAL '1 second'
          )
        ORDER BY market_id
        LIMIT $3
    "#;
    let rows: Vec<String> = sqlx::query_scalar(q)
        .bind(last)
        .bind(backoff_base)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

async fn mark_success(
    pool: &PgPool,
    market_id: &str,
    category: Option<String>,
    topic: String,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE dim_markets SET
            category_raw = $2,
            topic_primary = $3,
            enrich_gamma_fetched_at = NOW(),
            enrich_gamma_failed_at = NULL,
            enrich_gamma_last_error = NULL
        WHERE market_id = $1
        "#,
    )
    .bind(market_id)
    .bind(&category)
    .bind(&topic)
    .execute(pool)
    .await?;
    Ok(())
}

async fn mark_failure(
    pool: &PgPool,
    cfg: &Config,
    market_id: &str,
    err: &str,
) -> anyhow::Result<()> {
    // If Gamma is rate-limiting hard, do a small global sleep to avoid hammering.
    if err.starts_with("429") {
        tokio::time::sleep(Duration::from_millis(cfg.rate_limit_ms.max(250))).await;
    }
    sqlx::query(
        r#"
        UPDATE dim_markets SET
            enrich_gamma_failed_at = NOW(),
            enrich_gamma_attempts = enrich_gamma_attempts + 1,
            enrich_gamma_last_error = $2
        WHERE market_id = $1
          AND enrich_gamma_fetched_at IS NULL
        "#,
    )
    .bind(market_id)
    .bind(err)
    .execute(pool)
    .await?;
    Ok(())
}
