//! Fill `dim_markets.category_raw` and `topic_primary` via Gamma `GET /markets/{id}`.

use crate::config::Config;
use crate::taxonomy::rollup_category_to_topic;
use reqwest::Client;
use serde_json::Value;
use sqlx::PgPool;

pub async fn run(pool: &PgPool, cfg: &Config) -> anyhow::Result<usize> {
    let client = Client::builder().timeout(cfg.http_timeout).build()?;

    let ids: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT market_id FROM dim_markets
        WHERE category_raw IS NULL OR topic_primary IS NULL
        ORDER BY market_id
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut n = 0usize;
    for market_id in ids {
        let url = format!(
            "{}/markets/{}",
            cfg.gamma_origin,
            urlencoding::encode(&market_id)
        );
        let resp = client.get(&url).send().await?;
        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            tokio::time::sleep(std::time::Duration::from_millis(cfg.rate_limit_ms)).await;
            continue;
        }
        if !resp.status().is_success() {
            tracing::warn!(market_id = %market_id, status = %resp.status(), "gamma market by id failed");
            continue;
        }
        let market: Value = resp.json().await?;
        let category = market
            .get("category")
            .and_then(|v| v.as_str())
            .map(String::from);
        let topic = rollup_category_to_topic(category.as_deref());

        sqlx::query(
            r#"
            UPDATE dim_markets SET
                category_raw = $2,
                topic_primary = $3,
                fetched_at = NOW()
            WHERE market_id = $1
            "#,
        )
        .bind(&market_id)
        .bind(&category)
        .bind(&topic)
        .execute(pool)
        .await?;

        n += 1;
        tokio::time::sleep(std::time::Duration::from_millis(cfg.rate_limit_ms)).await;
    }

    tracing::info!(n, "enrich-gamma finished");
    Ok(n)
}
