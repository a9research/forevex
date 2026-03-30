//! Gamma `GET /markets` — same semantics as poly_data `update_markets.py`；
//! 或（规格主路径）从 OSS `polymarket/markets/*.parquet` 增量灌入。

use crate::checkpoint;
use crate::config::{Config, DimMarketsSource};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde_json::{json, Value};
use sqlx::PgPool;

const PIPELINE_KEY: &str = "ingest_markets";

pub async fn run(pool: &PgPool, cfg: &Config) -> anyhow::Result<usize> {
    let source = resolve_dim_markets_source(cfg).await?;
    match source {
        DimMarketsSource::GammaHttp => run_gamma_http(pool, cfg).await,
        DimMarketsSource::PmaParquetOss => {
            crate::ingest_markets_parquet::run_from_oss_parquet(pool, cfg).await
        }
        DimMarketsSource::Auto => {
            unreachable!("resolve_dim_markets_source never returns Auto")
        }
    }
}

async fn resolve_dim_markets_source(cfg: &Config) -> anyhow::Result<DimMarketsSource> {
    match cfg.dim_markets_source {
        DimMarketsSource::Auto => {
            if cfg.s3_bucket.is_some() {
                let n = crate::pma::list_oss_parquet_keys_under(cfg, "polymarket/markets")
                    .await?
                    .len();
                if n > 0 {
                    tracing::info!(
                        n,
                        "ingest-markets: OSS has polymarket/markets parquet → PMA path"
                    );
                    Ok(DimMarketsSource::PmaParquetOss)
                } else {
                    tracing::info!("ingest-markets: no markets parquet on OSS → Gamma HTTP");
                    Ok(DimMarketsSource::GammaHttp)
                }
            } else {
                Ok(DimMarketsSource::GammaHttp)
            }
        }
        DimMarketsSource::PmaParquetOss => {
            if cfg.s3_bucket.is_none() {
                anyhow::bail!(
                    "PIPELINE_DIM_MARKETS_SOURCE=pma_parquet requires PIPELINE_OSS_BUCKET (or PIPELINE_S3_BUCKET)"
                );
            }
            Ok(DimMarketsSource::PmaParquetOss)
        }
        DimMarketsSource::GammaHttp => Ok(DimMarketsSource::GammaHttp),
    }
}

async fn run_gamma_http(pool: &PgPool, cfg: &Config) -> anyhow::Result<usize> {
    let client = Client::builder()
        .timeout(cfg.http_timeout)
        .build()?;

    let mut current_offset: u32 = checkpoint::load(pool, PIPELINE_KEY)
        .await?
        .and_then(|j| j.get("offset").and_then(|x| x.as_u64()))
        .map(|u| u as u32)
        .unwrap_or(0);

    let mut total = 0usize;
    let batch = cfg.markets_batch_size;

    loop {
        let url = format!(
            "{}/markets?order=createdAt&ascending=true&limit={}&offset={}",
            cfg.gamma_origin, batch, current_offset
        );
        tracing::info!(%url, "fetch markets batch");
        let resp = client.get(&url).send().await?;
        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            continue;
        }
        if !resp.status().is_success() {
            anyhow::bail!("markets HTTP {}: {}", resp.status(), resp.text().await.unwrap_or_default());
        }
        let markets: Vec<Value> = resp.json().await?;
        if markets.is_empty() {
            break;
        }

        let mut batch_count = 0usize;
        for market in &markets {
            if upsert_one_market(pool, market).await? {
                batch_count += 1;
            }
        }
        total += batch_count;
        current_offset += markets.len() as u32;
        checkpoint::save(pool, PIPELINE_KEY, json!({ "offset": current_offset })).await?;
        tracing::info!(batch_count, current_offset, "markets batch done");

        if (markets.len() as u32) < batch {
            break;
        }
    }

    tracing::info!(total, "ingest-markets (gamma http) finished");
    Ok(total)
}

/// 与 Gamma HTTP 响应同形状的 JSON（供 PMA `markets` Parquet 映射复用）。
pub async fn upsert_from_gamma_like_json(pool: &PgPool, market: &Value) -> anyhow::Result<bool> {
    upsert_one_market(pool, market).await
}

async fn upsert_one_market(pool: &PgPool, market: &Value) -> anyhow::Result<bool> {
    let id = market.get("id").and_then(|v| v.as_str()).unwrap_or("");
    if id.is_empty() {
        return Ok(false);
    }

    let outcomes: Vec<String> = parse_json_array_field(market.get("outcomes"));
    let answer1 = outcomes.first().cloned().unwrap_or_default();
    let answer2 = outcomes.get(1).cloned().unwrap_or_default();

    let clob: Vec<String> = parse_json_array_field(market.get("clobTokenIds"));
    let token1 = clob.first().cloned().unwrap_or_default();
    let token2 = clob.get(1).cloned().unwrap_or_default();

    let neg_risk = market
        .get("negRiskAugmented")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
        || market
            .get("negRiskOther")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

    let question = market
        .get("question")
        .and_then(|v| v.as_str())
        .or_else(|| market.get("title").and_then(|v| v.as_str()))
        .unwrap_or("");

    let ticker = market
        .get("events")
        .and_then(|e| e.as_array())
        .and_then(|a| a.first())
        .and_then(|ev| ev.get("ticker"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let volume = parse_numeric(market.get("volume"));
    let condition_id = market
        .get("conditionId")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let slug = market.get("slug").and_then(|v| v.as_str()).unwrap_or("");
    let closed_time = parse_datetime(market.get("closedTime"));
    let created_at = parse_datetime(market.get("createdAt"));

    sqlx::query(
        r#"
        INSERT INTO dim_markets (
            market_id, condition_id, market_slug, question, answer1, answer2,
            neg_risk, token1, token2, volume, ticker, closed_time, created_at, fetched_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW())
        ON CONFLICT (market_id) DO UPDATE SET
            condition_id = EXCLUDED.condition_id,
            market_slug = EXCLUDED.market_slug,
            question = EXCLUDED.question,
            answer1 = EXCLUDED.answer1,
            answer2 = EXCLUDED.answer2,
            neg_risk = EXCLUDED.neg_risk,
            token1 = EXCLUDED.token1,
            token2 = EXCLUDED.token2,
            volume = EXCLUDED.volume,
            ticker = EXCLUDED.ticker,
            closed_time = EXCLUDED.closed_time,
            created_at = EXCLUDED.created_at,
            fetched_at = NOW()
        "#,
    )
    .bind(id)
    .bind(condition_id)
    .bind(slug)
    .bind(question)
    .bind(&answer1)
    .bind(&answer2)
    .bind(neg_risk)
    .bind(&token1)
    .bind(&token2)
    .bind(volume)
    .bind(ticker)
    .bind(closed_time)
    .bind(created_at)
    .execute(pool)
    .await?;

    Ok(true)
}

fn parse_json_array_field(v: Option<&Value>) -> Vec<String> {
    let Some(v) = v else {
        return Vec::new();
    };
    if let Some(s) = v.as_str() {
        if let Ok(arr) = serde_json::from_str::<Vec<String>>(s) {
            return arr;
        }
    }
    if let Some(arr) = v.as_array() {
        return arr
            .iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect();
    }
    Vec::new()
}

fn parse_numeric(v: Option<&Value>) -> Option<rust_decimal::Decimal> {
    use std::str::FromStr;
    v.and_then(|x| {
        if let Some(f) = x.as_f64() {
            return rust_decimal::Decimal::try_from(f).ok();
        }
        x.as_str()
            .and_then(|s| rust_decimal::Decimal::from_str(s).ok())
    })
}

fn parse_datetime(v: Option<&Value>) -> Option<DateTime<Utc>> {
    v.and_then(|x| {
        x.as_str()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.with_timezone(&Utc))
    })
}
