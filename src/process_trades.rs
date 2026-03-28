//! Process `stg_order_filled` + `dim_markets` → `fact_trades` (same logic as poly_data `get_processed_df`).

use crate::config::Config;
use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::collections::HashMap;
use std::str::FromStr;

const PIPELINE_KEY: &str = "process_trades";

const SCALE: i64 = 1_000_000;

pub async fn run(pool: &PgPool, _cfg: &Config) -> anyhow::Result<usize> {
    let last_id: i64 = load_checkpoint(pool)
        .await?
        .unwrap_or(0);

    let market_rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT market_id, token1, token2 FROM dim_markets WHERE token1 <> '' AND token2 <> ''",
    )
    .fetch_all(pool)
    .await?;

    let mut asset_to_market: HashMap<String, (String, String)> = HashMap::new();
    for (market_id, t1, t2) in market_rows {
        asset_to_market.insert(t1.clone(), (market_id.clone(), "token1".to_string()));
        asset_to_market.insert(t2, (market_id, "token2".to_string()));
    }

    let rows: Vec<StgRow> = sqlx::query_as(
        r#"
        SELECT id, timestamp_i64, maker, maker_asset_id, maker_amount_filled,
               taker, taker_asset_id, taker_amount_filled, transaction_hash
        FROM stg_order_filled
        WHERE id > $1
        ORDER BY id ASC
        LIMIT 50000
        "#,
    )
    .bind(last_id)
    .fetch_all(pool)
    .await?;

    let mut inserted = 0usize;
    let mut max_id = last_id;

    for r in &rows {
        max_id = max_id.max(r.id);
        let Some(ft) = process_one(r, &asset_to_market) else {
            continue;
        };

        let res = sqlx::query(
            r#"
            INSERT INTO fact_trades (
                ts, market_id, maker, taker, nonusdc_side, maker_direction, taker_direction,
                price, usd_amount, token_amount, transaction_hash, source
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'polymarket_pipeline')
            ON CONFLICT (transaction_hash, maker, taker, ts) DO NOTHING
            "#,
        )
        .bind(ft.ts)
        .bind(&ft.market_id)
        .bind(&ft.maker)
        .bind(&ft.taker)
        .bind(&ft.nonusdc_side)
        .bind(&ft.maker_direction)
        .bind(&ft.taker_direction)
        .bind(ft.price)
        .bind(ft.usd_amount)
        .bind(ft.token_amount)
        .bind(&ft.transaction_hash)
        .execute(pool)
        .await?;

        if res.rows_affected() > 0 {
            inserted += 1;
        }
    }

    if max_id > last_id {
        save_checkpoint(pool, max_id).await?;
    }

    tracing::info!(inserted, max_id, "process-trades finished");
    Ok(inserted)
}

#[derive(sqlx::FromRow)]
struct StgRow {
    id: i64,
    timestamp_i64: i64,
    maker: String,
    maker_asset_id: String,
    maker_amount_filled: String,
    taker: String,
    taker_asset_id: String,
    taker_amount_filled: String,
    transaction_hash: String,
}

struct FactRow {
    ts: chrono::DateTime<Utc>,
    market_id: String,
    maker: String,
    taker: String,
    nonusdc_side: String,
    maker_direction: String,
    taker_direction: String,
    price: Decimal,
    usd_amount: Decimal,
    token_amount: Decimal,
    transaction_hash: String,
}

fn process_one(r: &StgRow, asset_to_market: &HashMap<String, (String, String)>) -> Option<FactRow> {
    let nonusdc_asset_id = if r.maker_asset_id != "0" {
        &r.maker_asset_id
    } else {
        &r.taker_asset_id
    };

    let (market_id, side) = asset_to_market.get(nonusdc_asset_id)?;
    let market_id = market_id.clone();
    let side = side.clone();

    let maker_asset = if r.maker_asset_id == "0" {
        "USDC".to_string()
    } else {
        side.clone()
    };
    let taker_asset = if r.taker_asset_id == "0" {
        "USDC".to_string()
    } else {
        side.clone()
    };

    let ma = parse_amount(&r.maker_amount_filled).ok()? / Decimal::from(SCALE);
    let ta = parse_amount(&r.taker_amount_filled).ok()? / Decimal::from(SCALE);

    let taker_direction = if taker_asset == "USDC" {
        "BUY"
    } else {
        "SELL"
    };
    let maker_direction = if taker_asset == "USDC" {
        "SELL"
    } else {
        "BUY"
    };

    let nonusdc_side = if maker_asset != "USDC" {
        maker_asset.clone()
    } else {
        taker_asset.clone()
    };

    let usd_amount = if taker_asset == "USDC" { ta } else { ma };
    let token_amount = if taker_asset != "USDC" { ta } else { ma };

    let price = if taker_asset == "USDC" {
        if ma.is_zero() {
            return None;
        }
        ta / ma
    } else {
        if ta.is_zero() {
            return None;
        }
        ma / ta
    };

    let ts = Utc.timestamp_opt(r.timestamp_i64, 0).single()?;

    Some(FactRow {
        ts,
        market_id,
        maker: r.maker.clone(),
        taker: r.taker.clone(),
        nonusdc_side,
        maker_direction: maker_direction.to_string(),
        taker_direction: taker_direction.to_string(),
        price,
        usd_amount,
        token_amount,
        transaction_hash: r.transaction_hash.clone(),
    })
}

fn parse_amount(s: &str) -> anyhow::Result<Decimal> {
    Decimal::from_str(s.trim()).map_err(|e| anyhow::anyhow!("amount parse: {e}"))
}

async fn load_checkpoint(pool: &PgPool) -> anyhow::Result<Option<i64>> {
    let row: Option<(serde_json::Value,)> =
        sqlx::query_as("SELECT cursor_json FROM etl_checkpoint WHERE pipeline = $1")
            .bind(PIPELINE_KEY)
            .fetch_optional(pool)
            .await?;
    Ok(row.and_then(|(j,)| j.get("last_stg_id").and_then(|x| x.as_i64())))
}

async fn save_checkpoint(pool: &PgPool, last_stg_id: i64) -> anyhow::Result<()> {
    let cursor = serde_json::json!({ "last_stg_id": last_stg_id });
    sqlx::query(
        r#"
        INSERT INTO etl_checkpoint (pipeline, cursor_json, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (pipeline) DO UPDATE SET cursor_json = EXCLUDED.cursor_json, updated_at = NOW()
        "#,
    )
    .bind(PIPELINE_KEY)
    .bind(cursor)
    .execute(pool)
    .await?;
    Ok(())
}
