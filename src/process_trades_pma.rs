//! Process OSS PMA Parquet directly into `fact_trades` (skip PG `stg_order_filled`).
//!
//! Inputs:
//! - OSS `polymarket/blocks/*.parquet` for `block_number -> unix_seconds`
//! - OSS `polymarket/trades/*.parquet` for OrderFilled events
//!
//! Checkpoint:
//! - `etl_checkpoint.pipeline = process_trades_pma`
//! - `cursor_json.last_completed_file = <normalized object key>`

use crate::checkpoint;
use crate::config::Config;
use crate::pma;
use chrono::{TimeZone, Utc};
use arrow_array::Array;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::collections::HashMap;
use std::str::FromStr;

const PIPELINE_KEY: &str = "process_trades_pma";
const SCALE: i64 = 1_000_000;

pub async fn run(pool: &PgPool, cfg: &Config) -> anyhow::Result<usize> {
    if cfg.s3_bucket.is_none() {
        anyhow::bail!("process-trades-pma requires object storage (PIPELINE_OSS_BUCKET / PIPELINE_S3_BUCKET)");
    }

    let store = pma::build_object_store(cfg)?;

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

    let block_objs =
        pma::list_oss_parquet_objects_under_with_store(store.clone(), cfg, "polymarket/blocks").await?;
    let mut block_map: HashMap<i64, i64> = HashMap::new();
    for (path, _) in block_objs {
        let bytes = store.get(&path).await?.bytes().await?;
        merge_blocks_parquet_bytes(&bytes, &mut block_map)?;
    }
    tracing::info!(blocks = block_map.len(), "process-trades-pma: loaded block timestamps from object store");

    let trade_objs =
        pma::list_oss_parquet_objects_under_with_store(store.clone(), cfg, "polymarket/trades").await?;
    let cursor = checkpoint::load(pool, PIPELINE_KEY).await?;
    let last_done = cursor
        .as_ref()
        .and_then(|j| j.get("last_completed_file").and_then(|x| x.as_str()))
        .map(|s| pma::normalize_pma_parquet_relative_key(s));

    let total_files = trade_objs.len();
    let mut processed_files = 0usize;
    let mut inserted_total = 0usize;

    for (obj_path, norm_key) in trade_objs {
        if pma::should_skip_pma_file(&norm_key, last_done.as_deref()) {
            continue;
        }

        processed_files += 1;
        let bytes = store.get(&obj_path).await?.bytes().await?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
        let mut file_rows = 0usize;
        for batch in reader {
            let batch = batch?;
            file_rows += ingest_trade_batch_to_fact(pool, &batch, &block_map, &asset_to_market).await?;
        }

        inserted_total += file_rows;
        checkpoint::save(
            pool,
            PIPELINE_KEY,
            serde_json::json!({ "last_completed_file": norm_key }),
        )
        .await?;

        tracing::info!(
            key = %obj_path,
            rows = file_rows,
            inserted_total,
            processed_files,
            total_files,
            "process-trades-pma: trades file done"
        );
    }

    tracing::info!(inserted_total, "process-trades-pma finished");
    Ok(inserted_total)
}

fn parse_amount_scaled(s: &str) -> Option<Decimal> {
    Decimal::from_str(s.trim()).ok().map(|d| d / Decimal::from(SCALE))
}

async fn ingest_trade_batch_to_fact(
    pool: &PgPool,
    batch: &arrow_array::RecordBatch,
    block_map: &HashMap<i64, i64>,
    asset_to_market: &HashMap<String, (String, String)>,
) -> anyhow::Result<usize> {
    let n = batch.num_rows();
    let mut inserted = 0usize;

    for i in 0..n {
        let block = col_i64(batch, "block_number", i).unwrap_or(0);
        let ts_sec = block_map.get(&block).copied().unwrap_or(0);
        if ts_sec <= 0 {
            continue;
        }

        let maker = col_str(batch, "maker", i).unwrap_or_default();
        let taker = col_str(batch, "taker", i).unwrap_or_default();
        let tx = col_str(batch, "transaction_hash", i).unwrap_or_default();
        if maker.is_empty() || taker.is_empty() || tx.is_empty() {
            continue;
        }

        let maker_asset = col_str(batch, "maker_asset_id", i).unwrap_or_else(|| "0".to_string());
        let taker_asset = col_str(batch, "taker_asset_id", i).unwrap_or_else(|| "0".to_string());
        let maf = col_amount_str(batch, "maker_amount", i);
        let taf = col_amount_str(batch, "taker_amount", i);

        let ma = match parse_amount_scaled(&maf) {
            Some(v) => v,
            None => continue,
        };
        let ta = match parse_amount_scaled(&taf) {
            Some(v) => v,
            None => continue,
        };

        let nonusdc_asset_id = if maker_asset != "0" { &maker_asset } else { &taker_asset };
        let (market_id, side) = match asset_to_market.get(nonusdc_asset_id) {
            Some(v) => v.clone(),
            None => continue,
        };

        let maker_asset_side = if maker_asset == "0" { "USDC".to_string() } else { side.clone() };
        let taker_asset_side = if taker_asset == "0" { "USDC".to_string() } else { side.clone() };

        let taker_direction = if taker_asset_side == "USDC" { "BUY" } else { "SELL" };
        let maker_direction = if taker_asset_side == "USDC" { "SELL" } else { "BUY" };

        let nonusdc_side = if maker_asset_side != "USDC" {
            maker_asset_side.clone()
        } else {
            taker_asset_side.clone()
        };

        let usd_amount = if taker_asset_side == "USDC" { ta } else { ma };
        let token_amount = if taker_asset_side != "USDC" { ta } else { ma };

        let price = if taker_asset_side == "USDC" {
            if ma.is_zero() {
                continue;
            }
            ta / ma
        } else {
            if ta.is_zero() {
                continue;
            }
            ma / ta
        };

        let ts = match Utc.timestamp_opt(ts_sec, 0).single() {
            Some(v) => v,
            None => continue,
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
        .bind(ts)
        .bind(&market_id)
        .bind(&maker)
        .bind(&taker)
        .bind(&nonusdc_side)
        .bind(maker_direction)
        .bind(taker_direction)
        .bind(price)
        .bind(usd_amount)
        .bind(token_amount)
        .bind(&tx)
        .execute(pool)
        .await?;

        if res.rows_affected() > 0 {
            inserted += 1;
        }
    }

    Ok(inserted)
}

fn merge_blocks_parquet_bytes(bytes: &bytes::Bytes, map: &mut HashMap<i64, i64>) -> anyhow::Result<()> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?.build()?;
    for batch in reader {
        let batch = batch?;
        merge_blocks_batch(&batch, map)?;
    }
    Ok(())
}

fn merge_blocks_batch(batch: &arrow_array::RecordBatch, map: &mut HashMap<i64, i64>) -> anyhow::Result<()> {
    let n = batch.num_rows();
    for i in 0..n {
        let bn = col_i64(batch, "block_number", i);
        let ts_iso = col_str(batch, "timestamp", i);
        if let (Some(bn), Some(ts)) = (bn, ts_iso) {
            if let Some(sec) = parse_ts_iso_to_unix(&ts) {
                map.insert(bn, sec);
            }
        }
    }
    Ok(())
}

fn parse_ts_iso_to_unix(ts: &str) -> Option<i64> {
    let t = ts.trim();
    if t.is_empty() {
        return None;
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(t) {
        return Some(dt.timestamp());
    }
    let with_tz = if t.ends_with('Z') {
        format!("{}+00:00", &t[..t.len().saturating_sub(1)])
    } else {
        t.to_string()
    };
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&with_tz) {
        return Some(dt.timestamp());
    }
    None
}

fn col_str(batch: &arrow_array::RecordBatch, name: &str, row: usize) -> Option<String> {
    let c = batch.column_by_name(name)?;
    if let Some(a) = c.as_any().downcast_ref::<arrow_array::StringArray>() {
        if a.is_null(row) {
            return None;
        }
        return Some(a.value(row).to_string());
    }
    if let Some(a) = c.as_any().downcast_ref::<arrow_array::LargeStringArray>() {
        if a.is_null(row) {
            return None;
        }
        return Some(a.value(row).to_string());
    }
    None
}

fn col_i64(batch: &arrow_array::RecordBatch, name: &str, row: usize) -> Option<i64> {
    let c = batch.column_by_name(name)?;
    if let Some(a) = c.as_any().downcast_ref::<arrow_array::Int64Array>() {
        if a.is_null(row) {
            return None;
        }
        return Some(a.value(row));
    }
    if let Some(a) = c.as_any().downcast_ref::<arrow_array::UInt64Array>() {
        if a.is_null(row) {
            return None;
        }
        return Some(a.value(row) as i64);
    }
    None
}

fn col_amount_str(batch: &arrow_array::RecordBatch, name: &str, row: usize) -> String {
    let c = match batch.column_by_name(name) {
        Some(c) => c,
        None => return "0".to_string(),
    };
    if let Some(a) = c.as_any().downcast_ref::<arrow_array::Int64Array>() {
        if a.is_null(row) {
            return "0".to_string();
        }
        return a.value(row).to_string();
    }
    if let Some(a) = c.as_any().downcast_ref::<arrow_array::UInt64Array>() {
        if a.is_null(row) {
            return "0".to_string();
        }
        return a.value(row).to_string();
    }
    if let Some(a) = c.as_any().downcast_ref::<arrow_array::StringArray>() {
        if a.is_null(row) {
            return "0".to_string();
        }
        return a.value(row).to_string();
    }
    if let Some(a) = c.as_any().downcast_ref::<arrow_array::LargeStringArray>() {
        if a.is_null(row) {
            return "0".to_string();
        }
        return a.value(row).to_string();
    }
    "0".to_string()
}

