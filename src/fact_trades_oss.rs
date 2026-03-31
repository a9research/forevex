//! Build OSS `fact_trades` Parquet from OSS PMA `trades` Parquet (no PG `fact_trades`).
//!
//! Output (per-source-file, user choice 1B):
//! - input:  `polymarket/trades/trades_{a}_{b}.parquet`
//! - output: `polymarket/fact_trades/trades_{a}_{b}.parquet`
//!
//! Cursor (OSS, user choice 2A):
//! - `polymarket/.fact_trades_cursor` contains `last_completed_file=<input key>`

use crate::config::Config;
use crate::pma;
use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use indicatif::{ProgressBar, ProgressStyle};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

const CURSOR_KEY: &str = "polymarket/.fact_trades_cursor";
const SCALE: i64 = 1_000_000;

fn fact_trades_prefix(cfg: &Config) -> String {
    pma::oss_polymarket_path(cfg, "polymarket/fact_trades")
}

fn fact_trades_out_key(cfg: &Config, input_norm_key: &str) -> String {
    // input_norm_key is like `polymarket/trades/trades_a_b.parquet`
    let base = input_norm_key
        .rsplit_once('/')
        .map(|(_, f)| f)
        .unwrap_or(input_norm_key);
    let prefix = fact_trades_prefix(cfg).trim_end_matches('/').to_string();
    format!("{prefix}/{base}")
}

async fn read_oss_cursor(
    store: Arc<dyn ObjectStore>,
    cfg: &Config,
) -> anyhow::Result<Option<String>> {
    let key = ObjPath::from(pma::oss_polymarket_path(cfg, CURSOR_KEY).as_str());
    match store.get(&key).await {
        Ok(r) => {
            let b = r.bytes().await?;
            let s = String::from_utf8_lossy(&b);
            for line in s.lines() {
                let line = line.trim();
                if let Some(v) = line.strip_prefix("last_completed_file=") {
                    let v = v.trim();
                    if !v.is_empty() {
                        return Ok(Some(pma::normalize_pma_parquet_relative_key(v)));
                    }
                }
            }
            let s = s.trim();
            if !s.is_empty() {
                return Ok(Some(pma::normalize_pma_parquet_relative_key(s)));
            }
            Ok(None)
        }
        Err(_) => Ok(None),
    }
}

async fn write_oss_cursor(
    store: Arc<dyn ObjectStore>,
    cfg: &Config,
    input_norm_key: &str,
) -> anyhow::Result<()> {
    let key = ObjPath::from(pma::oss_polymarket_path(cfg, CURSOR_KEY).as_str());
    let body = format!("last_completed_file={}\n", input_norm_key);
    store.put(&key, PutPayload::from(body)).await?;
    Ok(())
}

fn parse_amount_scaled(s: &str) -> Option<Decimal> {
    Decimal::from_str(s.trim())
        .ok()
        .map(|d| d / Decimal::from(SCALE))
}

fn fact_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("ts_unix", DataType::Int64, false),
        Field::new("market_id", DataType::Utf8, false),
        Field::new("maker", DataType::Utf8, false),
        Field::new("taker", DataType::Utf8, false),
        Field::new("nonusdc_side", DataType::Utf8, false),
        Field::new("maker_direction", DataType::Utf8, false),
        Field::new("taker_direction", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("usd_amount", DataType::Float64, false),
        Field::new("token_amount", DataType::Float64, false),
        Field::new("transaction_hash", DataType::Utf8, false),
        Field::new("order_hash", DataType::Utf8, false),
        Field::new("log_index", DataType::UInt64, false),
    ]))
}

pub async fn run(pool: &PgPool, cfg: &Config) -> anyhow::Result<usize> {
    if cfg.s3_bucket.is_none() {
        anyhow::bail!(
            "fact-trades-oss requires object storage (PIPELINE_OSS_BUCKET / PIPELINE_S3_BUCKET)"
        );
    }
    let store = pma::build_object_store(cfg)?;

    // asset_id -> (market_id, token_side)
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

    // blocks map
    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::with_template("[{elapsed_precise}] {spinner} {msg}").unwrap());
    pb.enable_steady_tick(std::time::Duration::from_millis(250));
    pb.set_message("fact-trades-oss: listing blocks parquet on OSS");
    let block_objs =
        pma::list_oss_parquet_objects_under_with_store(store.clone(), cfg, "polymarket/blocks")
            .await?;
    pb.set_message("fact-trades-oss: loading block timestamps");
    let mut block_map: HashMap<i64, i64> = HashMap::new();
    for (path, _) in block_objs {
        let bytes = store.get(&path).await?.bytes().await?;
        merge_blocks_parquet_bytes(&bytes, &mut block_map)?;
    }
    pb.finish_and_clear();

    let last_done = read_oss_cursor(store.clone(), cfg).await?;

    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::with_template("[{elapsed_precise}] {spinner} {msg}").unwrap());
    pb.enable_steady_tick(std::time::Duration::from_millis(250));
    pb.set_message("fact-trades-oss: listing trades parquet on OSS");
    let trade_objs =
        pma::list_oss_parquet_objects_under_with_store(store.clone(), cfg, "polymarket/trades")
            .await?;
    pb.finish_and_clear();

    let total_files = trade_objs.len();
    let pb_files = ProgressBar::new(total_files as u64);
    pb_files.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} fact_trades_files {msg}",
        )
        .unwrap()
        .progress_chars("=>-"),
    );

    let mut written_files = 0usize;
    for (obj_path, norm_key) in trade_objs {
        if pma::should_skip_pma_file(&norm_key, last_done.as_deref()) {
            pb_files.inc(1);
            continue;
        }

        pb_files.set_message(obj_path.to_string());
        let bytes = store.get(&obj_path).await?.bytes().await?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;

        let out_key = fact_trades_out_key(cfg, &norm_key);
        let mut out_buf: Vec<u8> = Vec::new();
        {
            let schema = fact_schema();
            let mut w = ArrowWriter::try_new(&mut out_buf, schema.clone(), None)?;
            for batch in reader {
                let batch = batch?;
                let out = trade_batch_to_fact_batch(&batch, &block_map, &asset_to_market)?;
                if out.num_rows() > 0 {
                    w.write(&out)?;
                }
            }
            w.close()?;
        }

        // Only upload if we produced a valid parquet (could be 0 rows but still a file).
        store
            .put(
                &ObjPath::from(out_key.as_str()),
                PutPayload::from(bytes::Bytes::from(out_buf)),
            )
            .await?;

        write_oss_cursor(store.clone(), cfg, &norm_key).await?;
        written_files += 1;

        pb_files.inc(1);
    }

    pb_files.finish_with_message(format!("written_files={}", written_files));
    Ok(written_files)
}

fn trade_batch_to_fact_batch(
    batch: &RecordBatch,
    block_map: &HashMap<i64, i64>,
    asset_to_market: &HashMap<String, (String, String)>,
) -> anyhow::Result<RecordBatch> {
    let n = batch.num_rows();

    let mut ts_unix: Vec<i64> = Vec::new();
    let mut market_id: Vec<String> = Vec::new();
    let mut maker: Vec<String> = Vec::new();
    let mut taker: Vec<String> = Vec::new();
    let mut nonusdc_side: Vec<String> = Vec::new();
    let mut maker_direction: Vec<String> = Vec::new();
    let mut taker_direction: Vec<String> = Vec::new();
    let mut price: Vec<f64> = Vec::new();
    let mut usd_amount: Vec<f64> = Vec::new();
    let mut token_amount: Vec<f64> = Vec::new();
    let mut tx_hash: Vec<String> = Vec::new();
    let mut order_hash: Vec<String> = Vec::new();
    let mut log_index: Vec<u64> = Vec::new();

    for i in 0..n {
        let block = col_i64(batch, "block_number", i).unwrap_or(0);
        let ts_sec = block_map.get(&block).copied().unwrap_or(0);
        if ts_sec <= 0 {
            continue;
        }

        let maker_v = col_str(batch, "maker", i).unwrap_or_default();
        let taker_v = col_str(batch, "taker", i).unwrap_or_default();
        let tx = col_str(batch, "transaction_hash", i).unwrap_or_default();
        if maker_v.is_empty() || taker_v.is_empty() || tx.is_empty() {
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

        let nonusdc_asset_id = if maker_asset != "0" {
            &maker_asset
        } else {
            &taker_asset
        };
        let (mkt, side) = match asset_to_market.get(nonusdc_asset_id) {
            Some(v) => v.clone(),
            None => continue,
        };

        let maker_asset_side = if maker_asset == "0" {
            "USDC".to_string()
        } else {
            side.clone()
        };
        let taker_asset_side = if taker_asset == "0" {
            "USDC".to_string()
        } else {
            side.clone()
        };

        let taker_dir = if taker_asset_side == "USDC" {
            "BUY"
        } else {
            "SELL"
        };
        let maker_dir = if taker_asset_side == "USDC" {
            "SELL"
        } else {
            "BUY"
        };

        let nonusdc = if maker_asset_side != "USDC" {
            maker_asset_side.clone()
        } else {
            taker_asset_side.clone()
        };

        let usd = if taker_asset_side == "USDC" { ta } else { ma };
        let tok = if taker_asset_side != "USDC" { ta } else { ma };

        let p = if taker_asset_side == "USDC" {
            if ma.is_zero() {
                continue;
            }
            (ta / ma).to_f64().unwrap_or(0.0)
        } else {
            if ta.is_zero() {
                continue;
            }
            (ma / ta).to_f64().unwrap_or(0.0)
        };
        if p <= 0.0 {
            continue;
        }

        ts_unix.push(ts_sec);
        market_id.push(mkt);
        maker.push(maker_v);
        taker.push(taker_v);
        nonusdc_side.push(nonusdc);
        maker_direction.push(maker_dir.to_string());
        taker_direction.push(taker_dir.to_string());
        price.push(p);
        usd_amount.push(usd.to_f64().unwrap_or(0.0));
        token_amount.push(tok.to_f64().unwrap_or(0.0));
        tx_hash.push(tx);
        order_hash.push(col_str(batch, "order_hash", i).unwrap_or_default());
        log_index.push(col_u64(batch, "log_index", i).unwrap_or(0));
    }

    let rb = RecordBatch::try_new(
        fact_schema(),
        vec![
            Arc::new(arrow_array::Int64Array::from(ts_unix)),
            Arc::new(arrow_array::StringArray::from(market_id)),
            Arc::new(arrow_array::StringArray::from(maker)),
            Arc::new(arrow_array::StringArray::from(taker)),
            Arc::new(arrow_array::StringArray::from(nonusdc_side)),
            Arc::new(arrow_array::StringArray::from(maker_direction)),
            Arc::new(arrow_array::StringArray::from(taker_direction)),
            Arc::new(arrow_array::Float64Array::from(price)),
            Arc::new(arrow_array::Float64Array::from(usd_amount)),
            Arc::new(arrow_array::Float64Array::from(token_amount)),
            Arc::new(arrow_array::StringArray::from(tx_hash)),
            Arc::new(arrow_array::StringArray::from(order_hash)),
            Arc::new(arrow_array::UInt64Array::from(log_index)),
        ],
    )?;
    Ok(rb)
}

fn merge_blocks_parquet_bytes(
    bytes: &bytes::Bytes,
    map: &mut HashMap<i64, i64>,
) -> anyhow::Result<()> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?.build()?;
    for batch in reader {
        let batch = batch?;
        let n = batch.num_rows();
        for i in 0..n {
            let bn = col_i64(&batch, "block_number", i);
            let ts_iso = col_str(&batch, "timestamp", i);
            if let (Some(bn), Some(ts)) = (bn, ts_iso) {
                if let Some(sec) = crate::pma::parse_ts_iso_to_unix(&ts) {
                    map.insert(bn, sec);
                }
            }
        }
    }
    Ok(())
}

fn col_str(batch: &RecordBatch, name: &str, row: usize) -> Option<String> {
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

fn col_i64(batch: &RecordBatch, name: &str, row: usize) -> Option<i64> {
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

fn col_u64(batch: &RecordBatch, name: &str, row: usize) -> Option<u64> {
    let c = batch.column_by_name(name)?;
    if let Some(a) = c.as_any().downcast_ref::<arrow_array::UInt64Array>() {
        if a.is_null(row) {
            return None;
        }
        return Some(a.value(row));
    }
    if let Some(a) = c.as_any().downcast_ref::<arrow_array::Int64Array>() {
        if a.is_null(row) {
            return None;
        }
        return Some(a.value(row).max(0) as u64);
    }
    None
}

fn col_amount_str(batch: &RecordBatch, name: &str, row: usize) -> String {
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
