//! OSS `polymarket/markets/*.parquet` → `dim_markets`（规格 §4 主路径）。

use crate::checkpoint;
use crate::config::Config;
use crate::ingest_markets::upsert_from_gamma_like_json;
use crate::pma::{self, should_skip_pma_file};
use arrow_array::array::{Array, StringArray};
use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};
use sqlx::PgPool;

const PIPELINE_KEY: &str = "ingest_markets_pma";

pub async fn run_from_oss_parquet(pool: &PgPool, cfg: &Config) -> anyhow::Result<usize> {
    let store = pma::build_object_store(cfg)?;
    let objects = pma::list_oss_parquet_objects_under(cfg, "polymarket/markets").await?;
    let cursor = checkpoint::load(pool, PIPELINE_KEY).await?;
    let last_done = cursor
        .as_ref()
        .and_then(|j| j.get("last_completed_file").and_then(|x| x.as_str()))
        .map(|s| pma::normalize_pma_parquet_relative_key(s));

    let mut total = 0usize;
    for (obj_path, norm_key) in objects {
        if should_skip_pma_file(&norm_key, last_done.as_deref()) {
            continue;
        }
        let bytes = store.get(&obj_path).await?.bytes().await?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
        let mut file_rows = 0usize;
        for batch in reader {
            let batch = batch?;
            file_rows += ingest_markets_batch(pool, &batch).await?;
        }
        total += file_rows;
        checkpoint::save(
            pool,
            PIPELINE_KEY,
            json!({ "last_completed_file": norm_key }),
        )
        .await?;
        tracing::info!(key = %obj_path, rows = file_rows, "markets parquet file done");
    }

    tracing::info!(total, "ingest-markets (OSS parquet) finished");
    Ok(total)
}

async fn ingest_markets_batch(pool: &PgPool, batch: &RecordBatch) -> anyhow::Result<usize> {
    let n = batch.num_rows();
    let mut ok = 0usize;
    for i in 0..n {
        if let Some(v) = batch_row_to_gamma_json(batch, i) {
            if upsert_from_gamma_like_json(pool, &v).await? {
                ok += 1;
            }
        }
    }
    Ok(ok)
}

fn batch_row_to_gamma_json(batch: &RecordBatch, row: usize) -> Option<Value> {
    let id = col_str_any(batch, row, &["id", "market_id"])?;
    if id.is_empty() {
        return None;
    }

    let condition_id = col_str_any(batch, row, &["conditionId", "condition_id"])
        .unwrap_or_default();
    let slug = col_str_any(batch, row, &["slug", "market_slug"]).unwrap_or_default();
    let question = col_str_any(batch, row, &["question", "title"]).unwrap_or_default();

    let outcomes_raw = col_str_any(batch, row, &["outcomes"]);
    let outcomes: Value = outcomes_raw
        .as_ref()
        .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
        .map(|v| json!(v))
        .unwrap_or_else(|| json!([]));

    let clob_raw = col_str_any(batch, row, &["clobTokenIds", "clob_token_ids"]);
    let clob: Value = clob_raw
        .as_ref()
        .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
        .map(|v| json!(v))
        .unwrap_or_else(|| json!([]));

    let neg_risk = col_bool_any(batch, row, &["negRiskAugmented", "neg_risk", "negRisk"])
        .unwrap_or(false);

    let volume = col_str_any(batch, row, &["volume"]);
    let vol_val = volume.as_ref().and_then(|s| s.parse::<f64>().ok());

    let closed_time = col_str_any(batch, row, &["closedTime", "closed_time"]);
    let created_at = col_str_any(batch, row, &["createdAt", "created_at"]);

    let mut m = json!({
        "id": id,
        "conditionId": condition_id,
        "slug": slug,
        "question": question,
        "outcomes": outcomes,
        "clobTokenIds": clob,
        "negRiskAugmented": neg_risk,
    });
    if let Some(v) = vol_val {
        m.as_object_mut()?.insert("volume".to_string(), json!(v));
    }
    if let Some(s) = closed_time {
        if !s.is_empty() {
            m.as_object_mut()?.insert("closedTime".to_string(), json!(s));
        }
    }
    if let Some(s) = created_at {
        if !s.is_empty() {
            m.as_object_mut()?.insert("createdAt".to_string(), json!(s));
        }
    }
    Some(m)
}

fn col_str_any(batch: &RecordBatch, row: usize, names: &[&str]) -> Option<String> {
    for name in names {
        if let Some(s) = col_str(batch, *name, row) {
            if !s.is_empty() {
                return Some(s);
            }
        }
    }
    None
}

fn col_str(batch: &RecordBatch, name: &str, row: usize) -> Option<String> {
    let c = batch.column_by_name(name)?;
    if let Some(a) = c.as_any().downcast_ref::<StringArray>() {
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

fn col_bool_any(batch: &RecordBatch, row: usize, names: &[&str]) -> Option<bool> {
    for name in names {
        let c = batch.column_by_name(*name)?;
        if let Some(a) = c.as_any().downcast_ref::<arrow_array::BooleanArray>() {
            if a.is_null(row) {
                continue;
            }
            return Some(a.value(row));
        }
    }
    None
}
