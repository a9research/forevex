//! Ingest [prediction-market-analysis](https://github.com/jon-becker/prediction-market-analysis) Polymarket
//! `OrderFilled` Parquet into `stg_order_filled`.
//!
//! **对象存储为唯一 raw 源**：本地 `polymarket/**/*.parquet` 仅作解压/增量暂存；配置 bucket 时先 **上传 OSS 再删本地**，再从 OSS 读入 Postgres。

use crate::checkpoint;
use crate::config::Config;
use arrow_array::array::{Array, Int64Array, StringArray, UInt64Array};
use arrow_array::RecordBatch;
use futures_util::StreamExt;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sqlx::PgPool;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use walkdir::WalkDir;

pub const PIPELINE_KEY: &str = "pma_order_filled";

pub async fn run(pool: &PgPool, cfg: &Config) -> anyhow::Result<usize> {
    if cfg.s3_bucket.is_some() {
        let uploaded = sync_local_polymarket_to_oss_and_delete(cfg).await?;
        if uploaded > 0 {
            tracing::info!(
                uploaded,
                "PMA: local parquet uploaded to object store and removed from disk"
            );
        }
        ingest_from_object_store(pool, cfg).await
    } else if cfg.pma_allow_local_only {
        tracing::warn!(
            "PMA: PIPELINE_PMA_ALLOW_LOCAL_ONLY=1 — ingesting from local Parquet only (not for production)"
        );
        ingest_from_local_dir(pool, cfg).await
    } else {
        anyhow::bail!(
            "PMA raw data must use object storage: set PIPELINE_OSS_BUCKET or PIPELINE_S3_BUCKET \
             (and endpoint/keys for Aliyun OSS). For dev-only local Parquet, set PIPELINE_PMA_ALLOW_LOCAL_ONLY=1"
        );
    }
}

fn pma_root(cfg: &Config) -> PathBuf {
    cfg.pma_data_dir.clone()
}

/// Build S3-compatible `ObjectStore` (阿里云 OSS、MinIO、R2 等).
pub fn build_object_store(cfg: &Config) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let bucket = cfg
        .s3_bucket
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("object store bucket required"))?;
    use object_store::aws::AmazonS3Builder;
    let mut b = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(cfg.s3_region.as_deref().unwrap_or("us-east-1"));
    if cfg.s3_virtual_hosted {
        b = b.with_virtual_hosted_style_request(true);
    }
    if let Some(ep) = &cfg.s3_endpoint {
        b = b.with_endpoint(ep);
        if ep.starts_with("http://") {
            b = b.with_allow_http(true);
        }
    }
    if let (Some(k), Some(s)) = (&cfg.s3_access_key_id, &cfg.s3_secret_access_key) {
        b = b.with_access_key_id(k).with_secret_access_key(s);
    }
    Ok(Arc::new(b.build()?))
}

/// 对象键前缀（不含首尾 `/`），与上传路径一致。
pub fn s3_object_prefix(cfg: &Config) -> String {
    cfg.s3_prefix
        .as_deref()
        .unwrap_or("")
        .trim_matches('/')
        .to_string()
}

/// 解析含 `polymarket/trades` 的数据根目录（兼容 tar 顶层 `polymarket/` 或 `data/polymarket/`）。
pub fn resolve_polymarket_data_root(data_dir: &Path) -> anyhow::Result<PathBuf> {
    let a = data_dir.join("polymarket").join("trades");
    if a.is_dir() {
        return Ok(data_dir.to_path_buf());
    }
    let b = data_dir.join("data").join("polymarket").join("trades");
    if b.is_dir() {
        return Ok(data_dir.join("data"));
    }
    anyhow::bail!(
        "could not find polymarket/trades under {} or {}/data",
        data_dir.display(),
        data_dir.display()
    )
}

/// 将本地 `polymarket/**/*.parquet` **全部上传**到对象存储，成功后 **删除**本地 `polymarket/` 树。
/// 无 bucket 或未找到目录时返回 `Ok(0)`。
pub async fn sync_local_polymarket_to_oss_and_delete(cfg: &Config) -> anyhow::Result<u64> {
    if cfg.s3_bucket.is_none() {
        return Ok(0);
    }
    let base = match resolve_polymarket_data_root(&cfg.pma_data_dir) {
        Ok(b) => b,
        Err(_) => return Ok(0),
    };
    let poly = base.join("polymarket");
    if !poly.exists() {
        return Ok(0);
    }

    let store = build_object_store(cfg)?;
    let prefix = s3_object_prefix(cfg);

    let mut paths: Vec<PathBuf> = WalkDir::new(&poly)
        .into_iter()
        .filter_map(|e| e.ok())
        .map(|e| e.path().to_path_buf())
        .filter(|p| {
            p.is_file()
                && p.extension()
                    .and_then(|x| x.to_str())
                    .map(|e| e.eq_ignore_ascii_case("parquet"))
                    .unwrap_or(false)
        })
        .collect();
    paths.sort();

    if paths.is_empty() {
        return Ok(0);
    }

    tracing::info!(
        count = paths.len(),
        dest = %poly.display(),
        "PMA: uploading parquet files to object store"
    );

    for path in &paths {
        let rel = path.strip_prefix(&base)?;
        let key_str = rel.to_string_lossy().replace('\\', "/");
        let object_key = if prefix.is_empty() {
            key_str
        } else {
            format!("{prefix}/{key_str}")
        };
        let buf = tokio::fs::read(path).await?;
        store
            .put(
                &ObjPath::from(object_key.as_str()),
                PutPayload::from(bytes::Bytes::from(buf)),
            )
            .await?;
    }

    for path in &paths {
        tokio::fs::remove_file(path).await.ok();
    }

    let poly_tree = base.join("polymarket");
    if poly_tree.exists() {
        let p = poly_tree.clone();
        tokio::task::spawn_blocking(move || std::fs::remove_dir_all(&p))
            .await??;
    }

    Ok(paths.len() as u64)
}

/// Load block_number -> unix seconds from all `polymarket/blocks/*.parquet` under `base`.
fn load_block_timestamps_local(base: &Path) -> anyhow::Result<HashMap<i64, i64>> {
    let mut map = HashMap::new();
    let dir = base.join("polymarket/blocks");
    if !dir.is_dir() {
        tracing::warn!(
            path = %dir.display(),
            "PMA blocks directory missing — timestamps may be wrong (use 0)"
        );
        return Ok(map);
    }
    for entry in WalkDir::new(&dir).max_depth(1).into_iter().filter_map(|e| e.ok()) {
        let p = entry.path();
        if p.is_file() && p.extension().map(|x| x == "parquet").unwrap_or(false) {
            merge_blocks_parquet_file(p, &mut map)?;
        }
    }
    tracing::info!(blocks = map.len(), "loaded block timestamps from local PMA blocks");
    Ok(map)
}

fn merge_blocks_parquet_file(path: &Path, map: &mut HashMap<i64, i64>) -> anyhow::Result<()> {
    let file = std::fs::File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    for batch in reader {
        let batch = batch?;
        merge_blocks_batch(&batch, map)?;
    }
    Ok(())
}

fn merge_blocks_batch(batch: &RecordBatch, map: &mut HashMap<i64, i64>) -> anyhow::Result<()> {
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
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%SZ"))
    {
        let dt = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(dt, chrono::Utc);
        return Some(dt.timestamp());
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

fn col_i64(batch: &RecordBatch, name: &str, row: usize) -> Option<i64> {
    let c = batch.column_by_name(name)?;
    if let Some(a) = c.as_any().downcast_ref::<Int64Array>() {
        if a.is_null(row) {
            return None;
        }
        return Some(a.value(row));
    }
    if let Some(a) = c.as_any().downcast_ref::<UInt64Array>() {
        if a.is_null(row) {
            return None;
        }
        return Some(a.value(row) as i64);
    }
    None
}

fn col_amount_str(batch: &RecordBatch, name: &str, row: usize) -> String {
    let c = match batch.column_by_name(name) {
        Some(c) => c,
        None => return "0".to_string(),
    };
    if let Some(a) = c.as_any().downcast_ref::<Int64Array>() {
        if a.is_null(row) {
            return "0".to_string();
        }
        return a.value(row).to_string();
    }
    if let Some(a) = c.as_any().downcast_ref::<UInt64Array>() {
        if a.is_null(row) {
            return "0".to_string();
        }
        return a.value(row).to_string();
    }
    if let Some(a) = c.as_any().downcast_ref::<StringArray>() {
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

fn list_local_trade_parquets(base: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let dir = base.join("polymarket/trades");
    if !dir.is_dir() {
        anyhow::bail!(
            "PMA trades directory not found: {} (set PIPELINE_PMA_DATA_DIR or run bootstrap-data)",
            dir.display()
        );
    }
    let mut v: Vec<PathBuf> = WalkDir::new(&dir)
        .max_depth(1)
        .into_iter()
        .filter_map(|e| e.ok())
        .map(|e| e.path().to_path_buf())
        .filter(|p| p.is_file() && p.extension().map(|x| x == "parquet").unwrap_or(false))
        .collect();
    v.sort();
    Ok(v)
}

fn rel_path_for_checkpoint(path: &Path, base: &Path) -> String {
    path.strip_prefix(base)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

async fn ingest_from_local_dir(pool: &PgPool, cfg: &Config) -> anyhow::Result<usize> {
    let base = pma_root(cfg);
    let block_map = load_block_timestamps_local(&base)?;
    let files = list_local_trade_parquets(&base)?;
    let cursor = checkpoint::load(pool, PIPELINE_KEY).await?;
    let last_done = cursor
        .as_ref()
        .and_then(|j| j.get("last_completed_file").and_then(|x| x.as_str()))
        .map(String::from);

    let mut total = 0usize;
    let mut started = last_done.is_none();
    for path in &files {
        let rel = rel_path_for_checkpoint(path, &base);
        if !started {
            if Some(rel.as_str()) == last_done.as_deref() {
                started = true;
            }
            continue;
        }
        let n = ingest_one_trade_parquet_file(pool, path, &block_map).await?;
        total += n;
        checkpoint::save(
            pool,
            PIPELINE_KEY,
            serde_json::json!({ "last_completed_file": rel }),
        )
        .await?;
        tracing::info!(file = %path.display(), rows = n, "PMA trades file done");
    }

    tracing::info!(total, "ingest-pma finished (local-only)");
    Ok(total)
}

async fn ingest_one_trade_parquet_file(
    pool: &PgPool,
    path: &Path,
    block_map: &HashMap<i64, i64>,
) -> anyhow::Result<usize> {
    let file = std::fs::File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut inserted = 0usize;
    for batch in reader {
        let batch = batch?;
        inserted += ingest_trade_batch(pool, &batch, block_map).await?;
    }
    Ok(inserted)
}

async fn ingest_trade_batch(
    pool: &PgPool,
    batch: &RecordBatch,
    block_map: &HashMap<i64, i64>,
) -> anyhow::Result<usize> {
    let n = batch.num_rows();
    let mut inserted = 0usize;
    for i in 0..n {
        let block = col_i64(batch, "block_number", i).unwrap_or(0);
        let ts = block_map.get(&block).copied().unwrap_or(0);
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

        let r = sqlx::query(
            r#"
            INSERT INTO stg_order_filled (
                timestamp_i64, maker, maker_asset_id, maker_amount_filled,
                taker, taker_asset_id, taker_amount_filled, transaction_hash
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (transaction_hash, maker, taker, timestamp_i64, maker_asset_id, taker_asset_id) DO NOTHING
            "#,
        )
        .bind(ts)
        .bind(&maker)
        .bind(&maker_asset)
        .bind(&maf)
        .bind(&taker)
        .bind(&taker_asset)
        .bind(&taf)
        .bind(&tx)
        .execute(pool)
        .await?;
        if r.rows_affected() > 0 {
            inserted += 1;
        }
    }
    Ok(inserted)
}

async fn ingest_from_object_store(pool: &PgPool, cfg: &Config) -> anyhow::Result<usize> {
    let store = build_object_store(cfg)?;
    let prefix = s3_object_prefix(cfg);
    let blocks_key = if prefix.is_empty() {
        "polymarket/blocks".to_string()
    } else {
        format!("{prefix}/polymarket/blocks")
    };

    let mut block_map = HashMap::new();
    let mut list = store.list(Some(&ObjPath::from(blocks_key.as_str())));
    while let Some(res) = list.next().await {
        let meta = res?;
        if !meta.location.as_ref().ends_with(".parquet") {
            continue;
        }
        let bytes = store.get(&meta.location).await?.bytes().await?;
        merge_blocks_parquet_bytes(&bytes, &mut block_map)?;
    }
    tracing::info!(blocks = block_map.len(), "loaded block timestamps from object store");

    let trades_prefix = if prefix.is_empty() {
        "polymarket/trades".to_string()
    } else {
        format!("{prefix}/polymarket/trades")
    };
    let mut trade_paths: Vec<ObjPath> = Vec::new();
    let mut list = store.list(Some(&ObjPath::from(trades_prefix.as_str())));
    while let Some(res) = list.next().await {
        let meta = res?;
        if meta.location.as_ref().ends_with(".parquet") {
            trade_paths.push(meta.location);
        }
    }
    trade_paths.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));

    let cursor = checkpoint::load(pool, PIPELINE_KEY).await?;
    let last_done = cursor
        .as_ref()
        .and_then(|j| j.get("last_completed_file").and_then(|x| x.as_str()))
        .map(String::from);

    let mut total = 0usize;
    let mut started = last_done.is_none();
    for path in trade_paths {
        let key = path.as_ref().to_string();
        if !started {
            if Some(key.as_str()) == last_done.as_deref() {
                started = true;
            }
            continue;
        }
        let bytes = store.get(&path).await?.bytes().await?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
        let mut file_rows = 0usize;
        for batch in reader {
            let batch = batch?;
            file_rows += ingest_trade_batch(pool, &batch, &block_map).await?;
        }
        total += file_rows;
        checkpoint::save(
            pool,
            PIPELINE_KEY,
            serde_json::json!({ "last_completed_file": key }),
        )
        .await?;
        tracing::info!(key = %path, rows = file_rows, "PMA trades object done");
    }

    tracing::info!(total, "ingest-pma (object store) finished");
    Ok(total)
}

fn merge_blocks_parquet_bytes(bytes: &bytes::Bytes, map: &mut HashMap<i64, i64>) -> anyhow::Result<()> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?.build()?;
    for batch in reader {
        let batch = batch?;
        merge_blocks_batch(&batch, map)?;
    }
    Ok(())
}
