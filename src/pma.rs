//! Ingest [prediction-market-analysis](https://github.com/jon-becker/prediction-market-analysis) Polymarket
//! `OrderFilled` Parquet into `stg_order_filled`.
//!
//! **对象存储为唯一 raw 源**：本地 `polymarket/**/*.parquet` 仅作解压/增量暂存；配置 bucket 时先 **上传 OSS 再删本地**，再从 OSS 读入 Postgres。
//!
//! **四目录一致规则**（`polymarket/trades|blocks|markets|legacy_trades`）：列举、入库、统计、清理均 **忽略** macOS 旁路文件 **`._*.parquet`**（见 `is_apple_double_parquet_object_key`）。`ingest-markets`（OSS Parquet）与 `oss-apple-double` 等同理。

use crate::checkpoint;
use crate::config::Config;
use arrow_array::array::{Array, Int64Array, StringArray, UInt64Array};
use arrow_array::RecordBatch;
use futures_util::StreamExt;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use walkdir::WalkDir;

pub const PIPELINE_KEY: &str = "pma_order_filled";

/// 已按字典序完成到 `last_done`（含该文件）；仅处理 **严格大于** `last_done` 的文件。
/// `last_done == None` 表示全量（尚未记录进度）。
pub fn should_skip_pma_file(norm_key: &str, last_done: Option<&str>) -> bool {
    match last_done {
        None => false,
        Some(ld) => norm_key <= ld,
    }
}

/// Tarballs may only contain `._blocks_....parquet` (macOS AppleDouble sidecars bundled into PMA archives).
/// Strip the leading `._` on the **filename** so OSS keys and checkpoints match canonical `blocks_....parquet` names.
pub fn normalize_pma_parquet_relative_key(key: &str) -> String {
    if let Some((dir, file)) = key.rsplit_once('/') {
        if file.starts_with("._") && file.len() > 2 {
            return format!("{}/{}", dir, &file[2..]);
        }
    } else if key.starts_with("._") && key.len() > 2 {
        return key[2..].to_string();
    }
    key.to_string()
}

/// macOS 在拷贝到网络盘/OSS 时产生的 **AppleDouble** 旁路文件（通常极小，非 Parquet 数据体）。
/// 管线应 **完全忽略**，不列入清单、不读入、不上传；清理任务可安全删除。
pub fn is_apple_double_parquet_object_key(loc: &str) -> bool {
    let base = loc.rsplit_once('/').map(|(_, f)| f).unwrap_or(loc);
    base.starts_with("._") && base.ends_with(".parquet")
}

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

/// 将 `oss-cn-xxx.aliyuncs.com` 这类无协议主机名规范为 `https://...`，避免 reqwest `RelativeUrlWithoutBase`。
fn normalize_s3_endpoint(raw: &str) -> anyhow::Result<String> {
    let s = raw.trim();
    if s.is_empty() {
        anyhow::bail!("PIPELINE_OSS_ENDPOINT / PIPELINE_S3_ENDPOINT is empty");
    }
    let with_scheme = if s.starts_with("http://") || s.starts_with("https://") {
        s.to_string()
    } else {
        format!("https://{}", s.trim_start_matches('/'))
    };
    url::Url::parse(&with_scheme).map_err(|e| {
        anyhow::anyhow!(
            "invalid object store endpoint {with_scheme:?}: {e} — use e.g. https://oss-ap-northeast-1.aliyuncs.com"
        )
    })?;
    Ok(with_scheme)
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
        let ep = normalize_s3_endpoint(ep)?;
        b = b.with_endpoint(&ep);
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
                && !p
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("._"))
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
        let raw = rel.to_string_lossy().replace('\\', "/");
        let key_str = normalize_pma_parquet_relative_key(&raw);
        if key_str != raw {
            tracing::info!(from = %raw, to = %key_str, "PMA: normalized parquet object key (._ prefix)");
        }
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
        if p.is_file()
            && p.extension().map(|x| x == "parquet").unwrap_or(false)
            && !p
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("._"))
                .unwrap_or(false)
        {
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
        .filter(|p| {
            p.is_file()
                && p.extension().map(|x| x == "parquet").unwrap_or(false)
                && !p
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("._"))
                    .unwrap_or(false)
        })
        .collect();
    v.sort();
    Ok(v)
}

fn rel_path_for_checkpoint(path: &Path, base: &Path) -> String {
    let raw = path
        .strip_prefix(base)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/");
    normalize_pma_parquet_relative_key(&raw)
}

async fn ingest_from_local_dir(pool: &PgPool, cfg: &Config) -> anyhow::Result<usize> {
    let base = pma_root(cfg);
    let block_map = load_block_timestamps_local(&base)?;
    let files = list_local_trade_parquets(&base)?;
    let cursor = checkpoint::load(pool, PIPELINE_KEY).await?;
    let last_done = cursor
        .as_ref()
        .and_then(|j| j.get("last_completed_file").and_then(|x| x.as_str()))
        .map(|s| normalize_pma_parquet_relative_key(s));

    let mut total = 0usize;
    for path in &files {
        let rel = rel_path_for_checkpoint(path, &base);
        if should_skip_pma_file(&rel, last_done.as_deref()) {
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

/// `polymarket/trades` 等子路径（可带 `s3_prefix`）。
pub fn oss_polymarket_path(cfg: &Config, polymarket_suffix: &str) -> String {
    let prefix = s3_object_prefix(cfg);
    let suffix = polymarket_suffix.trim_matches('/');
    if prefix.is_empty() {
        suffix.to_string()
    } else {
        format!("{prefix}/{suffix}")
    }
}

/// 列出 OSS 下某目录内所有真实 `.parquet`（**跳过** `._*.parquet`），按规范化键排序。  
/// 供 `trades` / `blocks` / `markets` / `legacy_trades` 及 `ingest-pma` 共用。
pub async fn list_oss_parquet_objects_under_with_store(
    store: Arc<dyn ObjectStore>,
    cfg: &Config,
    polymarket_subdir: &str,
) -> anyhow::Result<Vec<(ObjPath, String)>> {
    let prefix = oss_polymarket_path(cfg, polymarket_subdir);
    let mut out = Vec::new();
    let mut list = store.list(Some(&ObjPath::from(prefix.as_str())));
    while let Some(res) = list.next().await {
        let meta = res?;
        let loc = meta.location.as_ref();
        if loc.ends_with(".parquet") && !is_apple_double_parquet_object_key(loc) {
            let norm = normalize_pma_parquet_relative_key(loc);
            out.push((meta.location, norm));
        }
    }
    out.sort_by(|a, b| a.1.cmp(&b.1));
    Ok(out)
}

async fn ingest_from_object_store(pool: &PgPool, cfg: &Config) -> anyhow::Result<usize> {
    let store = build_object_store(cfg)?;
    let mut block_map = HashMap::new();
    let block_objs =
        list_oss_parquet_objects_under_with_store(store.clone(), cfg, "polymarket/blocks").await?;
    for (path, _) in block_objs {
        let bytes = store.get(&path).await?.bytes().await?;
        merge_blocks_parquet_bytes(&bytes, &mut block_map)?;
    }
    tracing::info!(blocks = block_map.len(), "loaded block timestamps from object store");

    let trade_paths =
        list_oss_parquet_objects_under_with_store(store.clone(), cfg, "polymarket/trades").await?;

    let cursor = checkpoint::load(pool, PIPELINE_KEY).await?;
    let last_done = cursor
        .as_ref()
        .and_then(|j| j.get("last_completed_file").and_then(|x| x.as_str()))
        .map(|s| normalize_pma_parquet_relative_key(s));

    let mut total = 0usize;
    for (path, norm_key) in trade_paths {
        if should_skip_pma_file(&norm_key, last_done.as_deref()) {
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
            serde_json::json!({ "last_completed_file": norm_key }),
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

/// 列出 OSS 下某目录内所有 `.parquet`（`ObjectStore` 路径 + 规范化键），按规范化键排序。
pub async fn list_oss_parquet_objects_under(
    cfg: &Config,
    polymarket_subdir: &str,
) -> anyhow::Result<Vec<(ObjPath, String)>> {
    let store = build_object_store(cfg)?;
    list_oss_parquet_objects_under_with_store(store, cfg, polymarket_subdir).await
}

/// 仅规范化键（用于统计、展示）。
pub async fn list_oss_parquet_keys_under(cfg: &Config, polymarket_subdir: &str) -> anyhow::Result<Vec<String>> {
    let v = list_oss_parquet_objects_under(cfg, polymarket_subdir).await?;
    Ok(v.into_iter().map(|(_, k)| k).collect())
}

/// PMA 进度 + OSS 上 Parquet 文件计数（用于 `status` / HTTP）。
pub async fn pipeline_pma_status(pool: &PgPool, cfg: &Config) -> anyhow::Result<serde_json::Value> {
    let cursor = checkpoint::load(pool, PIPELINE_KEY).await?;
    let last_done = cursor
        .as_ref()
        .and_then(|j| j.get("last_completed_file").and_then(|x| x.as_str()))
        .map(|s| normalize_pma_parquet_relative_key(s));

    if cfg.s3_bucket.is_none() {
        return Ok(serde_json::json!({
            "mode": "no_object_store",
            "pma_order_filled_checkpoint": cursor,
            "hint": "配置 PIPELINE_OSS_BUCKET 后可展示四目录（trades/blocks/markets/legacy_trades）Parquet 文件数与待处理 trades 文件数",
        }));
    }

    let trade_keys = list_oss_parquet_keys_under(cfg, "polymarket/trades").await.unwrap_or_default();
    let block_keys = list_oss_parquet_keys_under(cfg, "polymarket/blocks").await.unwrap_or_default();
    let market_keys = list_oss_parquet_keys_under(cfg, "polymarket/markets").await.unwrap_or_default();
    let legacy_trade_keys =
        list_oss_parquet_keys_under(cfg, "polymarket/legacy_trades").await.unwrap_or_default();
    let pending: usize = trade_keys
        .iter()
        .filter(|k| !should_skip_pma_file(k, last_done.as_deref()))
        .count();

    Ok(serde_json::json!({
        "mode": "object_store",
        "bucket": cfg.s3_bucket,
        "s3_prefix": cfg.s3_prefix,
        "oss_trade_parquet_files": trade_keys.len(),
        "oss_block_parquet_files": block_keys.len(),
        "oss_markets_parquet_files": market_keys.len(),
        "oss_legacy_trades_parquet_files": legacy_trade_keys.len(),
        "trade_files_sample": trade_keys.iter().take(8).collect::<Vec<_>>(),
        "pma_order_filled": {
            "last_completed_file": last_done,
            "pending_trade_files": pending,
            "checkpoint": cursor,
        },
    }))
}

const PMA_OSS_SUBDIRS: &[&str] = &[
    "polymarket/trades",
    "polymarket/blocks",
    "polymarket/markets",
    "polymarket/legacy_trades",
];

/// 扫描 OSS 上所有 `._*.parquet`：是否与「去掉 `._` 后的 canonical 键」对应的 **真实数据文件** 同存。
/// 用于确认 AppleDouble 旁路可删（`canonical_counterpart_exists == true` 时仅为重复元数据）。
pub async fn report_apple_double_parquet_oss(cfg: &Config) -> anyhow::Result<serde_json::Value> {
    let store = build_object_store(cfg)?;
    let mut canonical_norm: HashSet<String> = HashSet::new();
    let mut apple_raw: Vec<(String, usize, String)> = Vec::new();

    for subdir in PMA_OSS_SUBDIRS {
        let prefix = oss_polymarket_path(cfg, subdir);
        let mut list = store.list(Some(&ObjPath::from(prefix.as_str())));
        while let Some(res) = list.next().await {
            let meta = res?;
            let loc = meta.location.as_ref();
            if !loc.ends_with(".parquet") {
                continue;
            }
            if is_apple_double_parquet_object_key(loc) {
                apple_raw.push((
                    loc.to_string(),
                    meta.size,
                    normalize_pma_parquet_relative_key(loc),
                ));
            } else {
                canonical_norm.insert(normalize_pma_parquet_relative_key(loc));
            }
        }
    }

    let mut apple = Vec::new();
    let mut orphan_only: Vec<String> = Vec::new();
    for (key, size, norm) in apple_raw {
        let exists = canonical_norm.contains(&norm);
        if !exists {
            orphan_only.push(key.clone());
        }
        apple.push(serde_json::json!({
            "key": key,
            "size": size,
            "normalized_key": norm,
            "canonical_counterpart_exists": exists,
        }));
    }

    Ok(serde_json::json!({
        "bucket": cfg.s3_bucket,
        "s3_prefix": cfg.s3_prefix,
        "note": "._*.parquet 为 macOS AppleDouble 旁路，非 PMA 数据本体；管线已忽略。可删除 canonical 已存在时的旁路。",
        "apple_double_parquet_count": apple.len(),
        "canonical_parquet_distinct_keys": canonical_norm.len(),
        "apple_double_without_counterpart": orphan_only,
        "apple_double_files": apple,
    }))
}

/// 删除 OSS 上所有 `._*.parquet`。`dry_run: true` 时只返回将删除的键，不调用 delete。
pub async fn delete_apple_double_parquet_oss(
    cfg: &Config,
    dry_run: bool,
) -> anyhow::Result<(usize, Vec<String>)> {
    let store = build_object_store(cfg)?;
    let mut targets: Vec<ObjPath> = Vec::new();
    for subdir in PMA_OSS_SUBDIRS {
        let prefix = oss_polymarket_path(cfg, subdir);
        let mut list = store.list(Some(&ObjPath::from(prefix.as_str())));
        while let Some(res) = list.next().await {
            let meta = res?;
            let loc = meta.location.as_ref();
            if loc.ends_with(".parquet") && is_apple_double_parquet_object_key(loc) {
                targets.push(meta.location);
            }
        }
    }
    let mut paths: Vec<String> = targets.iter().map(|p| p.to_string()).collect();
    paths.sort();

    if dry_run {
        return Ok((0, paths));
    }

    let mut deleted = 0usize;
    for loc in targets {
        store.delete(&loc).await?;
        deleted += 1;
    }
    Ok((deleted, paths))
}

#[cfg(test)]
mod tests {
    use super::{is_apple_double_parquet_object_key, normalize_pma_parquet_relative_key, should_skip_pma_file};

    #[test]
    fn should_skip_respects_lexicographic_order() {
        let ld = Some("polymarket/trades/b.parquet");
        assert!(should_skip_pma_file("polymarket/trades/a.parquet", ld));
        assert!(should_skip_pma_file("polymarket/trades/b.parquet", ld));
        assert!(!should_skip_pma_file("polymarket/trades/c.parquet", ld));
        assert!(!should_skip_pma_file("polymarket/trades/c.parquet", None));
    }

    #[test]
    fn apple_double_detection() {
        assert!(is_apple_double_parquet_object_key(
            "polymarket/markets/._markets_40000_50000.parquet"
        ));
        assert!(!is_apple_double_parquet_object_key(
            "polymarket/markets/markets_40000_50000.parquet"
        ));
    }

    #[test]
    fn normalize_strips_dot_underscore_prefix_on_filename() {
        assert_eq!(
            normalize_pma_parquet_relative_key("polymarket/blocks/._blocks_10000000_10100000.parquet"),
            "polymarket/blocks/blocks_10000000_10100000.parquet"
        );
        assert_eq!(
            normalize_pma_parquet_relative_key("polymarket/trades/._trades_0_10000.parquet"),
            "polymarket/trades/trades_0_10000.parquet"
        );
        assert_eq!(
            normalize_pma_parquet_relative_key("polymarket/blocks/blocks_1_2.parquet"),
            "polymarket/blocks/blocks_1_2.parquet"
        );
    }
}
