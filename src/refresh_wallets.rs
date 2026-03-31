//! Rebuild `dim_wallets` from OSS `fact_trades` Parquet (maker + taker).

use crate::config::Config;
use crate::pma;
use arrow_array::Array;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sqlx::PgPool;
use std::collections::HashMap;

pub async fn run(pool: &PgPool, cfg: &Config) -> anyhow::Result<u64> {
    if cfg.s3_bucket.is_none() {
        anyhow::bail!("refresh-wallets requires object storage fact_trades (PIPELINE_OSS_BUCKET / PIPELINE_S3_BUCKET)");
    }
    let store = pma::build_object_store(cfg)?;
    let objs = pma::list_oss_parquet_objects_under_with_store(
        store.clone(),
        cfg,
        "polymarket/fact_trades",
    )
    .await?;

    let mut m: HashMap<String, (i64, i64)> = HashMap::new(); // wallet -> (min_ts,max_ts)
    for (path, _) in objs {
        let bytes = store.get(&path).await?.bytes().await?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
        for batch in reader {
            let batch = batch?;
            let n = batch.num_rows();
            for i in 0..n {
                let ts = col_i64(&batch, "ts_unix", i).unwrap_or(0);
                if ts <= 0 {
                    continue;
                }
                let maker = col_str(&batch, "maker", i)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let taker = col_str(&batch, "taker", i)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                if !maker.is_empty() {
                    upsert_range(&mut m, &maker, ts);
                }
                if !taker.is_empty() {
                    upsert_range(&mut m, &taker, ts);
                }
            }
        }
    }

    if m.is_empty() {
        return Ok(0);
    }

    let mut addrs: Vec<String> = Vec::with_capacity(m.len());
    let mut firsts: Vec<chrono::DateTime<chrono::Utc>> = Vec::with_capacity(m.len());
    let mut lasts: Vec<chrono::DateTime<chrono::Utc>> = Vec::with_capacity(m.len());
    for (addr, (min_ts, max_ts)) in m {
        if let (Some(a), Some(b)) = (
            chrono::DateTime::<chrono::Utc>::from_timestamp(min_ts, 0),
            chrono::DateTime::<chrono::Utc>::from_timestamp(max_ts, 0),
        ) {
            addrs.push(addr);
            firsts.push(a);
            lasts.push(b);
        }
    }

    let r = sqlx::query(
        r#"
        INSERT INTO dim_wallets (proxy_address, first_seen_at, last_seen_at)
        SELECT x.proxy_address, x.first_seen_at, x.last_seen_at
        FROM UNNEST($1::text[], $2::timestamptz[], $3::timestamptz[]) AS x(proxy_address, first_seen_at, last_seen_at)
        ON CONFLICT (proxy_address) DO UPDATE SET
            first_seen_at = LEAST(dim_wallets.first_seen_at, EXCLUDED.first_seen_at),
            last_seen_at = GREATEST(dim_wallets.last_seen_at, EXCLUDED.last_seen_at)
        "#,
    )
    .bind(&addrs)
    .bind(&firsts)
    .bind(&lasts)
    .execute(pool)
    .await?;

    Ok(r.rows_affected())
}

fn upsert_range(m: &mut HashMap<String, (i64, i64)>, addr: &str, ts: i64) {
    m.entry(addr.to_string())
        .and_modify(|(min_ts, max_ts)| {
            *min_ts = (*min_ts).min(ts);
            *max_ts = (*max_ts).max(ts);
        })
        .or_insert((ts, ts));
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
