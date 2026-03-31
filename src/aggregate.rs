//! Rebuild `agg_wallet_topic` and `agg_global_daily` from OSS `fact_trades` Parquet (maker-only volume).

use crate::config::Config;
use crate::pma;
use arrow_array::Array;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sqlx::PgPool;
use std::collections::HashMap;

pub async fn run(pool: &PgPool, cfg: &Config) -> anyhow::Result<()> {
    if cfg.s3_bucket.is_none() {
        anyhow::bail!(
            "aggregate requires object storage fact_trades (PIPELINE_OSS_BUCKET / PIPELINE_S3_BUCKET)"
        );
    }

    let store = pma::build_object_store(cfg)?;
    let objs = pma::list_oss_parquet_objects_under_with_store(
        store.clone(),
        cfg,
        "polymarket/fact_trades",
    )
    .await?;

    let market_topics: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT market_id, topic_primary FROM dim_markets")
            .fetch_all(pool)
            .await?;
    let mut topic_by_market: HashMap<String, String> = HashMap::new();
    for (market_id, topic) in market_topics {
        topic_by_market.insert(market_id, topic.unwrap_or_else(|| "other".to_string()));
    }

    let now = chrono::Utc::now().timestamp();
    let cut_7d = now - 7 * 86400;
    let cut_30d = now - 30 * 86400;
    let cut_90d = now - 90 * 86400;

    // (maker, period, topic) -> (vol, count)
    let mut wt: HashMap<(String, String, String), (f64, i64)> = HashMap::new();
    // (date, topic) -> (vol, count)
    let mut gd: HashMap<(chrono::NaiveDate, String), (f64, i64)> = HashMap::new();

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
                if maker.is_empty() {
                    continue;
                }
                let market_id = col_str(&batch, "market_id", i).unwrap_or_default();
                if market_id.is_empty() {
                    continue;
                }
                let topic = topic_by_market
                    .get(&market_id)
                    .cloned()
                    .unwrap_or_else(|| "other".to_string());
                let usd = col_f64(&batch, "usd_amount", i).unwrap_or(0.0);

                add_wt(&mut wt, &maker, "all", &topic, usd);
                if ts >= cut_7d {
                    add_wt(&mut wt, &maker, "7d", &topic, usd);
                }
                if ts >= cut_30d {
                    add_wt(&mut wt, &maker, "30d", &topic, usd);
                }
                if ts >= cut_90d {
                    add_wt(&mut wt, &maker, "90d", &topic, usd);
                }

                if let Some(dt) = chrono::DateTime::<chrono::Utc>::from_timestamp(ts, 0) {
                    let date = dt.date_naive();
                    let e = gd.entry((date, topic)).or_insert((0.0, 0));
                    e.0 += usd;
                    e.1 += 1;
                }
            }
        }
    }

    // share_of_wallet needs totals per (wallet, period)
    let mut totals: HashMap<(String, String), f64> = HashMap::new();
    for ((w, period, _topic), (vol, _cnt)) in &wt {
        *totals.entry((w.clone(), period.clone())).or_insert(0.0) += *vol;
    }

    let mut proxy: Vec<String> = Vec::new();
    let mut topic: Vec<String> = Vec::new();
    let mut period: Vec<String> = Vec::new();
    let mut vol: Vec<f64> = Vec::new();
    let mut cnt: Vec<i64> = Vec::new();
    let mut share: Vec<Option<f64>> = Vec::new();
    for ((w, p, t), (v, c)) in wt {
        let total = totals.get(&(w.clone(), p.clone())).copied().unwrap_or(0.0);
        let s = if total > 0.0 { Some(v / total) } else { None };
        proxy.push(w);
        topic.push(t);
        period.push(p);
        vol.push(v);
        cnt.push(c);
        share.push(s);
    }

    let mut stat_date: Vec<chrono::NaiveDate> = Vec::new();
    let mut g_topic: Vec<String> = Vec::new();
    let mut g_vol: Vec<f64> = Vec::new();
    let mut g_cnt: Vec<i64> = Vec::new();
    let mut g_unique: Vec<Option<i64>> = Vec::new();
    for ((d, t), (v, c)) in gd {
        stat_date.push(d);
        g_topic.push(t);
        g_vol.push(v);
        g_cnt.push(c);
        // exact distinct makers would be huge to compute in-memory; keep NULL for now.
        g_unique.push(None);
    }

    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM agg_wallet_topic")
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM agg_global_daily")
        .execute(&mut *tx)
        .await?;

    if !proxy.is_empty() {
        sqlx::query(
            r#"
            INSERT INTO agg_wallet_topic (
                proxy_address, topic_primary, period, volume_usd, trade_count, share_of_wallet, computed_at
            )
            SELECT x.proxy_address, x.topic_primary, x.period, x.volume_usd, x.trade_count, x.share_of_wallet, NOW()
            FROM UNNEST(
                $1::text[],
                $2::text[],
                $3::text[],
                $4::double precision[],
                $5::bigint[],
                $6::double precision[]
            ) AS x(proxy_address, topic_primary, period, volume_usd, trade_count, share_of_wallet)
            "#,
        )
        .bind(&proxy)
        .bind(&topic)
        .bind(&period)
        .bind(&vol)
        .bind(&cnt)
        .bind(&share)
        .execute(&mut *tx)
        .await?;
    }

    if !stat_date.is_empty() {
        sqlx::query(
            r#"
            INSERT INTO agg_global_daily (
                stat_date, topic_primary, volume_usd, trade_count, unique_wallets
            )
            SELECT x.stat_date, x.topic_primary, x.volume_usd, x.trade_count, x.unique_wallets
            FROM UNNEST(
                $1::date[],
                $2::text[],
                $3::double precision[],
                $4::bigint[],
                $5::bigint[]
            ) AS x(stat_date, topic_primary, volume_usd, trade_count, unique_wallets)
            "#,
        )
        .bind(&stat_date)
        .bind(&g_topic)
        .bind(&g_vol)
        .bind(&g_cnt)
        .bind(&g_unique)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    tracing::info!("aggregate: agg_wallet_topic + agg_global_daily rebuilt (from OSS fact_trades)");
    Ok(())
}

fn add_wt(
    wt: &mut HashMap<(String, String, String), (f64, i64)>,
    maker: &str,
    period: &str,
    topic: &str,
    usd: f64,
) {
    let e = wt
        .entry((maker.to_string(), period.to_string(), topic.to_string()))
        .or_insert((0.0, 0));
    e.0 += usd;
    e.1 += 1;
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

fn col_f64(batch: &arrow_array::RecordBatch, name: &str, row: usize) -> Option<f64> {
    let c = batch.column_by_name(name)?;
    if let Some(a) = c.as_any().downcast_ref::<arrow_array::Float64Array>() {
        if a.is_null(row) {
            return None;
        }
        return Some(a.value(row));
    }
    if let Some(a) = c.as_any().downcast_ref::<arrow_array::Float32Array>() {
        if a.is_null(row) {
            return None;
        }
        return Some(a.value(row) as f64);
    }
    None
}
