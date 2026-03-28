//! Import poly_data archive `orderFilled_complete.csv` / `.csv.xz` into `stg_order_filled`.
//!
//! Columns (poly_data): `timestamp`, `maker`, `makerAssetId`, `makerAmountFilled`, `taker`,
//! `takerAssetId`, `takerAmountFilled`, `transactionHash`.

use crate::checkpoint::{self, PIPELINE_GOLDSKY_ORDER_FILLED};
use csv::StringRecord;
use serde_json::json;
use sqlx::{PgPool, QueryBuilder};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
use std::thread;
use tokio::sync::mpsc;
use xz2::read::XzDecoder;

struct ColIdx {
    ts: usize,
    maker: usize,
    maker_asset_id: usize,
    maker_amount_filled: usize,
    taker: usize,
    taker_asset_id: usize,
    taker_amount_filled: usize,
    tx_hash: usize,
}

impl ColIdx {
    fn from_headers(h: &StringRecord) -> anyhow::Result<Self> {
        let find = |names: &[&str]| -> anyhow::Result<usize> {
            for (i, col) in h.iter().enumerate() {
                let c = col.trim();
                if names.iter().any(|n| c.eq_ignore_ascii_case(n)) {
                    return Ok(i);
                }
            }
            anyhow::bail!("missing column (one of {:?})", names);
        };

        Ok(Self {
            ts: find(&["timestamp"])?,
            maker: find(&["maker"])?,
            maker_asset_id: find(&["makerAssetId", "maker_asset_id"])?,
            maker_amount_filled: find(&["makerAmountFilled", "maker_amount_filled"])?,
            taker: find(&["taker"])?,
            taker_asset_id: find(&["takerAssetId", "taker_asset_id"])?,
            taker_amount_filled: find(&["takerAmountFilled", "taker_amount_filled"])?,
            tx_hash: find(&["transactionHash", "transaction_hash"])?,
        })
    }
}

struct Row {
    timestamp_i64: i64,
    maker: String,
    maker_asset_id: String,
    maker_amount_filled: String,
    taker: String,
    taker_asset_id: String,
    taker_amount_filled: String,
    transaction_hash: String,
}

fn parse_ts(s: &str) -> anyhow::Result<i64> {
    let s = s.trim();
    if s.is_empty() {
        anyhow::bail!("empty timestamp");
    }
    if let Ok(i) = s.parse::<i64>() {
        return Ok(i);
    }
    if let Ok(f) = s.parse::<f64>() {
        return Ok(f as i64);
    }
    anyhow::bail!("bad timestamp: {s:?}")
}

fn parse_row(rec: &StringRecord, col: &ColIdx) -> anyhow::Result<Row> {
    let g = |i: usize| rec.get(i).unwrap_or("").trim().to_string();
    let ts = parse_ts(&g(col.ts))?;
    let maker = g(col.maker);
    let taker = g(col.taker);
    let tx = g(col.tx_hash);
    if maker.is_empty() || taker.is_empty() || tx.is_empty() {
        anyhow::bail!("missing maker/taker/tx");
    }
    Ok(Row {
        timestamp_i64: ts,
        maker,
        maker_asset_id: g(col.maker_asset_id),
        maker_amount_filled: g(col.maker_amount_filled),
        taker,
        taker_asset_id: g(col.taker_asset_id),
        taker_amount_filled: g(col.taker_amount_filled),
        transaction_hash: tx,
    })
}

fn open_reader(path: &Path) -> anyhow::Result<Box<dyn Read + Send>> {
    if path.as_os_str() == "-" {
        return Ok(Box::new(io::stdin()));
    }
    let f = File::open(path)?;
    let is_xz = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.eq_ignore_ascii_case("xz"))
        .unwrap_or(false)
        || path
            .to_str()
            .map(|s| s.ends_with(".csv.xz"))
            .unwrap_or(false);
    if is_xz {
        Ok(Box::new(XzDecoder::new(f)))
    } else {
        Ok(Box::new(f))
    }
}

async fn insert_batch(pool: &PgPool, rows: &[Row]) -> anyhow::Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let mut qb = QueryBuilder::new(
        r#"INSERT INTO stg_order_filled (
            timestamp_i64, maker, maker_asset_id, maker_amount_filled,
            taker, taker_asset_id, taker_amount_filled, transaction_hash
        ) "#,
    );
    qb.push_values(rows.iter(), |mut b, r| {
        b.push_bind(r.timestamp_i64)
            .push_bind(&r.maker)
            .push_bind(&r.maker_asset_id)
            .push_bind(&r.maker_amount_filled)
            .push_bind(&r.taker)
            .push_bind(&r.taker_asset_id)
            .push_bind(&r.taker_amount_filled)
            .push_bind(&r.transaction_hash);
    });
    qb.push(
        r#" ON CONFLICT (transaction_hash, maker, taker, timestamp_i64, maker_asset_id, taker_asset_id) DO NOTHING"#,
    );
    let res = qb.build().execute(pool).await?;
    Ok(res.rows_affected())
}

/// Run import: stream CSV from path or stdin (`-`), then optionally update Goldsky checkpoint cursor.
pub async fn run(
    pool: &PgPool,
    path: &Path,
    batch_size: usize,
    update_checkpoint: bool,
) -> anyhow::Result<u64> {
    let batch_size = batch_size.max(1);
    let path = path.to_path_buf();

    let (tx, mut rx) = mpsc::channel::<Vec<Row>>(4);

    let reader = thread::spawn(move || -> anyhow::Result<()> {
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(open_reader(&path)?);

        let headers = rdr.headers()?.clone();
        let col = ColIdx::from_headers(&headers)?;
        let mut batch = Vec::with_capacity(batch_size);
        let mut line = 0u64;

        for rec in rdr.records() {
            line += 1;
            let rec = match rec {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(line, error = %e, "csv record parse");
                    continue;
                }
            };
            match parse_row(&rec, &col) {
                Ok(row) => batch.push(row),
                Err(e) => {
                    tracing::debug!(line, error = %e, "skip row");
                    continue;
                }
            }
            if batch.len() >= batch_size {
                tx.blocking_send(std::mem::take(&mut batch))
                    .map_err(|_| anyhow::anyhow!("import channel closed"))?;
            }
        }
        if !batch.is_empty() {
            tx.blocking_send(batch)
                .map_err(|_| anyhow::anyhow!("import channel closed"))?;
        }
        Ok(())
    });

    let mut inserted = 0u64;
    while let Some(batch) = rx.recv().await {
        inserted += insert_batch(pool, &batch).await?;
    }

    reader.join().map_err(|_| anyhow::anyhow!("import reader thread panicked"))??;

    tracing::info!(inserted, "import-order-filled-snapshot: rows inserted (skipped duplicates)");

    if update_checkpoint {
        let max_ts: i64 =
            sqlx::query_scalar("SELECT COALESCE(MAX(timestamp_i64), 0) FROM stg_order_filled")
                .fetch_one(pool)
                .await?;
        checkpoint::save(
            pool,
            PIPELINE_GOLDSKY_ORDER_FILLED,
            json!({
                "last_timestamp": max_ts,
                "last_id": null,
                "sticky_timestamp": null
            }),
        )
        .await?;
        tracing::info!(
            max_ts,
            "import-order-filled-snapshot: etl_checkpoint updated for incremental Goldsky"
        );
    }

    Ok(inserted)
}
