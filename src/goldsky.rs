//! Goldsky GraphQL `orderFilledEvents` — aligned with poly_data `update_goldsky.py` (sticky `timestamp` + `id_gt`).

use crate::checkpoint;
use crate::config::Config;
use reqwest::Client;
use serde_json::{json, Value};
use sqlx::PgPool;

use crate::checkpoint::PIPELINE_GOLDSKY_ORDER_FILLED as PIPELINE_KEY;
const BATCH: i32 = 1000;

#[derive(Debug, Default, Clone)]
struct Cursor {
    last_timestamp: i64,
    last_id: Option<String>,
    sticky_timestamp: Option<i64>,
}

impl Cursor {
    fn from_json(j: &Value) -> Self {
        Self {
            last_timestamp: j.get("last_timestamp").and_then(|x| x.as_i64()).unwrap_or(0),
            last_id: j
                .get("last_id")
                .and_then(|x| x.as_str())
                .map(String::from),
            sticky_timestamp: j.get("sticky_timestamp").and_then(|x| x.as_i64()),
        }
    }

    fn to_json(&self) -> Value {
        json!({
            "last_timestamp": self.last_timestamp,
            "last_id": self.last_id,
            "sticky_timestamp": self.sticky_timestamp,
        })
    }
}

pub async fn run(pool: &PgPool, cfg: &Config) -> anyhow::Result<usize> {
    let client = Client::builder()
        .timeout(cfg.http_timeout)
        .build()?;

    let mut cur = load_cursor(pool).await?;
    let mut total = 0usize;

    loop {
        let where_clause = if let Some(sticky_ts) = cur.sticky_timestamp {
            let lid = cur.last_id.as_deref().unwrap_or("");
            format!(r#"timestamp: "{}", id_gt: "{}""#, sticky_ts, lid)
        } else {
            format!(r#"timestamp_gt: "{}""#, cur.last_timestamp)
        };

        let query = format!(
            r#"query {{
                orderFilledEvents(
                    orderBy: timestamp
                    orderDirection: asc
                    first: {batch}
                    where: {{ {where_clause} }}
                ) {{
                    id
                    timestamp
                    maker
                    makerAmountFilled
                    makerAssetId
                    taker
                    takerAmountFilled
                    takerAssetId
                    transactionHash
                }}
            }}"#,
            batch = BATCH,
            where_clause = where_clause
        );

        let body = json!({ "query": query });
        let resp = client
            .post(&cfg.goldsky_graphql_url)
            .json(&body)
            .send()
            .await?;
        if !resp.status().is_success() {
            anyhow::bail!("Goldsky HTTP {}: {}", resp.status(), resp.text().await?);
        }
        let v: Value = resp.json().await?;
        if let Some(err) = v.get("errors") {
            anyhow::bail!("GraphQL errors: {}", err);
        }
        let events = v
            .pointer("/data/orderFilledEvents")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();

        if events.is_empty() {
            if cur.sticky_timestamp.is_some() {
                cur.last_timestamp = cur.sticky_timestamp.unwrap_or(cur.last_timestamp);
                cur.sticky_timestamp = None;
                cur.last_id = None;
                save_cursor(pool, &cur).await?;
                continue;
            }
            tracing::info!("goldsky: no more events");
            break;
        }

        let mut parsed: Vec<(String, i64, Value)> = Vec::with_capacity(events.len());
        for ev in &events {
            let id = ev
                .get("id")
                .map(|x| x.to_string())
                .unwrap_or_default();
            let ts = ev
                .get("timestamp")
                .and_then(|x| x.as_str())
                .and_then(|s| s.parse::<i64>().ok())
                .or_else(|| ev.get("timestamp").and_then(|x| x.as_i64()))
                .unwrap_or(0);
            parsed.push((id, ts, ev.clone()));
        }
        parsed.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

        let batch_last_ts = parsed.last().map(|p| p.1).unwrap_or(0);
        let batch_last_id = parsed.last().map(|p| p.0.clone()).unwrap_or_default();

        for (_id, ts, ev) in &parsed {
            let maker = ev.get("maker").and_then(|x| x.as_str()).unwrap_or("");
            let taker = ev.get("taker").and_then(|x| x.as_str()).unwrap_or("");
            let mah = ev.get("makerAssetId");
            let maker_asset_id = mah
                .and_then(|x| x.as_str())
                .map(String::from)
                .or_else(|| mah.and_then(|x| x.as_i64()).map(|n| n.to_string()))
                .unwrap_or_default();
            let tah = ev.get("takerAssetId");
            let taker_asset_id = tah
                .and_then(|x| x.as_str())
                .map(String::from)
                .or_else(|| tah.and_then(|x| x.as_i64()).map(|n| n.to_string()))
                .unwrap_or_default();
            let maf = ev
                .get("makerAmountFilled")
                .map(|x| x.to_string())
                .unwrap_or_default();
            let taf = ev
                .get("takerAmountFilled")
                .map(|x| x.to_string())
                .unwrap_or_default();
            let tx = ev
                .get("transactionHash")
                .and_then(|x| x.as_str())
                .unwrap_or("");

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
            .bind(maker)
            .bind(&maker_asset_id)
            .bind(&maf)
            .bind(taker)
            .bind(&taker_asset_id)
            .bind(&taf)
            .bind(tx)
            .execute(pool)
            .await?;
            if r.rows_affected() > 0 {
                total += 1;
            }
        }

        let n = parsed.len();
        if n >= BATCH as usize {
            // Full batch: may need more pages at `batch_last_ts` (poly_data sticky semantics).
            cur.sticky_timestamp = Some(batch_last_ts);
            cur.last_id = Some(batch_last_id);
        } else if cur.sticky_timestamp.is_some() {
            cur.last_timestamp = cur.sticky_timestamp.unwrap_or(batch_last_ts);
            cur.sticky_timestamp = None;
            cur.last_id = None;
        } else {
            cur.last_timestamp = batch_last_ts;
        }

        save_cursor(pool, &cur).await?;

        if n < BATCH as usize && cur.sticky_timestamp.is_none() {
            break;
        }
    }

    tracing::info!(total, "ingest-goldsky finished (rows inserted)");
    Ok(total)
}

async fn load_cursor(pool: &PgPool) -> anyhow::Result<Cursor> {
    let j = checkpoint::load(pool, PIPELINE_KEY).await?;
    Ok(j.map(|v| Cursor::from_json(&v)).unwrap_or_default())
}

async fn save_cursor(pool: &PgPool, cur: &Cursor) -> anyhow::Result<()> {
    checkpoint::save(pool, PIPELINE_KEY, cur.to_json()).await
}
