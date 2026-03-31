//! PMA-aligned indexer that writes Parquet to OSS and stores cursor files on OSS.
//!
//! This is intentionally OSS-first: it does not depend on Postgres.

use crate::config::Config;
use crate::pma;
use crate::rpc::RotatingRpc;
use arrow_array::array::{Int64Array, StringArray, UInt64Array};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use ethabi::{ParamType, Token};
use indicatif::{ProgressBar, ProgressStyle};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, PutPayload};
use serde_json::Value;
use std::sync::Arc;

const CURSOR_BLOCKS: &str = "polymarket/.backfill_block_cursor";
// PMA trades indexer also uses `.backfill_block_cursor` as the resume pointer.
const CURSOR_TRADES: &str = "polymarket/.backfill_block_cursor";

fn cursor_key(cfg: &Config, rel: &str) -> ObjPath {
    ObjPath::from(pma::oss_polymarket_path(cfg, rel).as_str())
}

async fn read_cursor_u64(
    store: Arc<dyn ObjectStore>,
    cfg: &Config,
    rel: &str,
) -> anyhow::Result<Option<u64>> {
    let key = cursor_key(cfg, rel);
    match store.get(&key).await {
        Ok(r) => {
            let b = r.bytes().await?;
            let s = String::from_utf8_lossy(&b);
            let s = s.trim();
            if s.is_empty() {
                return Ok(None);
            }
            let v: u64 = s.parse()?;
            Ok(Some(v))
        }
        Err(_) => Ok(None),
    }
}

async fn write_cursor_u64(
    store: Arc<dyn ObjectStore>,
    cfg: &Config,
    rel: &str,
    v: u64,
) -> anyhow::Result<()> {
    let key = cursor_key(cfg, rel);
    store.put(&key, PutPayload::from(format!("{v}\n"))).await?;
    Ok(())
}

async fn rpc_block_number(rpc: &RotatingRpc) -> anyhow::Result<u64> {
    let v = rpc.call("eth_blockNumber", serde_json::json!([])).await?;
    let s = v
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("eth_blockNumber: expected hex string, got {v}"))?;
    Ok(u64::from_str_radix(s.trim_start_matches("0x"), 16)?)
}

async fn rpc_get_block_ts(rpc: &RotatingRpc, block: u64) -> anyhow::Result<u64> {
    let hex = format!("0x{:x}", block);
    let v = rpc
        .call("eth_getBlockByNumber", serde_json::json!([hex, false]))
        .await?;
    let ts_hex = v.get("timestamp").and_then(|x| x.as_str()).ok_or_else(|| {
        anyhow::anyhow!("eth_getBlockByNumber: missing timestamp for block={block}: {v}")
    })?;
    Ok(u64::from_str_radix(ts_hex.trim_start_matches("0x"), 16)?)
}

fn ts_to_iso(ts: u64) -> String {
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(ts as i64, 0)
        .unwrap_or_else(|| chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap());
    dt.to_rfc3339()
}

fn blocks_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::Int64, false),
        Field::new("timestamp", DataType::Utf8, false),
    ]))
}

fn trades_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::Int64, false),
        Field::new("log_index", DataType::UInt64, false),
        Field::new("transaction_hash", DataType::Utf8, false),
        Field::new("order_hash", DataType::Utf8, false),
        Field::new("maker", DataType::Utf8, false),
        Field::new("taker", DataType::Utf8, false),
        Field::new("maker_asset_id", DataType::Utf8, false),
        Field::new("taker_asset_id", DataType::Utf8, false),
        Field::new("maker_amount", DataType::Utf8, false),
        Field::new("taker_amount", DataType::Utf8, false),
        Field::new("fee", DataType::Utf8, false),
        Field::new("_contract", DataType::Utf8, false),
        Field::new("_fetched_at", DataType::Utf8, false),
    ]))
}

fn write_parquet_bytes(
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
) -> anyhow::Result<bytes::Bytes> {
    use parquet::arrow::ArrowWriter;
    let mut out = Vec::new();
    {
        let mut w = ArrowWriter::try_new(&mut out, schema, None)?;
        for b in batches {
            w.write(&b)?;
        }
        w.close()?;
    }
    Ok(bytes::Bytes::from(out))
}

pub struct IndexerOptions {
    pub blocks: bool,
    pub trades: bool,
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub chunk_blocks: u64,
    pub blocks_sample_stride: u64,
}

impl Default for IndexerOptions {
    fn default() -> Self {
        Self {
            blocks: true,
            trades: true,
            from_block: None,
            to_block: None,
            chunk_blocks: 10_000,
            blocks_sample_stride: 1_000,
        }
    }
}

pub async fn run_indexer_oss(cfg: &Config, opt: IndexerOptions) -> anyhow::Result<Value> {
    if cfg.s3_bucket.is_none() {
        anyhow::bail!("indexer-oss 需要 PIPELINE_OSS_BUCKET（或 PIPELINE_S3_BUCKET）");
    }
    let store = pma::build_object_store(cfg)?;
    let rpc = RotatingRpc::from_cfg(cfg)?;

    let head = rpc_block_number(&rpc).await?;
    let to_block = opt.to_block.unwrap_or(head);

    let start_from = match opt.from_block {
        Some(v) => v,
        None => read_cursor_u64(store.clone(), cfg, CURSOR_BLOCKS)
            .await?
            .map(|x| x + 1)
            .unwrap_or(0),
    };
    let from_block = start_from.min(to_block);

    let mut summary = serde_json::json!({
        "mode": "oss",
        "head_block": head,
        "from_block": from_block,
        "to_block": to_block,
        "blocks": { "enabled": opt.blocks },
        "trades": { "enabled": opt.trades },
        "cursor_files": {
            "blocks": pma::oss_polymarket_path(cfg, CURSOR_BLOCKS),
            "trades": pma::oss_polymarket_path(cfg, CURSOR_TRADES),
        }
    });

    if opt.blocks {
        let n = sync_blocks(
            store.clone(),
            &rpc,
            cfg,
            from_block,
            to_block,
            opt.chunk_blocks,
            opt.blocks_sample_stride,
        )
        .await?;
        summary["blocks"]["written_files"] = serde_json::json!(n);
    }
    if opt.trades {
        let n = sync_trades(
            store.clone(),
            &rpc,
            cfg,
            from_block,
            to_block,
            opt.chunk_blocks,
        )
        .await?;
        summary["trades"]["written_files"] = serde_json::json!(n);
    }

    Ok(summary)
}

async fn sync_blocks(
    store: Arc<dyn ObjectStore>,
    rpc: &RotatingRpc,
    cfg: &Config,
    from_block: u64,
    to_block: u64,
    chunk_blocks: u64,
    sample_stride: u64,
) -> anyhow::Result<usize> {
    let total_chunks = ((to_block.saturating_sub(from_block) + chunk_blocks) / chunk_blocks).max(1);
    let pb = ProgressBar::new(total_chunks);
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} blocks {msg}",
        )
        .unwrap()
        .progress_chars("=>-"),
    );

    let mut written = 0usize;
    let mut cur = from_block;
    while cur <= to_block {
        let end = (cur + chunk_blocks - 1).min(to_block);
        pb.set_message(format!("{cur}..={end}"));

        let mut block_numbers: Vec<u64> = Vec::new();
        let stride = sample_stride.max(1);
        let mut b = cur;
        while b <= end {
            block_numbers.push(b);
            match b.checked_add(stride) {
                Some(n) => b = n,
                None => break,
            }
        }
        if *block_numbers.last().unwrap_or(&cur) != end {
            block_numbers.push(end);
        }

        let pb_inner = ProgressBar::new(block_numbers.len() as u64);
        pb_inner.set_style(
            ProgressStyle::with_template(
                "  [{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} sample {msg}",
            )
            .unwrap()
            .progress_chars("=>-"),
        );
        let mut out_bn: Vec<i64> = Vec::with_capacity(block_numbers.len());
        let mut out_ts: Vec<String> = Vec::with_capacity(block_numbers.len());
        for bn in block_numbers {
            let ts = rpc_get_block_ts(rpc, bn).await?;
            out_bn.push(bn as i64);
            out_ts.push(ts_to_iso(ts));
            pb_inner.inc(1);
        }
        pb_inner.finish_and_clear();

        let batch = RecordBatch::try_new(
            blocks_schema(),
            vec![
                Arc::new(Int64Array::from(out_bn)),
                Arc::new(StringArray::from(out_ts)),
            ],
        )?;
        let bytes = write_parquet_bytes(blocks_schema(), vec![batch])?;
        let obj_key = pma::oss_polymarket_path(
            cfg,
            &format!("polymarket/blocks/blocks_{cur}_{end}.parquet"),
        );
        store
            .put(&ObjPath::from(obj_key.as_str()), PutPayload::from(bytes))
            .await?;
        write_cursor_u64(store.clone(), cfg, CURSOR_BLOCKS, end).await?;

        written += 1;
        pb.inc(1);
        cur = end.saturating_add(1);
        if cur == 0 {
            break;
        }
    }

    pb.finish_with_message("blocks done");
    Ok(written)
}

async fn sync_trades(
    store: Arc<dyn ObjectStore>,
    rpc: &RotatingRpc,
    cfg: &Config,
    from_block: u64,
    to_block: u64,
    chunk_blocks: u64,
) -> anyhow::Result<usize> {
    if cfg.polymarket_exchange_addresses.is_empty() {
        anyhow::bail!(
            "trades sync 需要 PIPELINE_POLYMARKET_EXCHANGE_ADDRESS（或 *_ADDRESSES，逗号分隔）；PMA 默认两份：CTF_EXCHANGE + NEGRISK_CTF_EXCHANGE"
        );
    }
    let topic0 = cfg.polymarket_order_filled_topic0.clone().ok_or_else(|| {
        anyhow::anyhow!("trades sync 需要 PIPELINE_POLYMARKET_ORDER_FILLED_TOPIC0")
    })?;

    let total_chunks = ((to_block.saturating_sub(from_block) + chunk_blocks) / chunk_blocks).max(1);
    let pb = ProgressBar::new(total_chunks);
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} trades {msg}",
        )
        .unwrap()
        .progress_chars("=>-"),
    );

    let mut written = 0usize;
    let mut cur = from_block;
    while cur <= to_block {
        let end = (cur + chunk_blocks - 1).min(to_block);
        pb.set_message(format!("{cur}..={end}"));

        let fetched_at = chrono::Utc::now().to_rfc3339();

        let mut out_block: Vec<i64> = Vec::new();
        let mut out_log_index: Vec<u64> = Vec::new();
        let mut out_tx: Vec<String> = Vec::new();
        let mut out_order_hash: Vec<String> = Vec::new();
        let mut out_maker: Vec<String> = Vec::new();
        let mut out_taker: Vec<String> = Vec::new();
        let mut out_maker_asset: Vec<String> = Vec::new();
        let mut out_taker_asset: Vec<String> = Vec::new();
        let mut out_maker_amt: Vec<String> = Vec::new();
        let mut out_taker_amt: Vec<String> = Vec::new();
        let mut out_fee: Vec<String> = Vec::new();
        let mut out_contract: Vec<String> = Vec::new();
        let mut out_fetched_at: Vec<String> = Vec::new();

        for contract_address in &cfg.polymarket_exchange_addresses {
            let params = serde_json::json!([{
                "fromBlock": format!("0x{:x}", cur),
                "toBlock": format!("0x{:x}", end),
                "address": contract_address,
                "topics": [topic0],
            }]);
            let logs = rpc.call("eth_getLogs", params).await?;
            let arr = logs
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("eth_getLogs: expected array, got {logs}"))?;

            for log in arr {
                let bn_hex = log
                    .get("blockNumber")
                    .and_then(|x| x.as_str())
                    .ok_or_else(|| anyhow::anyhow!("log missing blockNumber: {log}"))?;
                let bn = u64::from_str_radix(bn_hex.trim_start_matches("0x"), 16)?;
                let li_hex = log
                    .get("logIndex")
                    .and_then(|x| x.as_str())
                    .ok_or_else(|| anyhow::anyhow!("log missing logIndex: {log}"))?;
                let li = u64::from_str_radix(li_hex.trim_start_matches("0x"), 16)?;
                let tx = log
                    .get("transactionHash")
                    .and_then(|x| x.as_str())
                    .unwrap_or_default()
                    .to_string();
                let topics = log
                    .get("topics")
                    .and_then(|x| x.as_array())
                    .cloned()
                    .unwrap_or_default();

                // PMA ABI:
                // topics[0]=topic0, topics[1]=orderHash(bytes32), topics[2]=maker(address), topics[3]=taker(address)
                let order_hash = topics
                    .get(1)
                    .and_then(|x| x.as_str())
                    .unwrap_or_default()
                    .to_string();
                let maker = topics
                    .get(2)
                    .and_then(|x| x.as_str())
                    .map(topic_to_address)
                    .unwrap_or_default();
                let taker = topics
                    .get(3)
                    .and_then(|x| x.as_str())
                    .map(topic_to_address)
                    .unwrap_or_default();

                let data_hex = log.get("data").and_then(|x| x.as_str()).unwrap_or("0x");
                let (maker_asset, taker_asset, maker_amt, taker_amt, fee) =
                    decode_order_filled_data(data_hex)?;

                out_block.push(bn as i64);
                out_log_index.push(li);
                out_tx.push(tx);
                out_order_hash.push(order_hash);
                out_maker.push(maker);
                out_taker.push(taker);
                out_maker_asset.push(maker_asset);
                out_taker_asset.push(taker_asset);
                out_maker_amt.push(maker_amt);
                out_taker_amt.push(taker_amt);
                out_fee.push(fee);
                out_contract.push(contract_address.to_string());
                out_fetched_at.push(fetched_at.clone());
            }
        }

        let batch = RecordBatch::try_new(
            trades_schema(),
            vec![
                Arc::new(Int64Array::from(out_block)),
                Arc::new(UInt64Array::from(out_log_index)),
                Arc::new(StringArray::from(out_tx)),
                Arc::new(StringArray::from(out_order_hash)),
                Arc::new(StringArray::from(out_maker)),
                Arc::new(StringArray::from(out_taker)),
                Arc::new(StringArray::from(out_maker_asset)),
                Arc::new(StringArray::from(out_taker_asset)),
                Arc::new(StringArray::from(out_maker_amt)),
                Arc::new(StringArray::from(out_taker_amt)),
                Arc::new(StringArray::from(out_fee)),
                Arc::new(StringArray::from(out_contract)),
                Arc::new(StringArray::from(out_fetched_at)),
            ],
        )?;
        let bytes = write_parquet_bytes(trades_schema(), vec![batch])?;
        let obj_key = pma::oss_polymarket_path(
            cfg,
            &format!("polymarket/trades/trades_{cur}_{end}.parquet"),
        );
        store
            .put(&ObjPath::from(obj_key.as_str()), PutPayload::from(bytes))
            .await?;

        // PMA trades cursor: resume pointer (block) stored in `.backfill_block_cursor`.
        write_cursor_u64(store.clone(), cfg, CURSOR_TRADES, end).await?;

        written += 1;
        pb.inc(1);
        cur = end.saturating_add(1);
        if cur == 0 {
            break;
        }
    }
    pb.finish_with_message("trades done");
    Ok(written)
}

fn topic_to_address(t: &str) -> String {
    let s = t.trim();
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.len() < 40 {
        return String::new();
    }
    let addr = &s[s.len() - 40..];
    format!("0x{}", addr.to_ascii_lowercase())
}

fn decode_order_filled_data(
    data_hex: &str,
) -> anyhow::Result<(String, String, String, String, String)> {
    let s = data_hex.trim();
    let s = s.strip_prefix("0x").unwrap_or(s);
    let bytes = if s.is_empty() {
        vec![]
    } else {
        hex::decode(s)?
    };
    // makerAssetId, takerAssetId, makerAmountFilled, takerAmountFilled, fee
    let types = vec![
        ParamType::Uint(256),
        ParamType::Uint(256),
        ParamType::Uint(256),
        ParamType::Uint(256),
        ParamType::Uint(256),
    ];
    let decoded = ethabi::decode(&types, &bytes)?;
    let u = |t: &Token| -> String {
        match t {
            Token::Uint(x) => x.to_string(),
            _ => "0".to_string(),
        }
    };
    Ok((
        u(&decoded[0]),
        u(&decoded[1]),
        u(&decoded[2]),
        u(&decoded[3]),
        u(&decoded[4]),
    ))
}
