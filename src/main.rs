//! CLI: `polymarket-pipeline` — ingest & process (see docs/polymarket-data-foundation-pma-core.md).

use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use polymarket_pipeline::{
    aggregate, bootstrap,
    config::{Config, TradesProcessor},
    db, enrich_gamma, http_server, ingest_activities, ingest_markets, pma, pma_indexer,
    process_trades, process_trades_pma, refresh_wallets, report, snapshot_wallets,
};
use serde_json::json;
use sqlx::PgPool;

#[derive(Parser)]
#[command(
    name = "polymarket-pipeline",
    about = "Polymarket data pipeline (Gamma + PMA Parquet + process + aggregates)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Apply SQL migrations (`migrations/`)
    Migrate,
    /// Fetch Gamma markets into `dim_markets`
    IngestMarkets,
    /// Fill `category_raw` / `topic_primary` via Gamma `GET /markets/{id}`
    EnrichGamma,
    /// PMA Parquet：对象存储为 raw 源（本地仅暂存，上传后删除）→ `stg_order_filled`
    IngestPma,
    /// Build `fact_trades` from staging + `dim_markets`
    ProcessTrades,
    /// Rebuild `dim_wallets` from `fact_trades`
    RefreshWallets,
    /// Data API activity → `fact_account_activities` (PIPELINE_ACTIVITY_PROXIES)
    IngestActivities,
    /// user-stats / user-pnl / public-profile → `wallet_api_snapshot`
    SnapshotWallets,
    /// Rebuild `agg_wallet_topic` + `agg_global_daily`
    Aggregate,
    /// Migrate + full pipeline (Postgres only; no Parquet)
    RunAll,
    /// 增量同步：markets → enrich → PMA → process-trades → … → aggregate（**不含** migrate）
    Sync,
    /// Read-only HTTP (`PIPELINE_HTTP_BIND`): `/health`, `/stats`, `/pipeline-status`
    Serve,
    /// 打印 OSS PMA 进度 + `etl_checkpoint`（与 `/pipeline-status` 一致）
    Status,
    /// 清空 `stg_order_filled` 以释放磁盘（仅在确认已改为 OSS 直读或已完成下游后使用）
    CleanupStg,
    /// 报告 OSS 上 `._*.parquet`（macOS AppleDouble）是否与真实 Parquet 成对；`--delete` 删除这些旁路文件
    OssAppleDouble {
        /// 删除所有匹配的 `._*.parquet`（建议先不加本参数看 JSON 报告）
        #[arg(long)]
        delete: bool,
    },
    /// 下载 `data.tar.zst` 并解压；若已配置 OSS bucket 则上传并删除本地 `polymarket/`
    BootstrapData {
        /// 覆盖默认 `PIPELINE_BOOTSTRAP_DOWNLOAD_URL`
        #[arg(long)]
        url: Option<String>,
        /// 即使已有 `.pma_download_complete` 也重新下载解压
        #[arg(long)]
        force: bool,
        /// 已配置 bucket 时仍 **不** 上传、不删本地（调试）
        #[arg(long)]
        no_upload: bool,
    },

    /// PMA 对齐 indexer：从 Polygon RPC 增量写 OSS Parquet（blocks/trades）并把游标文件写到 OSS
    IndexerOss {
        /// 只同步 blocks（写 `polymarket/blocks/*.parquet` + `polymarket/.backfill_block_cursor`）
        #[arg(long)]
        blocks: bool,
        /// 只同步 trades（写 `polymarket/trades/*.parquet` + `polymarket/.backfill_offset`）
        #[arg(long)]
        trades: bool,
        /// 覆盖 cursor：从指定 block 开始（含）
        #[arg(long)]
        from_block: Option<u64>,
        /// 覆盖 cursor：同步到指定 block（含）；默认到链上 head
        #[arg(long)]
        to_block: Option<u64>,
        /// 每次 `eth_getLogs` / blocks chunk 的 block 范围（默认 10000）
        #[arg(long, default_value_t = 10_000)]
        chunk_blocks: u64,
        /// blocks 采样步长（默认 1000；写入采样点用于后续插值）
        #[arg(long, default_value_t = 1_000)]
        blocks_sample_stride: u64,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let cfg = Config::from_env()?;

    match &cli.command {
        Command::OssAppleDouble { delete } => {
            if cfg.s3_bucket.is_none() {
                anyhow::bail!("oss-apple-double 需要 PIPELINE_OSS_BUCKET（或 PIPELINE_S3_BUCKET）");
            }
            if *delete {
                let (n, paths) = pma::delete_apple_double_parquet_oss(&cfg, false).await?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "deleted": n,
                        "paths": paths,
                    }))?
                );
            } else {
                let v = pma::report_apple_double_parquet_oss(&cfg).await?;
                println!("{}", serde_json::to_string_pretty(&v)?);
            }
            return Ok(());
        }
        Command::IndexerOss {
            blocks,
            trades,
            from_block,
            to_block,
            chunk_blocks,
            blocks_sample_stride,
        } => {
            let v = pma_indexer::run_indexer_oss(
                &cfg,
                pma_indexer::IndexerOptions {
                    blocks: if !blocks && !trades { true } else { *blocks },
                    trades: if !blocks && !trades { true } else { *trades },
                    from_block: *from_block,
                    to_block: *to_block,
                    chunk_blocks: *chunk_blocks,
                    blocks_sample_stride: *blocks_sample_stride,
                },
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
            return Ok(());
        }
        _ => {}
    }

    let pool = db::connect(
        &cfg.database_url,
        cfg.db_max_connections,
        cfg.db_acquire_timeout,
    )
    .await?;

    match cli.command {
        Command::Migrate => {
            sqlx::migrate!("./migrations").run(&pool).await?;
            tracing::info!("migrations applied");
        }
        Command::IngestMarkets => {
            ingest_markets::run(&pool, &cfg).await?;
        }
        Command::EnrichGamma => {
            enrich_gamma::run(&pool, &cfg).await?;
        }
        Command::IngestPma => {
            pma::run(&pool, &cfg).await?;
        }
        Command::ProcessTrades => {
            process_trades::run(&pool, &cfg).await?;
        }
        Command::RefreshWallets => {
            refresh_wallets::run(&pool).await?;
        }
        Command::IngestActivities => {
            ingest_activities::run(&pool, &cfg).await?;
        }
        Command::SnapshotWallets => {
            snapshot_wallets::run(&pool, &cfg).await?;
        }
        Command::Aggregate => {
            aggregate::run(&pool).await?;
        }
        Command::RunAll => {
            sqlx::migrate!("./migrations").run(&pool).await?;
            run_pipeline_steps(&pool, &cfg).await?;
            tracing::info!("run-all completed");
        }
        Command::Sync => {
            run_pipeline_steps(&pool, &cfg).await?;
            tracing::info!("sync completed");
        }
        Command::Serve => {
            http_server::run_http(&cfg, pool).await?;
        }
        Command::Status => {
            let v = report::pipeline_report(&pool, &cfg).await?;
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        Command::CleanupStg => {
            let before: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM stg_order_filled")
                .fetch_one(&pool)
                .await
                .unwrap_or(0);
            sqlx::query("TRUNCATE stg_order_filled")
                .execute(&pool)
                .await?;
            tracing::info!(before, "cleanup-stg: stg_order_filled truncated");
        }
        Command::BootstrapData {
            url,
            force,
            no_upload,
        } => {
            bootstrap::run(
                &cfg,
                bootstrap::BootstrapOptions {
                    url,
                    force,
                    no_upload,
                },
            )
            .await?;
        }
        Command::OssAppleDouble { .. } => {
            unreachable!("oss-apple-double handled before DB connect");
        }
        Command::IndexerOss { .. } => {
            unreachable!("indexer-oss handled before DB connect");
        }
    }

    Ok(())
}

async fn run_pipeline_steps(
    pool: &PgPool,
    cfg: &polymarket_pipeline::config::Config,
) -> anyhow::Result<()> {
    let mut total_steps = 1 /* markets */ + 1 /* enrich */ + 1 /* trades */ + 1 /* wallets */ + 1 /* activities */ + 1 /* snapshots */ + 1 /* aggregate */;
    if matches!(cfg.trades_processor, TradesProcessor::PgStg) {
        total_steps += 1; // ingest-pma + process-trades are separate concerns
    }

    let pb = ProgressBar::new(total_steps);
    pb.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );

    pb.set_message("ingest-markets");
    ingest_markets::run(pool, cfg).await?;
    pb.inc(1);

    if cfg.skip_enrich_gamma {
        pb.set_message("enrich-gamma (skipped)");
        tracing::warn!("sync/run-all: PIPELINE_SKIP_ENRICH_GAMMA=1 — skipping enrich-gamma");
    } else {
        pb.set_message("enrich-gamma");
        enrich_gamma::run(pool, cfg).await?;
    }
    pb.inc(1);

    match cfg.trades_processor {
        TradesProcessor::PgStg => {
            pb.set_message("ingest-pma (PG stg)");
            pma::run(pool, cfg).await?;
            pb.inc(1);
            pb.set_message("process-trades (PG stg -> fact)");
            process_trades::run(pool, cfg).await?;
            pb.inc(1);
        }
        TradesProcessor::PmaOssDirect => {
            pb.set_message("process-trades-pma (OSS -> fact)");
            process_trades_pma::run(pool, cfg).await?;
            pb.inc(1);
        }
    }

    pb.set_message("refresh-wallets");
    refresh_wallets::run(pool).await?;
    pb.inc(1);

    pb.set_message("ingest-activities");
    ingest_activities::run(pool, cfg).await?;
    pb.inc(1);

    pb.set_message("snapshot-wallets");
    snapshot_wallets::run(pool, cfg).await?;
    pb.inc(1);

    pb.set_message("aggregate");
    aggregate::run(pool).await?;
    pb.inc(1);

    pb.finish_with_message("pipeline completed");
    Ok(())
}
