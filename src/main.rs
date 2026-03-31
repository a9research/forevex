//! CLI: `polymarket-pipeline` — ingest & process (see docs/polymarket-data-foundation-pma-core.md).

use clap::{Parser, Subcommand};
use polymarket_pipeline::{
    aggregate, bootstrap, config::{Config, TradesProcessor}, db, enrich_gamma, http_server, ingest_activities,
    ingest_markets, pma, process_trades, process_trades_pma, refresh_wallets, report, snapshot_wallets,
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
    }

    Ok(())
}

async fn run_pipeline_steps(pool: &PgPool, cfg: &polymarket_pipeline::config::Config) -> anyhow::Result<()> {
    ingest_markets::run(pool, cfg).await?;
    enrich_gamma::run(pool, cfg).await?;
    match cfg.trades_processor {
        TradesProcessor::PgStg => {
            pma::run(pool, cfg).await?;
            process_trades::run(pool, cfg).await?;
        }
        TradesProcessor::PmaOssDirect => {
            process_trades_pma::run(pool, cfg).await?;
        }
    }
    refresh_wallets::run(pool).await?;
    ingest_activities::run(pool, cfg).await?;
    snapshot_wallets::run(pool, cfg).await?;
    aggregate::run(pool).await?;
    Ok(())
}
