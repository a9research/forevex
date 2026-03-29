//! CLI: `polymarket-pipeline` — ingest & process (see docs/polymarket-data-platform-unified.md).

use clap::{Parser, Subcommand};
use polymarket_pipeline::{
    aggregate, config::Config, db, enrich_gamma, goldsky, http_server, import_order_filled_snapshot,
    ingest_activities, ingest_markets, process_trades, refresh_wallets, snapshot_wallets,
};
use sqlx::PgPool;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "polymarket-pipeline",
    about = "Polymarket data pipeline (Gamma + Goldsky + process + aggregates)"
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
    /// Fetch Goldsky `orderFilledEvents` into `stg_order_filled`
    IngestGoldsky,
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
    /// 增量同步：拉取最新 markets / Goldsky + process-trades → … → aggregate（**不含** migrate；日常一条命令）
    Sync,
    /// Read-only HTTP (`PIPELINE_HTTP_BIND`): `/health`, `/stats`
    Serve,
    /// Import poly_data `orderFilled_complete.csv` / `.xz` → `stg_order_filled` (updates Goldsky checkpoint)
    ImportOrderFilledSnapshot {
        /// Path to `.csv`, `.csv.xz`, or `-` for stdin (plain CSV, not xz)
        path: PathBuf,
        #[arg(long, default_value_t = 2000)]
        batch_size: usize,
        /// Do not write `etl_checkpoint` for `goldsky_order_filled` after import
        #[arg(long)]
        no_update_checkpoint: bool,
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
        Command::IngestGoldsky => {
            goldsky::run(&pool, &cfg).await?;
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
        Command::ImportOrderFilledSnapshot {
            path,
            batch_size,
            no_update_checkpoint,
        } => {
            import_order_filled_snapshot::run(
                &pool,
                path.as_path(),
                batch_size,
                !no_update_checkpoint,
            )
            .await?;
        }
    }

    Ok(())
}

/// 与 `run-all` 中 migrate **之后** 的步骤一致（适合 `sync` 与首次 `run-all` 复用）。
async fn run_pipeline_steps(pool: &PgPool, cfg: &polymarket_pipeline::config::Config) -> anyhow::Result<()> {
    ingest_markets::run(pool, cfg).await?;
    enrich_gamma::run(pool, cfg).await?;
    goldsky::run(pool, cfg).await?;
    process_trades::run(pool, cfg).await?;
    refresh_wallets::run(pool).await?;
    ingest_activities::run(pool, cfg).await?;
    snapshot_wallets::run(pool, cfg).await?;
    aggregate::run(pool).await?;
    Ok(())
}
