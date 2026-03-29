//! CLI: `polymarket-pipeline` — ingest & process (see docs/polymarket-data-platform-unified.md).

use clap::{Parser, Subcommand};
use polymarket_pipeline::{
    aggregate, bootstrap, config::Config, db, enrich_gamma, http_server, ingest_activities, ingest_markets, pma,
    process_trades, refresh_wallets, snapshot_wallets,
};
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
    /// Read-only HTTP (`PIPELINE_HTTP_BIND`): `/health`, `/stats`
    Serve,
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
    }

    Ok(())
}

async fn run_pipeline_steps(pool: &PgPool, cfg: &polymarket_pipeline::config::Config) -> anyhow::Result<()> {
    ingest_markets::run(pool, cfg).await?;
    enrich_gamma::run(pool, cfg).await?;
    pma::run(pool, cfg).await?;
    process_trades::run(pool, cfg).await?;
    refresh_wallets::run(pool).await?;
    ingest_activities::run(pool, cfg).await?;
    snapshot_wallets::run(pool, cfg).await?;
    aggregate::run(pool).await?;
    Ok(())
}
