use clap::{Parser, Subcommand};
use forevex::api::{router, AppState};
use forevex::config::Config;
use forevex::store::Store;
use forevex::sync;
use forevex::upstream::Upstream;
use std::sync::Arc;

#[derive(Parser)]
#[command(name = "forevex", about = "Polymarket user/positions/activity sync (CLI + API)")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run HTTP API (migrations applied on startup).
    Serve,
    /// Pull upstream and write to Postgres.
    Sync {
        #[command(subcommand)]
        cmd: SyncCmd,
    },
}

#[derive(Subcommand)]
enum SyncCmd {
    /// Resolve @slug or 0x…, then fetch profile + value + traded + user-stats + user-pnl.
    User {
        /// Wallet `0x…` or `@username` / `username`.
        input: String,
    },
    /// Fetch open + closed positions (Data API).
    Positions {
        proxy: String,
    },
    /// Fetch `/activity` for one market (condition id).
    Activity {
        proxy: String,
        #[arg(long)]
        market: String,
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
    let store = Store::connect(&cfg.database_url).await?;
    store.run_migrations().await?;
    let upstream = Upstream::new(cfg.clone())?;

    match cli.command {
        Command::Serve => {
            let state = Arc::new(AppState {
                store: store.clone(),
                upstream,
                config: cfg.clone(),
            });
            let app = router(state);
            let listener = tokio::net::TcpListener::bind(&cfg.bind).await?;
            tracing::info!("listening on {}", cfg.bind);
            axum::serve(listener, app).await?;
        }
        Command::Sync { cmd } => match cmd {
            SyncCmd::User { input } => {
                let v = sync::sync_user(&store, &upstream, &input).await?;
                println!("{}", serde_json::to_string_pretty(&v)?);
            }
            SyncCmd::Positions { proxy } => {
                let v = sync::sync_positions(&store, &upstream, &proxy).await?;
                println!("{}", serde_json::to_string_pretty(&v)?);
            }
            SyncCmd::Activity { proxy, market } => {
                let v = sync::sync_activity(&store, &upstream, &proxy, &market).await?;
                println!("{}", serde_json::to_string_pretty(&v)?);
            }
        },
    }

    Ok(())
}
