//! Minimal read-only HTTP: health + row counts (P4 非 Parquet 部分).

use crate::config::Config;
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    serve as axum_serve,
    Json, Router,
};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
}

pub async fn run_http(cfg: &Config, pool: PgPool) -> anyhow::Result<()> {
    let state = AppState { pool };
    let app = Router::new()
        .route("/health", get(health))
        .route("/stats", get(stats))
        .with_state(Arc::new(state));

    let listener = tokio::net::TcpListener::bind(&cfg.http_bind).await?;
    tracing::info!(addr = %cfg.http_bind, "HTTP serve");
    axum_serve(listener, app).await?;
    Ok(())
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn stats(State(st): State<Arc<AppState>>) -> Result<Json<Value>, StatusCode> {
    let pool = &st.pool;
    let dim_markets: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM dim_markets")
        .fetch_one(pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let stg: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM stg_order_filled")
        .fetch_one(pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let trades: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM fact_trades")
        .fetch_one(pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let wallets: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM dim_wallets")
        .fetch_one(pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let activities: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM fact_account_activities")
        .fetch_one(pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let snapshots: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM wallet_api_snapshot")
        .fetch_one(pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(json!({
        "dim_markets": dim_markets,
        "stg_order_filled": stg,
        "fact_trades": trades,
        "dim_wallets": wallets,
        "fact_account_activities": activities,
        "wallet_api_snapshot": snapshots,
    })))
}
