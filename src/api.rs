use crate::config::Config;
use crate::store::Store;
use crate::sync;
use crate::upstream::Upstream;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub upstream: Upstream,
    pub config: Config,
}

pub fn router(state: Arc<AppState>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .route("/health", get(health))
        .route("/v1/meta", get(meta))
        .route("/v1/users/sync", post(sync_user_body))
        .route("/v1/users/:proxy", get(get_user))
        .route("/v1/wallets/:proxy/positions", get(list_positions))
        .route("/v1/wallets/:proxy/positions/sync", post(sync_positions_path))
        .route("/v1/wallets/:proxy/activity", get(get_activity).post(sync_activity_query))
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    Json(json!({ "ok": true, "service": "forevex" }))
}

async fn meta(State(s): State<Arc<AppState>>) -> impl IntoResponse {
    Json(json!({
        "publicBaseUrl": s.config.public_base_url,
        "note": "Auth not enabled in v0.1; add later behind reverse proxy or API key."
    }))
}

#[derive(Deserialize)]
struct SyncUserBody {
    input: String,
}

async fn sync_user_body(
    State(s): State<Arc<AppState>>,
    Json(body): Json<SyncUserBody>,
) -> Result<Json<Value>, ApiError> {
    let v = sync::sync_user(&s.store, &s.upstream, &body.input)
        .await
        .map_err(ApiError::from_anyhow)?;
    Ok(Json(v))
}

async fn get_user(
    State(s): State<Arc<AppState>>,
    Path(proxy): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let p = proxy.trim().to_lowercase();
    let row = s
        .store
        .fetch_wallet_snapshot(&p)
        .await
        .map_err(ApiError::from_anyhow)?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "wallet not found; POST /v1/users/sync first"))?;
    Ok(Json(json!({
        "proxy": row.proxy_address,
        "resolvedUsername": row.resolved_username,
        "gammaProfile": row.gamma_profile.map(|j| j.0),
        "value": row.data_value.map(|j| j.0),
        "traded": row.data_traded.map(|j| j.0),
        "userStats": row.user_stats.map(|j| j.0),
        "userPnl": row.user_pnl.map(|j| j.0),
        "profileFetchedAt": row.profile_fetched_at,
        "metricsFetchedAt": row.metrics_fetched_at,
    })))
}

#[derive(Deserialize)]
struct PositionsQuery {
    state: Option<String>,
}

async fn list_positions(
    State(s): State<Arc<AppState>>,
    Path(proxy): Path<String>,
    Query(q): Query<PositionsQuery>,
) -> Result<Json<Value>, ApiError> {
    let p = proxy.trim().to_lowercase();
    let st = q.state.as_deref();
    if let Some(x) = st {
        if x != "open" && x != "closed" {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "state must be open or closed",
            ));
        }
    }
    let rows = s
        .store
        .list_positions(&p, st)
        .await
        .map_err(ApiError::from_anyhow)?;
    let list: Vec<Value> = rows
        .into_iter()
        .map(|(k, raw, at)| json!({ "positionKey": k, "raw": raw, "syncedAt": at }))
        .collect();
    Ok(Json(json!({ "proxy": p, "positions": list })))
}

async fn sync_positions_path(
    State(s): State<Arc<AppState>>,
    Path(proxy): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let v = sync::sync_positions(&s.store, &s.upstream, &proxy)
        .await
        .map_err(ApiError::from_anyhow)?;
    Ok(Json(v))
}

#[derive(Deserialize)]
struct ActivityQuery {
    market: String,
}

async fn get_activity(
    State(s): State<Arc<AppState>>,
    Path(proxy): Path<String>,
    Query(q): Query<ActivityQuery>,
) -> Result<Json<Value>, ApiError> {
    let p = proxy.trim().to_lowercase();
    let row = s
        .store
        .fetch_activity(&p, &q.market)
        .await
        .map_err(ApiError::from_anyhow)?;
    let Some((events, max_ts, synced)) = row else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "no cached activity; POST /v1/wallets/:proxy/activity?market=…",
        ));
    };
    Ok(Json(json!({
        "proxy": p,
        "market": q.market,
        "events": events,
        "maxEventTs": max_ts,
        "syncedAt": synced,
    })))
}

async fn sync_activity_query(
    State(s): State<Arc<AppState>>,
    Path(proxy): Path<String>,
    Query(q): Query<ActivityQuery>,
) -> Result<Json<Value>, ApiError> {
    let v = sync::sync_activity(&s.store, &s.upstream, &proxy, &q.market)
        .await
        .map_err(ApiError::from_anyhow)?;
    Ok(Json(v))
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
    fn from_anyhow(e: anyhow::Error) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            message: e.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(json!({ "error": self.message }));
        (self.status, body).into_response()
    }
}
