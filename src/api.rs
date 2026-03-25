//! REST-style HTTP API under **`/api/v1`** (JSON, pluralized collections, noun routes).
//!
//! | Method | Path | Body / query | Description |
//! |--------|------|----------------|-------------|
//! | `GET` | `/health` | — | Liveness |
//! | `GET` | `/api/v1/meta` | — | Service metadata |
//! | `POST` | `/api/v1/users` | `{ "input": "@slug" \| "0x…" }` | Resolve + sync upstream → DB, **201 Created** |
//! | `GET` | `/api/v1/users/:proxy` | — | Wallet snapshot JSON（含 **`positionsSyncedAt`**：曾完成持仓 sync 则有值，否则 `null` → 客户端应先 blocking sync） |
//! | `GET` | `/api/v1/users/:proxy/positions` | `?state=open\|closed` | List cached positions |
//! | `POST` | `/api/v1/users/:proxy/positions/sync` | — | Refresh positions from Data API |
//! | `GET` | `/api/v1/users/:proxy/activity` | `?market=<condition_id>` | Cached activity |
//! | `POST` | `/api/v1/users/:proxy/activity` | `{ "market": "<condition_id>" }` | Sync activity for market |
//! | `GET` | `/api/v1/users/:proxy/analytics/positions` | — | 持仓聚合：胜率、**按 Gamma 标签分类**（`gamma_market_tags_cache`：`/markets/slug?include_tag` + [`/markets/{id}/tags`](https://docs.polymarket.com/api-reference/markets/get-market-tags-by-id)）、均价桶、Yes/No 比 |

use crate::config::Config;
use crate::gamma_tags::{collect_slugs_from_positions, ensure_gamma_buckets_for_slugs};
use crate::position_analytics::compute_position_analytics;
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

    // 使用完整路径注册，避免 `nest` 与反代 strip 前缀时难以对照；路径与表头文档一致。
    Router::new()
        .route("/health", get(health))
        .route("/api/v1/meta", get(meta))
        .route("/api/v1/users", post(create_user))
        .route("/api/v1/users/:proxy", get(get_user))
        .route(
            "/api/v1/users/:proxy/positions",
            get(list_positions),
        )
        .route(
            "/api/v1/users/:proxy/positions/sync",
            post(sync_positions_path),
        )
        .route(
            "/api/v1/users/:proxy/activity",
            get(get_activity).post(sync_activity_body),
        )
        .route(
            "/api/v1/users/:proxy/analytics/positions",
            get(get_position_analytics),
        )
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
        "apiVersion": "v1",
        "note": "Auth not enabled in v0.1; add later behind reverse proxy or API key."
    }))
}

#[derive(Deserialize)]
struct CreateUserBody {
    /// `@slug` or `0x…` proxy address.
    input: String,
}

/// `POST /api/v1/users` — upsert user snapshot from upstream (canonical “create” for this service).
async fn create_user(
    State(s): State<Arc<AppState>>,
    Json(body): Json<CreateUserBody>,
) -> Result<impl IntoResponse, ApiError> {
    let v = sync::sync_user(&s.store, &s.upstream, &body.input)
        .await
        .map_err(ApiError::from_anyhow)?;
    Ok((StatusCode::CREATED, Json(v)))
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
        .ok_or_else(|| {
            ApiError::new(
                StatusCode::NOT_FOUND,
                "wallet not found; POST /api/v1/users with {\"input\":\"@slug or 0x…\"} first",
            )
        })?;
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
        "positionsSyncedAt": row.positions_synced_at,
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
                "query parameter state must be \"open\" or \"closed\"",
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

#[derive(Deserialize)]
struct ActivitySyncBody {
    market: String,
}

async fn get_activity(
    State(s): State<Arc<AppState>>,
    Path(proxy): Path<String>,
    Query(q): Query<ActivityQuery>,
) -> Result<Json<Value>, ApiError> {
    let p = proxy.trim().to_lowercase();
    let m = q.market.trim();
    if m.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "missing required query parameter: market",
        ));
    }
    let row = s
        .store
        .fetch_activity(&p, m)
        .await
        .map_err(ApiError::from_anyhow)?;
    let Some((events, max_ts, synced)) = row else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "no cached activity for this market; POST /api/v1/users/:proxy/activity with JSON body {\"market\":\"<condition_id>\"}",
        ));
    };
    Ok(Json(json!({
        "proxy": p,
        "market": m,
        "events": events,
        "maxEventTs": max_ts,
        "syncedAt": synced,
    })))
}

async fn get_position_analytics(
    State(s): State<Arc<AppState>>,
    Path(proxy): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let p = proxy.trim().to_lowercase();
    let exists = s
        .store
        .fetch_wallet_snapshot(&p)
        .await
        .map_err(ApiError::from_anyhow)?
        .is_some();
    if !exists {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "wallet not found; POST /api/v1/users with {\"input\":\"@slug or 0x…\"} first",
        ));
    }
    let open_rows = s
        .store
        .list_positions(&p, Some("open"))
        .await
        .map_err(ApiError::from_anyhow)?;
    let closed_rows = s
        .store
        .list_positions(&p, Some("closed"))
        .await
        .map_err(ApiError::from_anyhow)?;
    let open: Vec<Value> = open_rows.into_iter().map(|(_, raw, _)| raw).collect();
    let closed: Vec<Value> = closed_rows.into_iter().map(|(_, raw, _)| raw).collect();
    let slugs = collect_slugs_from_positions(&open, &closed);
    let slug_map = ensure_gamma_buckets_for_slugs(&s.store, &s.upstream, &s.config, &slugs)
        .await
        .map_err(ApiError::from_anyhow)?;
    let a = compute_position_analytics(&p, &open, &closed, &slug_map);
    Ok(Json(a))
}

async fn sync_activity_body(
    State(s): State<Arc<AppState>>,
    Path(proxy): Path<String>,
    Json(body): Json<ActivitySyncBody>,
) -> Result<Json<Value>, ApiError> {
    let m = body.market.trim();
    if m.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "JSON body must include non-empty \"market\" (condition id)",
        ));
    }
    let v = sync::sync_activity(&s.store, &s.upstream, &proxy, m)
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
