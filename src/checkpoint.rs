//! Shared `etl_checkpoint` helpers.

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::PgPool;

pub async fn load(pool: &PgPool, pipeline: &str) -> anyhow::Result<Option<Value>> {
    let row: Option<(Value,)> =
        sqlx::query_as("SELECT cursor_json FROM etl_checkpoint WHERE pipeline = $1")
            .bind(pipeline)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(j,)| j))
}

pub async fn save(pool: &PgPool, pipeline: &str, cursor: Value) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO etl_checkpoint (pipeline, cursor_json, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (pipeline) DO UPDATE SET cursor_json = EXCLUDED.cursor_json, updated_at = NOW()
        "#,
    )
    .bind(pipeline)
    .bind(cursor)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn save_ts_only(pool: &PgPool, pipeline: &str, last_ts: i64) -> anyhow::Result<()> {
    save(pool, pipeline, json!({ "last_timestamp": last_ts })).await
}

/// All pipeline checkpoints (for `/pipeline-status` / `status` CLI).
pub async fn list_all(pool: &PgPool) -> anyhow::Result<Vec<(String, Value, DateTime<Utc>)>> {
    let rows: Vec<(String, Value, DateTime<Utc>)> = sqlx::query_as(
        "SELECT pipeline, cursor_json, updated_at FROM etl_checkpoint ORDER BY pipeline",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}
