//! Shared `etl_checkpoint` helpers.

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
