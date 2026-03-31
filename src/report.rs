//! `status` CLI 与 `/pipeline-status` 共用：OSS PMA 进度 + `etl_checkpoint` 全表。

use crate::checkpoint;
use crate::config::Config;
use crate::pma;
use serde_json::{json, Value};
use sqlx::PgPool;

pub async fn pipeline_report(pool: &PgPool, cfg: &Config) -> anyhow::Result<Value> {
    let checkpoints = checkpoint::list_all(pool).await?;
    let pma = pma::pipeline_pma_status(pool, cfg).await?;
    let stg_rows: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM stg_order_filled")
        .fetch_one(pool)
        .await
        .unwrap_or(0);
    let fact_rows: i64 = sqlx::query_scalar("SELECT COUNT(*)::bigint FROM fact_trades")
        .fetch_one(pool)
        .await
        .unwrap_or(0);
    let etl: Vec<Value> = checkpoints
        .into_iter()
        .map(|(p, j, t)| {
            json!({
                "pipeline": p,
                "cursor": j,
                "updated_at": t,
            })
        })
        .collect();
    Ok(json!({
        "pma": pma,
        "tables": {
            "stg_order_filled": stg_rows,
            "fact_trades": fact_rows
        },
        "etl_checkpoints": etl,
    }))
}
