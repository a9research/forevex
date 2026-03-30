//! `status` CLI 与 `/pipeline-status` 共用：OSS PMA 进度 + `etl_checkpoint` 全表。

use crate::checkpoint;
use crate::config::Config;
use crate::pma;
use serde_json::{json, Value};
use sqlx::PgPool;

pub async fn pipeline_report(pool: &PgPool, cfg: &Config) -> anyhow::Result<Value> {
    let checkpoints = checkpoint::list_all(pool).await?;
    let pma = pma::pipeline_pma_status(pool, cfg).await?;
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
        "etl_checkpoints": etl,
    }))
}
