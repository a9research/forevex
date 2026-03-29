use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;

pub async fn connect(
    database_url: &str,
    max_connections: u32,
    acquire_timeout: Duration,
) -> anyhow::Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(acquire_timeout)
        .connect(database_url)
        .await?;
    Ok(pool)
}
