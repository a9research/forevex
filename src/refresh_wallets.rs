//! Rebuild `dim_wallets` from `fact_trades` (maker + taker).

use sqlx::PgPool;

pub async fn run(pool: &PgPool) -> anyhow::Result<u64> {
    let r = sqlx::query(
        r#"
        INSERT INTO dim_wallets (proxy_address, first_seen_at, last_seen_at)
        SELECT LOWER(addr), MIN(ts), MAX(ts)
        FROM (
            SELECT maker AS addr, ts FROM fact_trades
            UNION ALL
            SELECT taker AS addr, ts FROM fact_trades
        ) x
        GROUP BY addr
        ON CONFLICT (proxy_address) DO UPDATE SET
            first_seen_at = LEAST(dim_wallets.first_seen_at, EXCLUDED.first_seen_at),
            last_seen_at = GREATEST(dim_wallets.last_seen_at, EXCLUDED.last_seen_at)
        "#,
    )
    .execute(pool)
    .await?;

    Ok(r.rows_affected())
}
