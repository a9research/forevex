//! Rebuild `agg_wallet_topic` and `agg_global_daily` (maker-only volume).

use sqlx::PgPool;

pub async fn run(pool: &PgPool) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;

    sqlx::query("DELETE FROM agg_wallet_topic")
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM agg_global_daily")
        .execute(&mut *tx)
        .await?;

    sqlx::query(
        r#"
        INSERT INTO agg_wallet_topic (
            proxy_address, topic_primary, period, volume_usd, trade_count, share_of_wallet, computed_at
        )
        SELECT
            q.proxy_address,
            q.topic_primary,
            q.period,
            q.volume_usd,
            q.trade_count,
            q.volume_usd / NULLIF(
                SUM(q.volume_usd) OVER (PARTITION BY q.proxy_address, q.period),
                0
            ),
            NOW()
        FROM (
            SELECT
                f.maker AS proxy_address,
                COALESCE(d.topic_primary, 'other') AS topic_primary,
                'all'::text AS period,
                SUM(COALESCE(f.usd_amount, 0)) AS volume_usd,
                COUNT(*)::bigint AS trade_count
            FROM fact_trades f
            INNER JOIN dim_markets d ON d.market_id = f.market_id
            GROUP BY f.maker, COALESCE(d.topic_primary, 'other')
            UNION ALL
            SELECT
                f.maker,
                COALESCE(d.topic_primary, 'other'),
                '7d',
                SUM(COALESCE(f.usd_amount, 0)),
                COUNT(*)::bigint
            FROM fact_trades f
            INNER JOIN dim_markets d ON d.market_id = f.market_id
            WHERE f.ts >= (NOW() AT TIME ZONE 'utc') - INTERVAL '7 days'
            GROUP BY f.maker, COALESCE(d.topic_primary, 'other')
            UNION ALL
            SELECT
                f.maker,
                COALESCE(d.topic_primary, 'other'),
                '30d',
                SUM(COALESCE(f.usd_amount, 0)),
                COUNT(*)::bigint
            FROM fact_trades f
            INNER JOIN dim_markets d ON d.market_id = f.market_id
            WHERE f.ts >= (NOW() AT TIME ZONE 'utc') - INTERVAL '30 days'
            GROUP BY f.maker, COALESCE(d.topic_primary, 'other')
            UNION ALL
            SELECT
                f.maker,
                COALESCE(d.topic_primary, 'other'),
                '90d',
                SUM(COALESCE(f.usd_amount, 0)),
                COUNT(*)::bigint
            FROM fact_trades f
            INNER JOIN dim_markets d ON d.market_id = f.market_id
            WHERE f.ts >= (NOW() AT TIME ZONE 'utc') - INTERVAL '90 days'
            GROUP BY f.maker, COALESCE(d.topic_primary, 'other')
        ) q
        "#,
    )
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO agg_global_daily (
            stat_date, topic_primary, volume_usd, trade_count, unique_wallets
        )
        SELECT
            (f.ts AT TIME ZONE 'UTC')::date AS stat_date,
            COALESCE(d.topic_primary, 'other') AS topic_primary,
            SUM(COALESCE(f.usd_amount, 0)) AS volume_usd,
            COUNT(*)::bigint AS trade_count,
            COUNT(DISTINCT f.maker)::bigint AS unique_wallets
        FROM fact_trades f
        INNER JOIN dim_markets d ON d.market_id = f.market_id
        GROUP BY 1, 2
        "#,
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    tracing::info!("aggregate: agg_wallet_topic + agg_global_daily rebuilt");
    Ok(())
}
