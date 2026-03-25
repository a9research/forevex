use anyhow::Context;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::types::Json;
use sqlx::PgPool;

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
}

impl Store {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await
            .context("connect postgres")?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Minimal row so FK from `positions` / `market_activity_cache` succeeds.
    pub async fn ensure_wallet_row(&self, proxy: &str) -> anyhow::Result<()> {
        sqlx::query(
            r#"INSERT INTO wallet_user_snapshot (proxy_address) VALUES ($1)
               ON CONFLICT (proxy_address) DO NOTHING"#,
        )
        .bind(proxy)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn run_migrations(&self) -> anyhow::Result<()> {
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .context("sqlx migrate")?;
        Ok(())
    }

    pub async fn upsert_wallet_snapshot(
        &self,
        proxy: &str,
        resolved_username: Option<&str>,
        gamma_profile: Option<Value>,
        data_value: Option<Value>,
        data_traded: Option<Value>,
        user_stats: Option<Value>,
        user_pnl: Option<Value>,
    ) -> anyhow::Result<()> {
        let now = Utc::now();
        sqlx::query(
            r#"
            INSERT INTO wallet_user_snapshot (
              proxy_address, resolved_username, gamma_profile, data_value, data_traded,
              user_stats, user_pnl, profile_fetched_at, metrics_fetched_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (proxy_address) DO UPDATE SET
              resolved_username = EXCLUDED.resolved_username,
              gamma_profile = EXCLUDED.gamma_profile,
              data_value = EXCLUDED.data_value,
              data_traded = EXCLUDED.data_traded,
              user_stats = EXCLUDED.user_stats,
              user_pnl = EXCLUDED.user_pnl,
              profile_fetched_at = EXCLUDED.profile_fetched_at,
              metrics_fetched_at = EXCLUDED.metrics_fetched_at
            "#,
        )
        .bind(proxy)
        .bind(resolved_username)
        .bind(gamma_profile.map(|v| Json(v)))
        .bind(data_value.map(|v| Json(v)))
        .bind(data_traded.map(|v| Json(v)))
        .bind(user_stats.map(|v| Json(v)))
        .bind(user_pnl.map(|v| Json(v)))
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn fetch_wallet_snapshot(
        &self,
        proxy: &str,
    ) -> anyhow::Result<Option<WalletSnapshotRow>> {
        let row = sqlx::query_as::<_, WalletSnapshotRow>(
            r#"SELECT proxy_address, resolved_username, gamma_profile, data_value, data_traded,
                      user_stats, user_pnl, profile_fetched_at, metrics_fetched_at, positions_synced_at
               FROM wallet_user_snapshot WHERE proxy_address = $1"#,
        )
        .bind(proxy)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn replace_positions(
        &self,
        proxy: &str,
        state: &str,
        rows: &[(String, Value)],
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM positions WHERE proxy_address = $1 AND state = $2")
            .bind(proxy)
            .bind(state)
            .execute(&mut *tx)
            .await?;
        let now: DateTime<Utc> = Utc::now();
        for (key, raw) in rows {
            sqlx::query(
                r#"INSERT INTO positions (proxy_address, state, position_key, raw, synced_at)
                   VALUES ($1, $2, $3, $4, $5)
                   ON CONFLICT (proxy_address, state, position_key)
                   DO UPDATE SET raw = EXCLUDED.raw, synced_at = EXCLUDED.synced_at"#,
            )
            .bind(proxy)
            .bind(state)
            .bind(key)
            .bind(Json(raw.clone()))
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// 在成功写入 open/closed 持仓后调用，供客户端判断可走「只读 GET + 后台 sync」。
    pub async fn set_positions_synced_at(&self, proxy: &str) -> anyhow::Result<()> {
        let now = Utc::now();
        sqlx::query(
            r#"UPDATE wallet_user_snapshot SET positions_synced_at = $2 WHERE proxy_address = $1"#,
        )
        .bind(proxy)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_positions(
        &self,
        proxy: &str,
        state: Option<&str>,
    ) -> anyhow::Result<Vec<(String, Value, DateTime<Utc>)>> {
        let rows = if let Some(s) = state {
            sqlx::query_as::<_, PositionListRow>(
                r#"SELECT position_key, raw, synced_at FROM positions
                   WHERE proxy_address = $1 AND state = $2 ORDER BY position_key"#,
            )
            .bind(proxy)
            .bind(s)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, PositionListRow>(
                r#"SELECT position_key, raw, synced_at FROM positions
                   WHERE proxy_address = $1 ORDER BY state, position_key"#,
            )
            .bind(proxy)
            .fetch_all(&self.pool)
            .await?
        };
        Ok(rows
            .into_iter()
            .map(|r| (r.position_key, r.raw.0.clone(), r.synced_at))
            .collect())
    }

    pub async fn upsert_activity(
        &self,
        proxy: &str,
        market: &str,
        events: Value,
        max_ts: Option<i64>,
    ) -> anyhow::Result<()> {
        let now = Utc::now();
        sqlx::query(
            r#"INSERT INTO market_activity_cache (proxy_address, market_condition_id, events, max_event_ts, synced_at)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (proxy_address, market_condition_id)
               DO UPDATE SET events = EXCLUDED.events, max_event_ts = EXCLUDED.max_event_ts, synced_at = EXCLUDED.synced_at"#,
        )
        .bind(proxy)
        .bind(market)
        .bind(Json(events))
        .bind(max_ts)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn fetch_activity(
        &self,
        proxy: &str,
        market: &str,
    ) -> anyhow::Result<Option<(Value, Option<i64>, DateTime<Utc>)>> {
        let row = sqlx::query_as::<_, ActivityRow>(
            r#"SELECT events, max_event_ts, synced_at FROM market_activity_cache
               WHERE proxy_address = $1 AND market_condition_id = $2"#,
        )
        .bind(proxy)
        .bind(market)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| (r.events.0, r.max_event_ts, r.synced_at)))
    }

    pub async fn fetch_gamma_market_tags_cache(
        &self,
        slug: &str,
    ) -> anyhow::Result<Option<GammaMarketTagsCacheRow>> {
        let row = sqlx::query_as::<_, GammaMarketTagsCacheRow>(
            r#"SELECT slug, gamma_market_id, category, tags, tags_source, primary_bucket, fetched_at, fetch_error
               FROM gamma_market_tags_cache WHERE slug = $1"#,
        )
        .bind(slug)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn upsert_gamma_market_tags_cache(
        &self,
        slug: &str,
        gamma_market_id: Option<&str>,
        category: Option<&str>,
        tags: &Value,
        tags_source: &str,
        primary_bucket: &str,
        fetch_error: Option<&str>,
    ) -> anyhow::Result<()> {
        let now = Utc::now();
        sqlx::query(
            r#"INSERT INTO gamma_market_tags_cache (
              slug, gamma_market_id, category, tags, tags_source, primary_bucket, fetched_at, fetch_error
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (slug) DO UPDATE SET
              gamma_market_id = EXCLUDED.gamma_market_id,
              category = EXCLUDED.category,
              tags = EXCLUDED.tags,
              tags_source = EXCLUDED.tags_source,
              primary_bucket = EXCLUDED.primary_bucket,
              fetched_at = EXCLUDED.fetched_at,
              fetch_error = EXCLUDED.fetch_error"#,
        )
        .bind(slug)
        .bind(gamma_market_id)
        .bind(category)
        .bind(Json(tags.clone()))
        .bind(tags_source)
        .bind(primary_bucket)
        .bind(now)
        .bind(fetch_error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct GammaMarketTagsCacheRow {
    pub slug: String,
    pub gamma_market_id: Option<String>,
    pub category: Option<String>,
    pub tags: Json<Value>,
    pub tags_source: String,
    pub primary_bucket: String,
    pub fetched_at: DateTime<Utc>,
    pub fetch_error: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct WalletSnapshotRow {
    pub proxy_address: String,
    pub resolved_username: Option<String>,
    pub gamma_profile: Option<Json<Value>>,
    pub data_value: Option<Json<Value>>,
    pub data_traded: Option<Json<Value>>,
    pub user_stats: Option<Json<Value>>,
    pub user_pnl: Option<Json<Value>>,
    pub profile_fetched_at: Option<DateTime<Utc>>,
    pub metrics_fetched_at: Option<DateTime<Utc>>,
    pub positions_synced_at: Option<DateTime<Utc>>,
}

#[derive(Debug, sqlx::FromRow)]
struct PositionListRow {
    position_key: String,
    raw: Json<Value>,
    synced_at: DateTime<Utc>,
}

#[derive(Debug, sqlx::FromRow)]
struct ActivityRow {
    events: Json<Value>,
    max_event_ts: Option<i64>,
    synced_at: DateTime<Utc>,
}
