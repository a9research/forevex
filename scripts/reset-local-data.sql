-- 清空 forevex 业务数据（用户快照、持仓、activity 缓存、Gamma 标签缓存）。
-- 不删除 sqlx 迁移表 `_sqlx_migrations`，无需重跑迁移。
--
-- 用法（按你的 DATABASE_URL 改连接方式）：
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/reset-local-data.sql
-- 或：
--   docker compose exec -T postgres psql -U forevex -d forevex -v ON_ERROR_STOP=1 -f - < scripts/reset-local-data.sql

BEGIN;

-- 子表随 wallet 级联清空（positions / market_activity_cache 均 ON DELETE CASCADE 指向 wallet_user_snapshot）
TRUNCATE TABLE wallet_user_snapshot CASCADE;

-- 独立表：与钱包无关的全局 slug 缓存
TRUNCATE TABLE gamma_market_tags_cache;

COMMIT;

-- 验证（可选）：
-- SELECT 'wallet_user_snapshot' AS t, count(*) FROM wallet_user_snapshot
-- UNION ALL SELECT 'positions', count(*) FROM positions
-- UNION ALL SELECT 'market_activity_cache', count(*) FROM market_activity_cache
-- UNION ALL SELECT 'gamma_market_tags_cache', count(*) FROM gamma_market_tags_cache;
