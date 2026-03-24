-- 标记是否至少完成过一次持仓同步（区分「从未 sync」与「已 sync 但 0 仓」）
ALTER TABLE wallet_user_snapshot
ADD COLUMN positions_synced_at TIMESTAMPTZ;
