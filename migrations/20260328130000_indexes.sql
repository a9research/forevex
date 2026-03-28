CREATE INDEX IF NOT EXISTS idx_fact_act_proxy ON fact_account_activities (proxy_address);
CREATE INDEX IF NOT EXISTS idx_fact_act_type ON fact_account_activities (activity_type);
CREATE INDEX IF NOT EXISTS idx_fact_trades_ts_market ON fact_trades (ts, market_id);
