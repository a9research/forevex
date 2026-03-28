-- Polymarket pipeline schema (see docs/polymarket-data-platform-unified.md)

CREATE TABLE IF NOT EXISTS dim_markets (
    market_id TEXT PRIMARY KEY,
    condition_id TEXT,
    market_slug TEXT,
    question TEXT,
    answer1 TEXT,
    answer2 TEXT,
    neg_risk BOOLEAN,
    token1 TEXT NOT NULL,
    token2 TEXT NOT NULL,
    volume NUMERIC,
    ticker TEXT,
    closed_time TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    category_raw TEXT,
    topic_primary TEXT,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_markets_condition ON dim_markets (condition_id);
CREATE INDEX IF NOT EXISTS idx_dim_markets_slug ON dim_markets (market_slug);

CREATE TABLE IF NOT EXISTS stg_order_filled (
    id BIGSERIAL PRIMARY KEY,
    timestamp_i64 BIGINT NOT NULL,
    maker TEXT NOT NULL,
    maker_asset_id TEXT NOT NULL,
    maker_amount_filled TEXT NOT NULL,
    taker TEXT NOT NULL,
    taker_asset_id TEXT NOT NULL,
    taker_amount_filled TEXT NOT NULL,
    transaction_hash TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_stg_order_filled UNIQUE (transaction_hash, maker, taker, timestamp_i64, maker_asset_id, taker_asset_id)
);

CREATE INDEX IF NOT EXISTS idx_stg_ts ON stg_order_filled (timestamp_i64);
CREATE INDEX IF NOT EXISTS idx_stg_tx ON stg_order_filled (transaction_hash);

CREATE TABLE IF NOT EXISTS fact_trades (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    market_id TEXT REFERENCES dim_markets (market_id),
    maker TEXT NOT NULL,
    taker TEXT NOT NULL,
    nonusdc_side TEXT,
    maker_direction TEXT,
    taker_direction TEXT,
    price NUMERIC,
    usd_amount NUMERIC,
    token_amount NUMERIC,
    transaction_hash TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'polymarket_pipeline',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_fact_trades UNIQUE (transaction_hash, maker, taker, ts)
);

CREATE INDEX IF NOT EXISTS idx_fact_trades_ts ON fact_trades (ts);
CREATE INDEX IF NOT EXISTS idx_fact_trades_market ON fact_trades (market_id);
CREATE INDEX IF NOT EXISTS idx_fact_trades_maker ON fact_trades (maker);

CREATE TABLE IF NOT EXISTS dim_wallets (
    proxy_address TEXT PRIMARY KEY CHECK (proxy_address ~ '^0x[a-fA-F0-9]{40}$'),
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_account_activities (
    id BIGSERIAL PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL,
    block_number BIGINT,
    tx_hash TEXT,
    proxy_address TEXT NOT NULL,
    activity_type TEXT NOT NULL,
    condition_id TEXT,
    amount_usd NUMERIC,
    payload_json JSONB,
    source TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wallet_api_snapshot (
    id BIGSERIAL PRIMARY KEY,
    proxy_address TEXT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    user_stats_json JSONB,
    user_pnl_json JSONB,
    gamma_profile_json JSONB
);

CREATE INDEX IF NOT EXISTS idx_wallet_snap_proxy ON wallet_api_snapshot (proxy_address, fetched_at DESC);

CREATE TABLE IF NOT EXISTS agg_wallet_topic (
    proxy_address TEXT NOT NULL,
    topic_primary TEXT NOT NULL,
    period TEXT NOT NULL,
    volume_usd NUMERIC NOT NULL,
    trade_count BIGINT NOT NULL,
    share_of_wallet NUMERIC,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (proxy_address, topic_primary, period)
);

CREATE TABLE IF NOT EXISTS agg_global_daily (
    stat_date DATE NOT NULL,
    topic_primary TEXT NOT NULL,
    volume_usd NUMERIC NOT NULL,
    trade_count BIGINT NOT NULL,
    unique_wallets BIGINT,
    PRIMARY KEY (stat_date, topic_primary)
);

CREATE TABLE IF NOT EXISTS etl_checkpoint (
    pipeline TEXT PRIMARY KEY,
    cursor_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
