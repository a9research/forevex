-- Layered persistence: wallet snapshot -> positions -> activity cache

CREATE TABLE wallet_user_snapshot (
    proxy_address TEXT PRIMARY KEY CHECK (proxy_address ~ '^0x[a-fA-F0-9]{40}$'),
    resolved_username TEXT,
    gamma_profile JSONB,
    data_value JSONB,
    data_traded JSONB,
    user_stats JSONB,
    user_pnl JSONB,
    profile_fetched_at TIMESTAMPTZ,
    metrics_fetched_at TIMESTAMPTZ
);

CREATE INDEX idx_wallet_user_snapshot_username ON wallet_user_snapshot (resolved_username);

CREATE TABLE positions (
    id BIGSERIAL PRIMARY KEY,
    proxy_address TEXT NOT NULL REFERENCES wallet_user_snapshot (proxy_address) ON DELETE CASCADE,
    state TEXT NOT NULL CHECK (state IN ('open', 'closed')),
    position_key TEXT NOT NULL,
    raw JSONB NOT NULL,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (proxy_address, state, position_key)
);

CREATE INDEX idx_positions_proxy_state ON positions (proxy_address, state);

CREATE TABLE market_activity_cache (
    id BIGSERIAL PRIMARY KEY,
    proxy_address TEXT NOT NULL REFERENCES wallet_user_snapshot (proxy_address) ON DELETE CASCADE,
    market_condition_id TEXT NOT NULL,
    events JSONB NOT NULL DEFAULT '[]'::jsonb,
    max_event_ts BIGINT,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (proxy_address, market_condition_id)
);

CREATE INDEX idx_activity_proxy ON market_activity_cache (proxy_address);
