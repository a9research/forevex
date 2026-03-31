-- enrich-gamma: add retry state + separate "enriched" marker.

ALTER TABLE dim_markets
    ADD COLUMN IF NOT EXISTS enrich_gamma_fetched_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS enrich_gamma_failed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS enrich_gamma_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS enrich_gamma_last_error TEXT;

CREATE INDEX IF NOT EXISTS idx_dim_markets_enrich_gamma_pending
    ON dim_markets (market_id)
    WHERE enrich_gamma_fetched_at IS NULL;

