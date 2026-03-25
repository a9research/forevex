-- Gamma 市场标签缓存：slug → GET /markets/slug/{slug}?include_tag=true + GET /markets/{id}/tags
CREATE TABLE gamma_market_tags_cache (
    slug TEXT PRIMARY KEY,
    gamma_market_id TEXT,
    category TEXT,
    tags JSONB NOT NULL DEFAULT '[]'::jsonb,
    tags_source TEXT NOT NULL DEFAULT 'slug_include_tag',
    primary_bucket TEXT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fetch_error TEXT
);

CREATE INDEX idx_gamma_market_tags_fetched ON gamma_market_tags_cache (fetched_at DESC);
