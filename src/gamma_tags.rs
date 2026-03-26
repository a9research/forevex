//! Gamma：`GET /markets/slug/{slug}?include_tag=true` 取 `id`/`category`，再
//! [`GET /markets/{id}/tags`](https://docs.polymarket.com/api-reference/markets/get-market-tags-by-id)
//! 拉标签；结果写入 `gamma_market_tags_cache`，供持仓聚合按 **primary_bucket** 分类。

use crate::config::Config;
use crate::gamma_taxonomy::{canonicalize_bucket_name, GammaTaxonomy};
use crate::market_type::classify_slug;
use crate::store::{GammaMarketTagsCacheRow, Store};
use crate::upstream::Upstream;
use anyhow::Context;
use chrono::Utc;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// 与持仓 `slug` 对齐：trim + 小写，作缓存键。
pub fn normalize_slug_key(s: &str) -> String {
    s.trim().to_lowercase()
}

fn normalize_bucket_key(s: &str) -> String {
    let s = s.trim().to_lowercase();
    s.chars()
        .map(|c| {
            if c.is_whitespace() || c == '_' {
                '-'
            } else {
                c
            }
        })
        .collect()
}

/// 优先 `category`，其次 tags 的 `label` / `slug`，最后回退 `classify_slug(slug)`。
pub fn primary_bucket_from_gamma_parts(
    category: Option<&str>,
    tags: &[Value],
    slug_for_fallback: &str,
) -> String {
    if let Some(c) = category.map(str::trim).filter(|s| !s.is_empty()) {
        return normalize_bucket_key(c);
    }
    for t in tags {
        let Some(o) = t.as_object() else { continue };
        if let Some(l) = o.get("label").and_then(|v| v.as_str()).map(str::trim) {
            if !l.is_empty() {
                return normalize_bucket_key(l);
            }
        }
    }
    for t in tags {
        let Some(o) = t.as_object() else { continue };
        if let Some(s) = o.get("slug").and_then(|v| v.as_str()).map(str::trim) {
            if !s.is_empty() {
                return normalize_bucket_key(s);
            }
        }
    }
    classify_slug(slug_for_fallback).to_string()
}

fn gamma_market_id_num(v: &Value) -> Option<i64> {
    v.get("id").and_then(|x| {
        if let Some(i) = x.as_i64() {
            return Some(i);
        }
        x.as_str()?.parse().ok()
    })
}

/// 拉取并解析单个 slug；失败时仍返回可展示桶名（`classify_slug` 或 `unknown`）。
pub async fn resolve_slug_tags(
    up: &Upstream,
    taxonomy: &GammaTaxonomy,
    slug: &str,
) -> anyhow::Result<(String, Option<String>, Option<String>, Value, &'static str, Option<String>)> {
    let slug_trim = slug.trim();
    if slug_trim.is_empty() {
        return Ok((
            "unknown".to_string(),
            None,
            None,
            Value::Array(vec![]),
            "empty_slug",
            None,
        ));
    }

    let market = up
        .gamma_market_by_slug_include_tags(slug_trim)
        .await
        .context("gamma market by slug")?;

    let gid = gamma_market_id_num(&market).map(|n| n.to_string());
    let category = market
        .get("category")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let mut tags_val = market.get("tags").cloned().unwrap_or(Value::Array(vec![]));
    let mut src: &'static str = "slug_include_tag";

    if let Some(n) = gamma_market_id_num(&market) {
        match up.gamma_market_tags_by_id(n).await {
            Ok(arr) => {
                if arr.as_array().map(|a| !a.is_empty()).unwrap_or(false) {
                    tags_val = arr;
                    src = "market_id_tags";
                }
            }
            Err(e) => {
                tracing::debug!(slug = %slug_trim, "gamma /markets/{{id}}/tags: {e:#}");
            }
        }
    }

    let tag_arr = tags_val.as_array().map(Vec::as_slice).unwrap_or(&[]);
    let bucket = taxonomy.distribution_bucket(category.as_deref(), tag_arr, slug_trim);

    Ok((bucket, gid, category, tags_val, src, None))
}

/// 为去重后的 slug 列表填充 `normalize_slug(slug) → primary_bucket`（读缓存、按需刷新）。
pub async fn ensure_gamma_buckets_for_slugs(
    store: &Store,
    up: &Upstream,
    cfg: &Config,
    slugs: &HashSet<String>,
) -> anyhow::Result<HashMap<String, String>> {
    let mut out: HashMap<String, String> = HashMap::new();
    let mut repr: HashMap<String, String> = HashMap::new();
    for s in slugs {
        let k = normalize_slug_key(s);
        if k.is_empty() {
            out.insert(k, "unknown".to_string());
            continue;
        }
        repr.entry(k).or_insert_with(|| s.clone());
    }

    let mut keys: Vec<String> = repr.keys().cloned().collect();
    keys.sort();
    let max = cfg.gamma_max_slug_enrich as usize;

    let taxonomy = GammaTaxonomy::cached(up, cfg.gamma_taxonomy_cache_ttl_sec).await;

    for (i, key) in keys.iter().enumerate() {
        let raw = repr.get(key).unwrap();
        if i >= max {
            out.insert(
                key.clone(),
                canonicalize_bucket_name(&classify_slug(raw.as_str()).to_string()),
            );
            continue;
        }

        if let Some(row) = store.fetch_gamma_market_tags_cache(key).await? {
            if !cache_row_stale(&row, cfg) {
                out.insert(key.clone(), canonicalize_bucket_name(&row.primary_bucket));
                continue;
            }
        }

        match resolve_slug_tags(up, &taxonomy, raw).await {
            Ok((bucket, gid, cat, tags, src, _)) => {
                if let Err(e) = store
                    .upsert_gamma_market_tags_cache(
                        key,
                        gid.as_deref(),
                        cat.as_deref(),
                        &tags,
                        src,
                        &bucket,
                        None,
                    )
                    .await
                {
                    tracing::warn!(slug = %key, "upsert gamma_market_tags_cache: {e:#}");
                }
                out.insert(key.clone(), bucket);
            }
            Err(e) => {
                tracing::warn!(slug = %key, "resolve_slug_tags: {e:#}");
                let fallback = canonicalize_bucket_name(&classify_slug(raw.as_str()).to_string());
                let _ = store
                    .upsert_gamma_market_tags_cache(
                        key,
                        None,
                        None,
                        &Value::Array(vec![]),
                        "error",
                        &fallback,
                        Some(&format!("{e:#}")),
                    )
                    .await;
                out.insert(key.clone(), fallback);
            }
        }
    }

    Ok(out)
}

fn cache_row_stale(row: &GammaMarketTagsCacheRow, cfg: &Config) -> bool {
    let now = Utc::now();
    let age = now.signed_duration_since(row.fetched_at);
    let ttl_ok = chrono::Duration::seconds(cfg.gamma_tags_cache_ttl_sec as i64);
    let ttl_err = chrono::Duration::hours(1);

    if row.fetch_error.is_none() {
        age > ttl_ok
    } else {
        age > ttl_err
    }
}

/// 从 `positions` JSON 收集非空 slug（去重、规范化键）.
pub fn collect_slugs_from_positions(open: &[Value], closed: &[Value]) -> HashSet<String> {
    let mut s = HashSet::new();
    for v in open.iter().chain(closed.iter()) {
        let Some(o) = v.as_object() else { continue };
        let slug = o
            .get("slug")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim();
        if !slug.is_empty() {
            s.insert(slug.to_string());
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn primary_prefers_category() {
        let b = primary_bucket_from_gamma_parts(
            Some(" US Politics "),
            &[],
            "x",
        );
        assert_eq!(b, "us-politics");
    }

    #[test]
    fn primary_from_tag_label() {
        let tags = vec![json!({"label": "Crypto", "id": "1"})];
        let b = primary_bucket_from_gamma_parts(None, &tags, "fallback-slug");
        assert_eq!(b, "crypto");
    }
}
