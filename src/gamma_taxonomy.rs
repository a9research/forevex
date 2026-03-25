//! Gamma **`/tags`** + **`/sports`**（与 polymarket-account-analyzer `GammaTaxonomy` 对齐）：
//! 用官方 **`/sports`** tag id 识别体育；其余类目用 **启发式 + 与官网 [Browse / Topics](https://polymarket.com/) 一致的顶层桶** 归并，
//! 避免子类（如 soccer、某选举标签）与顶层（Sports、Politics）在 **Market distribution** 里并列。
//!
//! 归并规则见 [`rollup_to_polymarket_topic`]；可按实际数据继续扩充关键词表。

use crate::upstream::Upstream;
use anyhow::Context;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

static TAXONOMY_CACHE: std::sync::OnceLock<Mutex<Option<(Instant, u64, Arc<GammaTaxonomy>)>>> =
    std::sync::OnceLock::new();

#[derive(Debug, Clone, Default)]
pub struct GammaTaxonomy {
    /// Tag id → 展示名（优先 label）。
    tags_by_id: HashMap<String, String>,
    /// `/sports` 中 tag id → sport 键（保留供后续二级钻取；当前分布用 `sport_tag_ids` 归并 `sports`）。
    #[allow(dead_code)]
    tag_id_to_sport: HashMap<String, String>,
    /// 落在任一项运动下的 tag id。
    sport_tag_ids: HashSet<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SportsMetadataRow {
    #[serde(default)]
    sport: Option<String>,
    /// 逗号分隔 tag id
    #[serde(default)]
    tags: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TagRow {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    slug: Option<String>,
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

/// 体育子类（含 `/sports` 下的运动名、常见联赛关键词）。
fn is_sports_cluster(n: &str) -> bool {
    if n == "sports" || n == "sport" {
        return true;
    }
    const SUB: &[&str] = &[
        "soccer",
        "nba",
        "basketball",
        "ncaa",
        "football",
        "nfl",
        "mlb",
        "nhl",
        "f1",
        "tennis",
        "golf",
        "mma",
        "ufc",
        "esports",
        "cbb",
        "wnba",
        "mls",
        "epl",
        "uel",
        "ucl",
        "nascar",
        "boxing",
        "cricket",
        "rugby",
        "olympics",
    ];
    SUB.iter().any(|s| *s == n)
}

fn is_politics_cluster(n: &str) -> bool {
    if n.contains("geopolitic") {
        return false;
    }
    if n.contains("politic") {
        return true;
    }
    if n.contains("election") && !n.contains("selection") {
        return true;
    }
    const SUB: &[&str] = &[
        "elections",
        "election",
        "congress",
        "senate",
        "midterms",
        "primaries",
        "parliament",
        "white-house",
        "presidential",
        "government",
        "homeland-security",
        "shutdown",
        "democrat",
        "republican",
    ];
    SUB.iter().any(|s| *s == n)
}

fn is_crypto_cluster(n: &str) -> bool {
    if n.contains("pop-culture") {
        return false;
    }
    if n.contains("crypto") {
        return true;
    }
    const SUB: &[&str] = &[
        "bitcoin",
        "ethereum",
        "defi",
        "solana",
        "blockchain",
        "nft",
        "altcoin",
    ];
    SUB.iter().any(|s| n.contains(s) || *s == n)
}

fn is_ai_cluster(n: &str) -> bool {
    n == "ai"
        || n.contains("artificial-intelligence")
        || n.contains("machine-learning")
        || n.contains("openai")
        || n.contains("llm")
}

fn is_tech_cluster(n: &str) -> bool {
    if is_ai_cluster(n) {
        return false;
    }
    n == "tech"
        || n.contains("technology")
        || n.contains("big-tech")
        || n.contains("software")
        || n.contains("spacex")
}

fn is_finance_cluster(n: &str) -> bool {
    const SUB: &[&str] = &[
        "finance",
        "fed",
        "rates",
        "commodities",
        "commodity",
        "stocks",
        "bonds",
        "forex",
        "ipo",
        "tariffs",
    ];
    SUB.iter().any(|s| *s == n || n.contains(s))
}

fn is_economy_cluster(n: &str) -> bool {
    const SUB: &[&str] = &[
        "economy",
        "recession",
        "gdp",
        "inflation",
        "employment",
        "jobs",
    ];
    SUB.iter().any(|s| *s == n || n.contains(s))
}

fn is_pop_culture_cluster(n: &str) -> bool {
    const SUB: &[&str] = &[
        "pop-culture",
        "movies",
        "music",
        "celebrity",
        "entertainment",
        "tv",
        "oscars",
        "grammy",
    ];
    SUB.iter().any(|s| *s == n || n.contains(s))
}

fn is_culture_cluster(n: &str) -> bool {
    if is_pop_culture_cluster(n) {
        return false;
    }
    n == "culture" || n.contains("literature") || n.contains("books")
}

fn is_geopolitics_cluster(n: &str) -> bool {
    const SUB: &[&str] = &[
        "geopolitic",
        "geopolitics",
        "middle-east",
        "iran",
        "israel",
        "ukraine",
        "russia",
        "china",
        "taiwan",
        "north-korea",
        "lebanon",
        "syria",
        "yemen",
        "hormuz",
        "strait",
        "nato",
        "ceasefire",
        "invasion",
    ];
    SUB.iter().any(|s| *s == n || n.contains(s))
}

fn is_weather_cluster(n: &str) -> bool {
    n.contains("weather") || n.contains("temperature") || n.contains("hurricane")
}

/// 将 Gamma `category` / 标签展示名（已 `normalize_bucket_key`）映射到与官网 Topics 一致的**顶层桶**。
fn rollup_to_polymarket_topic(n: &str) -> Option<&'static str> {
    if is_sports_cluster(n) {
        return Some("sports");
    }
    // 「地缘政治」须在「政治」之前：避免 `geopolitics` 命中 `politic` 子串
    if is_geopolitics_cluster(n) {
        return Some("geopolitics");
    }
    if is_politics_cluster(n) {
        return Some("politics");
    }
    if is_crypto_cluster(n) {
        return Some("crypto");
    }
    if is_ai_cluster(n) {
        return Some("ai");
    }
    if is_tech_cluster(n) {
        return Some("tech");
    }
    if is_finance_cluster(n) {
        return Some("finance");
    }
    if is_economy_cluster(n) {
        return Some("economy");
    }
    if is_pop_culture_cluster(n) {
        return Some("pop-culture");
    }
    if is_culture_cluster(n) {
        return Some("culture");
    }
    if is_weather_cluster(n) {
        return Some("weather");
    }
    None
}

fn apply_topic_rollup(normalized: &str) -> String {
    rollup_to_polymarket_topic(normalized)
        .map(String::from)
        .unwrap_or_else(|| normalized.to_string())
}

impl GammaTaxonomy {
    pub fn empty() -> Self {
        Self::default()
    }

    pub async fn fetch(up: &Upstream) -> anyhow::Result<Self> {
        let mut tags_by_id = HashMap::new();
        let mut tag_id_to_sport = HashMap::new();
        let mut sport_tag_ids = HashSet::new();

        let mut offset: u32 = 0;
        const PAGE: u32 = 500;
        loop {
            let url = format!(
                "{}/tags?limit={}&offset={}",
                up.config().gamma_origin, PAGE, offset
            );
            let v = up.get_json_optional(&url).await?;
            let page: Vec<TagRow> =
                serde_json::from_value(v).with_context(|| format!("gamma /tags offset={offset}"))?;
            let n = page.len();
            for row in page {
                let Some(id) = row.id.filter(|s| !s.trim().is_empty()) else {
                    continue;
                };
                let disp = row
                    .label
                    .filter(|s| !s.trim().is_empty())
                    .or(row.slug.clone())
                    .unwrap_or_else(|| id.clone());
                tags_by_id.insert(id, disp);
            }
            if n == 0 {
                break;
            }
            offset += n as u32;
            if n < PAGE as usize {
                break;
            }
        }

        let sports_url = format!("{}/sports", up.config().gamma_origin);
        let sv = up.get_json_optional(&sports_url).await?;
        let sports: Vec<SportsMetadataRow> =
            serde_json::from_value(sv).context("gamma /sports parse")?;

        for row in sports {
            let sport_key = normalize_bucket_key(
                row.sport
                    .as_deref()
                    .unwrap_or("sports")
                    .trim(),
            );
            let Some(tags_csv) = row.tags.as_deref() else {
                continue;
            };
            for raw in tags_csv.split(',') {
                let tid = raw.trim();
                if tid.is_empty() {
                    continue;
                }
                sport_tag_ids.insert(tid.to_string());
                tag_id_to_sport
                    .entry(tid.to_string())
                    .or_insert_with(|| sport_key.clone());
            }
        }

        Ok(Self {
            tags_by_id,
            tag_id_to_sport,
            sport_tag_ids,
        })
    }

    /// 进程内 TTL 缓存（多请求复用，减少全量 `/tags` 拉取）。
    pub async fn cached(up: &Upstream, ttl_sec: u64) -> Arc<GammaTaxonomy> {
        let cell = TAXONOMY_CACHE.get_or_init(|| Mutex::new(None));
        let mut g = cell.lock().await;
        let now = Instant::now();
        if let Some((t, cached_ttl, arc)) = g.as_ref() {
            if *cached_ttl == ttl_sec && now.duration_since(*t) < Duration::from_secs(ttl_sec.max(60)) {
                return arc.clone();
            }
        }
        let arc = Arc::new(
            Self::fetch(up)
                .await
                .unwrap_or_else(|e| {
                    tracing::warn!("gamma taxonomy fetch: {e:#}");
                    Self::empty()
                }),
        );
        *g = Some((now, ttl_sec, arc.clone()));
        arc
    }

    /// 账户页 **Market distribution** 用桶名：类目/标签经 [`rollup_to_polymarket_topic`] 归并为官网 Topics 向的顶层（`sports`、`politics`、`crypto`…）。
    pub fn distribution_bucket(
        &self,
        category: Option<&str>,
        tags: &[serde_json::Value],
        slug_for_fallback: &str,
    ) -> String {
        if let Some(c) = category.map(str::trim).filter(|s| !s.is_empty()) {
            let n = normalize_bucket_key(c);
            return apply_topic_rollup(&n);
        }

        for t in tags {
            let Some(o) = t.as_object() else { continue };
            let id = o.get("id").and_then(|v| v.as_str()).map(str::trim).unwrap_or("");
            if !id.is_empty() && self.sport_tag_ids.contains(id) {
                return "sports".to_string();
            }
        }

        for t in tags {
            let Some(o) = t.as_object() else { continue };
            let id = o.get("id").and_then(|v| v.as_str()).map(str::trim).unwrap_or("");
            if !id.is_empty() {
                if let Some(lab) = self.tags_by_id.get(id) {
                    let n = normalize_bucket_key(lab);
                    return apply_topic_rollup(&n);
                }
            }
        }

        for t in tags {
            let Some(o) = t.as_object() else { continue };
            if let Some(l) = o.get("label").and_then(|v| v.as_str()).map(str::trim).filter(|s| !s.is_empty())
            {
                let n = normalize_bucket_key(l);
                return apply_topic_rollup(&n);
            }
        }

        for t in tags {
            let Some(o) = t.as_object() else { continue };
            if let Some(s) = o.get("slug").and_then(|v| v.as_str()).map(str::trim).filter(|s| !s.is_empty())
            {
                let n = normalize_bucket_key(s);
                return apply_topic_rollup(&n);
            }
        }

        let fb = crate::market_type::classify_slug(slug_for_fallback).to_string();
        apply_topic_rollup(&fb)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sports_category_rollsup() {
        let t = GammaTaxonomy::empty();
        let b = t.distribution_bucket(Some("Sports"), &[], "x");
        assert_eq!(b, "sports");
    }

    #[test]
    fn soccer_label_rollsup() {
        let t = GammaTaxonomy::empty();
        let tags = vec![json!({"label": "Soccer", "id": "999"})];
        let b = t.distribution_bucket(None, &tags, "x");
        assert_eq!(b, "sports");
    }

    #[test]
    fn us_politics_category_rollsup() {
        let t = GammaTaxonomy::empty();
        let b = t.distribution_bucket(Some("US Politics"), &[], "x");
        assert_eq!(b, "politics");
    }

    #[test]
    fn geopolitics_before_politics() {
        let t = GammaTaxonomy::empty();
        let b = t.distribution_bucket(Some("Geopolitics"), &[], "x");
        assert_eq!(b, "geopolitics");
    }

    #[test]
    fn bitcoin_tag_rollsup() {
        let t = GammaTaxonomy::empty();
        let tags = vec![json!({"label": "Bitcoin", "id": "x"})];
        let b = t.distribution_bucket(None, &tags, "z");
        assert_eq!(b, "crypto");
    }
}
