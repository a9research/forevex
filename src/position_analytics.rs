//! 仅基于 Data API **open + closed positions** 的聚合（无 `/trades`）。
//! 口径说明见响应内 `notes` 与 `source` 字段。

use crate::gamma_tags::normalize_slug_key;
use crate::market_type::{classify_slug, fallback_bucket_from_title};
use serde::Serialize;
use serde_json::Value;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

fn bucket_for_position(
    slug: &str,
    title: &str,
    slug_to_bucket: &HashMap<String, String>,
) -> String {
    let k = normalize_slug_key(slug);
    let base = slug_to_bucket
        .get(&k)
        .cloned()
        .unwrap_or_else(|| classify_slug(slug).to_string());
    if base != "unknown" {
        return base;
    }
    fallback_bucket_from_title(title)
        .map(String::from)
        .unwrap_or(base)
}

fn num_field(o: &serde_json::Map<String, Value>, a: &str, b: &str) -> Option<f64> {
    o.get(a)
        .or_else(|| o.get(b))
        .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
}

fn str_field<'a>(o: &'a serde_json::Map<String, Value>, a: &str, b: &str) -> Option<&'a str> {
    o.get(a)
        .or_else(|| o.get(b))
        .and_then(|v| v.as_str())
}

fn total_investment_usd(o: &serde_json::Map<String, Value>) -> f64 {
    let initial = num_field(o, "initialValue", "initial_value");
    let total_bought = num_field(o, "totalBought", "total_bought");
    let size = num_field(o, "size", "size").unwrap_or(0.0);
    let avg = num_field(o, "avgPrice", "avg_price").unwrap_or(0.0);
    if let Some(iv) = initial {
        if iv > 0.0 {
            return iv;
        }
    }
    if let Some(tb) = total_bought {
        if tb > 0.0 && avg > 0.0 {
            return tb * avg;
        }
    }
    if size > 1e-12 && avg > 0.0 {
        return size * avg;
    }
    0.0
}

/// 与前端 `rawPositionToDisplayRow` 已结束最终价值一致：currentValue 否则 cost + realizedPnl。
fn effective_value_usd(o: &serde_json::Map<String, Value>, is_open: bool) -> f64 {
    let cv = num_field(o, "currentValue", "current_value").unwrap_or(0.0);
    if cv > 0.0 {
        return cv;
    }
    if !is_open {
        let cost = total_investment_usd(o);
        let pnl = num_field(o, "realizedPnl", "realized_pnl").unwrap_or(0.0);
        if cost > 0.0 {
            return cost + pnl;
        }
    }
    let size = num_field(o, "size", "size").unwrap_or(0.0);
    let avg = num_field(o, "avgPrice", "avg_price").unwrap_or(0.0);
    (size * avg).max(0.0)
}

fn avg_price(o: &serde_json::Map<String, Value>) -> f64 {
    num_field(o, "avgPrice", "avg_price").unwrap_or(0.0)
}

fn slug_of(o: &serde_json::Map<String, Value>) -> &str {
    str_field(o, "slug", "slug").unwrap_or("")
}

fn title_of(o: &serde_json::Map<String, Value>) -> String {
    let t = str_field(o, "title", "title")
        .or_else(|| str_field(o, "question", "question"))
        .unwrap_or("")
        .trim();
    if !t.is_empty() {
        return t.to_string();
    }
    let s = slug_of(o);
    if s.is_empty() {
        "—".to_string()
    } else {
        s.to_string()
    }
}

fn bucket_key(price: f64) -> &'static str {
    if price < 0.1 {
        "lt_0_1"
    } else if price < 0.3 {
        "0_1_to_0_3"
    } else if price < 0.5 {
        "0_3_to_0_5"
    } else if price < 0.7 {
        "0_5_to_0_7"
    } else if price < 0.9 {
        "0_7_to_0_9"
    } else {
        "gt_0_9"
    }
}

#[derive(Serialize)]
pub struct MarketContrib {
    pub slug: String,
    pub title: String,
    pub notional_usd: f64,
}

#[derive(Serialize)]
pub struct MarketDistRow {
    pub market_type: String,
    pub position_count: u32,
    pub notional_usd: f64,
    /// 该分类下按 slug 汇总的盘口（hover 展示）；最多 50 条，按金额降序。
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub markets: Vec<MarketContrib>,
}

#[derive(Default)]
struct BucketAgg {
    position_count: u32,
    notional_usd: f64,
    by_slug: HashMap<String, SlugAgg>,
}

struct SlugAgg {
    title: String,
    notional_usd: f64,
}

impl BucketAgg {
    fn add(&mut self, slug: &str, title: &str, ev: f64) {
        self.position_count += 1;
        self.notional_usd += ev;
        let key = if slug.is_empty() {
            "__no_slug__".to_string()
        } else {
            slug.to_string()
        };
        match self.by_slug.entry(key) {
            Entry::Occupied(mut e) => {
                let a = e.get_mut();
                a.notional_usd += ev;
                if (a.title.is_empty() || a.title == "—") && !title.is_empty() && title != "—" {
                    a.title = title.to_string();
                }
            }
            Entry::Vacant(v) => {
                v.insert(SlugAgg {
                    title: if title.is_empty() {
                        "—".to_string()
                    } else {
                        title.to_string()
                    },
                    notional_usd: ev,
                });
            }
        }
    }

    fn into_dist_row(self, market_type: String) -> MarketDistRow {
        const MAX_MARKETS: usize = 50;
        let mut markets: Vec<MarketContrib> = self
            .by_slug
            .into_iter()
            .map(|(slug, a)| MarketContrib {
                slug: if slug == "__no_slug__" { String::new() } else { slug },
                title: a.title,
                notional_usd: a.notional_usd,
            })
            .collect();
        markets.sort_by(|a, b| {
            b.notional_usd
                .partial_cmp(&a.notional_usd)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        if markets.len() > MAX_MARKETS {
            markets.truncate(MAX_MARKETS);
        }
        MarketDistRow {
            market_type,
            position_count: self.position_count,
            notional_usd: self.notional_usd,
            markets,
        }
    }
}

#[derive(Serialize)]
pub struct WinRateTypeRow {
    pub market_type: String,
    pub closed_with_pnl: u32,
    pub wins: u32,
    pub win_rate_pct: f64,
}

#[derive(Serialize)]
pub struct PriceBucketRow {
    pub label: String,
    pub range_low: f64,
    pub range_high: f64,
    pub count: u32,
}

#[derive(Serialize)]
pub struct OutcomeBias {
    pub yes_pct: f64,
    pub no_pct: f64,
    pub yes_count: u32,
    pub no_count: u32,
    pub neutral_count: u32,
}

#[derive(Serialize)]
pub struct PositionAnalytics {
    pub schema: &'static str,
    pub source: &'static str,
    pub proxy: String,
    pub closed_positions: u32,
    pub open_positions: u32,
    /// 已平仓中有有效 `realizedPnl` 的条数
    pub closed_with_realized_pnl: u32,
    /// 已平仓胜率（盈利市场数 / 有盈亏数据的已平仓数），0–100
    pub closed_win_rate_pct: f64,
    pub market_distribution: Vec<MarketDistRow>,
    pub win_rate_by_market_type: Vec<WinRateTypeRow>,
    pub price_buckets_avg_price: Vec<PriceBucketRow>,
    pub outcome_position_bias: OutcomeBias,
    pub notes: Vec<&'static str>,
}

pub fn compute_position_analytics(
    proxy: &str,
    open: &[Value],
    closed: &[Value],
    slug_to_bucket: &HashMap<String, String>,
) -> PositionAnalytics {
    let notes = vec![
        "Aggregates from cached positions only (no /trades).",
        "closed_win_rate_pct: count(realizedPnl>0) / count(valid realizedPnl on closed).",
        "market_distribution: sum(notional) by bucket; each row includes `markets` (slug,title,notional) for hover drill-down.",
        "price_buckets_avg_price: histogram of avgPrice per position (open+closed), not per-fill.",
        "outcome_position_bias: share of position rows by outcome text (yes/no), not trade fills.",
    ];

    let mut closed_wins = 0_u32;
    let mut closed_with_pnl = 0_u32;
    let mut win_by_type: HashMap<String, (u32, u32)> = HashMap::new();
    let mut dist: HashMap<String, BucketAgg> = HashMap::new();
    let mut price_buckets: HashMap<&'static str, u32> = HashMap::from([
        ("lt_0_1", 0),
        ("0_1_to_0_3", 0),
        ("0_3_to_0_5", 0),
        ("0_5_to_0_7", 0),
        ("0_7_to_0_9", 0),
        ("gt_0_9", 0),
    ]);
    let mut yes_c = 0_u32;
    let mut no_c = 0_u32;
    let mut neu_c = 0_u32;

    for v in closed {
        let Some(o) = v.as_object() else { continue };
        if let Some(pnl) = num_field(o, "realizedPnl", "realized_pnl") {
            closed_with_pnl += 1;
            if pnl > 0.0 {
                closed_wins += 1;
            }
            let slug = slug_of(o);
            let tit = title_of(o);
            let mt = bucket_for_position(slug, &tit, slug_to_bucket);
            let e = win_by_type.entry(mt).or_insert((0, 0));
            e.1 += 1;
            if pnl > 0.0 {
                e.0 += 1;
            }
        }
    }

    let closed_win_rate_pct = if closed_with_pnl > 0 {
        (closed_wins as f64 / closed_with_pnl as f64) * 100.0
    } else {
        0.0
    };

    for v in open {
        let Some(o) = v.as_object() else { continue };
        let slug = slug_of(o);
        let tit = title_of(o);
        let mt = bucket_for_position(slug, &tit, slug_to_bucket);
        let ev = effective_value_usd(o, true);
        dist.entry(mt).or_default().add(slug, &tit, ev);
        let ap = avg_price(o);
        if ap > 0.0 {
            *price_buckets.entry(bucket_key(ap)).or_insert(0) += 1;
        }
        count_outcome(o, &mut yes_c, &mut no_c, &mut neu_c);
    }

    for v in closed {
        let Some(o) = v.as_object() else { continue };
        let slug = slug_of(o);
        let tit = title_of(o);
        let mt = bucket_for_position(slug, &tit, slug_to_bucket);
        let ev = effective_value_usd(o, false);
        dist.entry(mt).or_default().add(slug, &tit, ev);
        let ap = avg_price(o);
        if ap > 0.0 {
            *price_buckets.entry(bucket_key(ap)).or_insert(0) += 1;
        }
        count_outcome(o, &mut yes_c, &mut no_c, &mut neu_c);
    }

    let mut market_distribution: Vec<MarketDistRow> = dist
        .into_iter()
        .map(|(market_type, agg)| agg.into_dist_row(market_type))
        .collect();
    market_distribution.sort_by(|a, b| {
        b.notional_usd
            .partial_cmp(&a.notional_usd)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut win_rate_by_market_type: Vec<WinRateTypeRow> = win_by_type
        .into_iter()
        .map(|(market_type, (wins, total))| WinRateTypeRow {
            market_type,
            closed_with_pnl: total,
            wins,
            win_rate_pct: if total > 0 {
                (wins as f64 / total as f64) * 100.0
            } else {
                0.0
            },
        })
        .collect();
    win_rate_by_market_type.sort_by(|a, b| {
        b.win_rate_pct
            .partial_cmp(&a.win_rate_pct)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let labels: [(&str, &str, f64, f64); 6] = [
        ("<0.1", "lt_0_1", 0.0, 0.1),
        ("0.1–0.3", "0_1_to_0_3", 0.1, 0.3),
        ("0.3–0.5", "0_3_to_0_5", 0.3, 0.5),
        ("0.5–0.7", "0_5_to_0_7", 0.5, 0.7),
        ("0.7–0.9", "0_7_to_0_9", 0.7, 0.9),
        (">0.9", "gt_0_9", 0.9, 1.0),
    ];
    let price_buckets_avg_price: Vec<PriceBucketRow> = labels
        .iter()
        .map(|(label, key, lo, hi)| PriceBucketRow {
            label: (*label).to_string(),
            range_low: *lo,
            range_high: *hi,
            count: *price_buckets.get(*key).unwrap_or(&0),
        })
        .collect();

    let total_side = yes_c + no_c;
    let (yes_pct, no_pct) = if total_side > 0 {
        (
            (yes_c as f64 / total_side as f64) * 100.0,
            (no_c as f64 / total_side as f64) * 100.0,
        )
    } else {
        (0.0, 0.0)
    };

    PositionAnalytics {
        schema: "forevex.position_analytics.v1",
        source: "positions_only",
        proxy: proxy.to_string(),
        closed_positions: closed.len() as u32,
        open_positions: open.len() as u32,
        closed_with_realized_pnl: closed_with_pnl,
        closed_win_rate_pct,
        market_distribution,
        win_rate_by_market_type,
        price_buckets_avg_price,
        outcome_position_bias: OutcomeBias {
            yes_pct,
            no_pct,
            yes_count: yes_c,
            no_count: no_c,
            neutral_count: neu_c,
        },
        notes,
    }
}

fn count_outcome(
    o: &serde_json::Map<String, Value>,
    yes_c: &mut u32,
    no_c: &mut u32,
    neu_c: &mut u32,
) {
    let Some(out) = str_field(o, "outcome", "outcome") else {
        *neu_c += 1;
        return;
    };
    let ol = out.to_lowercase();
    if ol.contains("yes") && !ol.contains("no") {
        *yes_c += 1;
    } else if ol.contains("no") {
        *no_c += 1;
    } else {
        *neu_c += 1;
    }
}
