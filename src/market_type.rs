//! Slug → 市场类型（与 polymarket-account-analyzer `MarketTypeConfig::default` 的 **slug 回退规则** 对齐，无 Gamma 时仅用文本规则）。
//!
//! Gamma `distribution_bucket` 已跑 **category + `/tags` + `/sports` 标签树**；仍落入 `unknown` 时多为：
//! - 超出 `gamma_max_slug_enrich` 未打 Gamma；
//! - API 无 category/tags，且 slug 不含 `sports` 等子串；
//! - 缓存里旧 `unknown`。
//! 此处补充 **联赛前缀 / 常见联赛子串** + **标题启发式**（与 `gamma_taxonomy::is_sports_cluster` 口径一致）。

/// 与 `polymarket-account-analyzer` 默认 `market_type.rules` 一致：首条匹配优先。
pub fn classify_slug(slug: &str) -> &'static str {
    let slug_lc = slug.to_lowercase();
    const PREFIX: &[(&str, &str)] = &[
        ("nba-", "sports"),
        ("nfl-", "sports"),
        ("mlb-", "sports"),
        ("nhl-", "sports"),
        ("ufc-", "sports"),
        ("wnba-", "sports"),
        ("cbb-", "sports"),
        ("cfb-", "sports"),
        ("mls-", "sports"),
        ("epl-", "sports"),
        ("ucl-", "sports"),
        ("uel-", "sports"),
        ("f1-", "sports"),
        ("esports-", "sports"),
    ];
    for (pfx, ty) in PREFIX {
        if slug_lc.starts_with(pfx) {
            return ty;
        }
    }
    const RULES: &[(&str, &str)] = &[
        ("5-min", "5-min"),
        ("1h", "1h"),
        ("daily", "daily"),
        ("politics", "politics"),
        ("sports", "sports"),
        ("crypto", "crypto"),
        ("ufc", "sports"),
        ("nba", "sports"),
        ("nfl", "sports"),
        ("mlb", "sports"),
        ("nhl", "sports"),
        ("wnba", "sports"),
        ("mls", "sports"),
        ("mma", "sports"),
        ("epl-", "sports"),
        ("soccer", "sports"),
        ("tennis", "sports"),
        ("golf", "sports"),
        ("boxing", "sports"),
        ("cricket", "sports"),
        ("olympics", "sports"),
        ("f1", "sports"),
        ("nascar", "sports"),
        ("esports", "sports"),
    ];
    for (needle, ty) in RULES {
        if slug_lc.contains(needle) {
            return ty;
        }
    }
    "unknown"
}

/// 当 slug / Gamma 仍归 `unknown` 时，用盘口标题做最后一道 **sports** 识别（如 NBA/UFC「A vs B」）。
pub fn fallback_bucket_from_title(title: &str) -> Option<&'static str> {
    let t = title.to_lowercase();
    const KW: &[&str] = &[
        "ufc",
        "nba",
        "nfl",
        "nhl",
        "mlb",
        "wnba",
        "mls",
        "mma",
        "f1",
        "nascar",
        "premier league",
        "champions league",
        "super bowl",
        "march madness",
        "moneyline",
        "spread:",
        "over/under",
        "o/u",
        "fight night",
        "pga",
        "pga tour",
    ];
    if KW.iter().any(|k| t.contains(k)) {
        return Some("sports");
    }
    // 典型体育对阵，且排除明显政治用语
    if t.contains(" vs ") || t.contains(" vs.") {
        const POL: &[&str] = &[
            "election",
            "president",
            "senate",
            "trump",
            "biden",
            "vote",
            "poll",
            "debate",
            "governor",
            "congress",
            "parliament",
            "primary",
        ];
        if !POL.iter().any(|p| t.contains(p)) && t.len() >= 10 {
            return Some("sports");
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sports_slug() {
        assert_eq!(classify_slug("nba-sports-lakers-2026"), "sports");
    }

    #[test]
    fn unknown() {
        assert_eq!(classify_slug("random-market-slug"), "unknown");
    }

    #[test]
    fn nba_prefix() {
        assert_eq!(classify_slug("nba-lal-mem-2025-01-15"), "sports");
    }

    #[test]
    fn title_fallback_ufc() {
        assert_eq!(
            fallback_bucket_from_title("UFC Fight Night: Sean Strickland vs. Anthony Hernandez"),
            Some("sports")
        );
    }

    #[test]
    fn title_fallback_vs_no_politics() {
        assert_eq!(
            fallback_bucket_from_title("Spurs vs. Pistons"),
            Some("sports")
        );
    }
}
