//! Slug → 市场类型（与 polymarket-account-analyzer `MarketTypeConfig::default` 的 **slug 回退规则** 对齐，无 Gamma 时仅用文本规则）。

/// 与 `polymarket-account-analyzer` 默认 `market_type.rules` 一致：首条匹配优先。
pub fn classify_slug(slug: &str) -> &'static str {
    let slug_lc = slug.to_lowercase();
    const RULES: &[(&str, &str)] = &[
        ("5-min", "5-min"),
        ("1h", "1h"),
        ("daily", "daily"),
        ("politics", "politics"),
        ("sports", "sports"),
        ("crypto", "crypto"),
    ];
    for (needle, ty) in RULES {
        if slug_lc.contains(needle) {
            return ty;
        }
    }
    "unknown"
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
}
