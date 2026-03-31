//! Topic rollup aligned with forevex `GammaTaxonomy::rollup_to_polymarket_topic` (simplified).

pub fn normalize_key(s: &str) -> String {
    s.trim()
        .to_lowercase()
        .chars()
        .map(|c| {
            if c.is_whitespace() || c == '_' {
                '-'
            } else {
                c
            }
        })
        .collect()
}

/// Map Gamma `category` string (or tag-like label) to a single top-level topic bucket.
pub fn rollup_category_to_topic(category: Option<&str>) -> String {
    let Some(c) = category.map(str::trim).filter(|s| !s.is_empty()) else {
        return "other".to_string();
    };
    let n = normalize_key(c);
    rollup_normalized(&n).unwrap_or_else(|| "other".to_string())
}

fn rollup_normalized(n: &str) -> Option<String> {
    if is_sports_cluster(n) {
        return Some("sports".to_string());
    }
    if is_iran_cluster(n) {
        return Some("iran".to_string());
    }
    if is_elections_topic(n) {
        return Some("elections".to_string());
    }
    if is_mentions_topic(n) {
        return Some("mentions".to_string());
    }
    if is_politics_figure_slug(n) {
        return Some("politics".to_string());
    }
    if is_geopolitics_cluster(n) {
        return Some("geopolitics".to_string());
    }
    if is_politics_cluster(n) {
        return Some("politics".to_string());
    }
    if is_crypto_cluster(n) {
        return Some("crypto".to_string());
    }
    if is_ai_cluster(n) {
        return Some("ai".to_string());
    }
    if is_tech_cluster(n) {
        return Some("tech".to_string());
    }
    if is_finance_cluster(n) {
        return Some("finance".to_string());
    }
    if is_economy_cluster(n) {
        return Some("economy".to_string());
    }
    if is_pop_culture_cluster(n) {
        return Some("pop-culture".to_string());
    }
    if is_culture_cluster(n) {
        return Some("culture".to_string());
    }
    if is_weather_cluster(n) {
        return Some("weather".to_string());
    }
    None
}

fn is_sports_cluster(n: &str) -> bool {
    n == "sports"
        || n == "sport"
        || n == "soccer"
        || n.contains("nba")
        || n.contains("nfl")
        || n.contains("ncaa")
}

fn is_iran_cluster(n: &str) -> bool {
    n == "iran" || n.contains("iranian") || n.contains("khamenei") || n.contains("tehran")
}

fn is_elections_topic(n: &str) -> bool {
    matches!(
        n,
        "elections" | "election" | "us-election" | "midterms" | "primaries"
    )
}

fn is_mentions_topic(n: &str) -> bool {
    n == "mentions" || n == "mention"
}

fn is_politics_figure_slug(n: &str) -> bool {
    matches!(
        n,
        "trump" | "biden" | "harris" | "obama" | "macron" | "sunak" | "merkel"
    )
}

fn is_geopolitics_cluster(n: &str) -> bool {
    n.contains("geopolitic")
        || n.contains("ukraine")
        || n.contains("russia")
        || n.contains("china")
        || n.contains("israel")
        || n.contains("middle-east")
        || n.contains("nato")
}

fn is_politics_cluster(n: &str) -> bool {
    if n.contains("geopolitic") {
        return false;
    }
    n.contains("politic")
        || n.contains("election")
        || n.contains("congress")
        || n.contains("senate")
        || n.contains("white-house")
        || n.contains("us-current-affairs")
        || n.contains("current-affairs")
}

fn is_crypto_cluster(n: &str) -> bool {
    n.contains("crypto")
        || n.contains("bitcoin")
        || n.contains("ethereum")
        || n.contains("defi")
        || n.contains("nft")
}

fn is_ai_cluster(n: &str) -> bool {
    n == "ai" || n.contains("artificial-intelligence") || n.contains("openai") || n.contains("llm")
}

fn is_tech_cluster(n: &str) -> bool {
    if is_ai_cluster(n) {
        return false;
    }
    n == "tech" || n.contains("technology") || n.contains("big-tech") || n == "technology"
}

fn is_finance_cluster(n: &str) -> bool {
    n.contains("finance")
        || n.contains("fed")
        || n.contains("stocks")
        || n.contains("commodities")
        || n.contains("forex")
}

fn is_economy_cluster(n: &str) -> bool {
    n.contains("economy") || n.contains("inflation") || n.contains("gdp") || n.contains("recession")
}

fn is_pop_culture_cluster(n: &str) -> bool {
    n.contains("pop-culture")
        || n.contains("movies")
        || n.contains("music")
        || n.contains("celebrity")
        || n.contains("entertainment")
}

fn is_culture_cluster(n: &str) -> bool {
    if is_pop_culture_cluster(n) {
        return false;
    }
    n == "culture" || n.contains("literature")
}

fn is_weather_cluster(n: &str) -> bool {
    n.contains("weather") || n.contains("hurricane")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn us_current_affairs_to_politics() {
        assert_eq!(
            rollup_category_to_topic(Some("US-current-affairs")),
            "politics"
        );
    }

    #[test]
    fn tech_to_tech() {
        assert_eq!(rollup_category_to_topic(Some("Tech")), "tech");
    }
}
