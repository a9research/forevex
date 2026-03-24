use crate::store::Store;
use crate::upstream::Upstream;
use anyhow::Context;
use serde_json::{json, Value};

fn normalize_proxy(s: &str) -> Option<String> {
    let w = s.trim().to_lowercase();
    if w.len() == 42 && w.starts_with("0x") && w[2..].chars().all(|c| c.is_ascii_hexdigit()) {
        Some(w)
    } else {
        None
    }
}

/// Accept `0x…` or `@handle` / `handle` (Gamma username).
pub fn parse_user_input(input: &str) -> anyhow::Result<UserInput> {
    let t = input.trim();
    let t = t.strip_prefix('@').unwrap_or(t).trim();
    if let Some(p) = normalize_proxy(t) {
        return Ok(UserInput::Proxy(p));
    }
    if t.is_empty() {
        anyhow::bail!("empty user input");
    }
    Ok(UserInput::Username(t.to_string()))
}

pub enum UserInput {
    Proxy(String),
    Username(String),
}

pub async fn resolve_proxy(up: &Upstream, input: UserInput) -> anyhow::Result<(String, Option<String>)> {
    match input {
        UserInput::Proxy(p) => Ok((p, None)),
        UserInput::Username(u) => {
            let j = up.gamma_public_profile(None, Some(&u)).await?;
            let proxy = j
                .get("proxyWallet")
                .or_else(|| j.get("proxy_wallet"))
                .and_then(|v| v.as_str())
                .map(|s| s.trim().to_lowercase())
                .filter(|s| normalize_proxy(s).is_some())
                .context("gamma profile: missing proxyWallet")?;
            Ok((proxy, Some(u)))
        }
    }
}

fn position_key(row: &Value, idx: usize) -> String {
    let o = row.as_object();
    let asset = o
        .and_then(|m| m.get("asset"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if !asset.is_empty() {
        return format!("asset:{asset}");
    }
    let cid = o
        .and_then(|m| m.get("conditionId").or(m.get("condition_id")))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let out = o
        .and_then(|m| m.get("outcome"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let slug = o
        .and_then(|m| m.get("slug"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if !cid.is_empty() {
        format!("cid:{cid}:out:{out}:slug:{slug}")
    } else {
        format!("idx:{idx}:{slug}")
    }
}

fn event_ts_sec(v: &Value) -> Option<i64> {
    let t = v.get("timestamp")?;
    let n = if let Some(i) = t.as_i64() {
        i
    } else if let Some(f) = t.as_f64() {
        f as i64
    } else {
        return None;
    };
    Some(if n > 1_000_000_000_000 { n / 1000 } else { n })
}

fn max_event_ts(events: &[Value]) -> Option<i64> {
    events.iter().filter_map(event_ts_sec).max()
}

async fn paginate_positions_open(up: &Upstream, user: &str) -> anyhow::Result<Vec<Value>> {
    let limit = 500u32;
    let mut offset = 0u32;
    let mut out = Vec::new();
    loop {
        let page: Vec<Value> = up.positions_page(user, limit, offset).await?;
        if page.is_empty() {
            break;
        }
        let n = page.len() as u32;
        out.extend(page);
        if n < limit {
            break;
        }
        offset = offset.saturating_add(limit);
    }
    Ok(out)
}

async fn paginate_positions_closed(up: &Upstream, user: &str) -> anyhow::Result<Vec<Value>> {
    let limit = 500u32;
    let mut offset = 0u32;
    let mut out = Vec::new();
    loop {
        let page: Vec<Value> = up.closed_positions_page(user, limit, offset).await?;
        if page.is_empty() {
            break;
        }
        let n = page.len() as u32;
        out.extend(page);
        if n < limit {
            break;
        }
        offset = offset.saturating_add(limit);
    }
    Ok(out)
}

pub async fn sync_user(store: &Store, up: &Upstream, input: &str) -> anyhow::Result<Value> {
    let parsed = parse_user_input(input)?;
    let (proxy, username_hint) = resolve_proxy(up, parsed).await?;

    let gamma = up
        .gamma_public_profile(Some(&proxy), None)
        .await
        .unwrap_or_else(|e| json!({ "error": e.to_string() }));

    let value = up.data_value(&proxy).await.unwrap_or_else(|e| json!({ "error": e.to_string() }));
    let traded = up.data_traded(&proxy).await.unwrap_or_else(|e| json!({ "error": e.to_string() }));
    let stats = up.user_stats(&proxy).await.unwrap_or_else(|e| json!({ "error": e.to_string() }));
    let pnl = up
        .user_pnl_default_series(&proxy)
        .await
        .unwrap_or_else(|e| json!({ "error": e.to_string() }));

    store
        .upsert_wallet_snapshot(
            &proxy,
            username_hint.as_deref(),
            Some(gamma.clone()),
            Some(value.clone()),
            Some(traded.clone()),
            Some(stats.clone()),
            Some(pnl.clone()),
        )
        .await?;

    Ok(json!({
        "proxy": proxy,
        "resolvedUsername": username_hint,
        "gammaProfile": gamma,
        "value": value,
        "traded": traded,
        "userStats": stats,
        "userPnl": pnl,
    }))
}

pub async fn sync_positions(store: &Store, up: &Upstream, proxy: &str) -> anyhow::Result<Value> {
    let p = normalize_proxy(proxy).context("invalid proxy address")?;
    store.ensure_wallet_row(&p).await?;

    let open = paginate_positions_open(up, &p).await?;
    let closed = paginate_positions_closed(up, &p).await?;

    let open_rows: Vec<(String, Value)> = open
        .iter()
        .enumerate()
        .map(|(i, r)| (position_key(r, i), r.clone()))
        .collect();
    let closed_rows: Vec<(String, Value)> = closed
        .iter()
        .enumerate()
        .map(|(i, r)| (position_key(r, i), r.clone()))
        .collect();

    store.replace_positions(&p, "open", &open_rows).await?;
    store.replace_positions(&p, "closed", &closed_rows).await?;
    store.set_positions_synced_at(&p).await?;

    Ok(json!({
        "proxy": p,
        "openCount": open_rows.len(),
        "closedCount": closed_rows.len(),
    }))
}

pub async fn sync_activity(
    store: &Store,
    up: &Upstream,
    proxy: &str,
    market_condition_id: &str,
) -> anyhow::Result<Value> {
    let p = normalize_proxy(proxy).context("invalid proxy address")?;
    store.ensure_wallet_row(&p).await?;
    let market = market_condition_id.trim();
    if market.is_empty() {
        anyhow::bail!("market (condition id) required");
    }

    let limit = 500u32;
    let mut offset = 0u32;
    let mut all: Vec<Value> = Vec::new();
    loop {
        let page: Vec<Value> = up.activity_page(&p, Some(market), limit, offset).await?;
        if page.is_empty() {
            break;
        }
        let n = page.len() as u32;
        all.extend(page);
        if n < limit {
            break;
        }
        offset = offset.saturating_add(limit);
        if offset > 50_000 {
            tracing::warn!("activity pagination stopped at offset cap");
            break;
        }
    }

    let max_ts = max_event_ts(&all);
    let events = Value::Array(all);
    store.upsert_activity(&p, market, events.clone(), max_ts).await?;

    Ok(json!({
        "proxy": p,
        "market": market,
        "count": events.as_array().map(|a| a.len()).unwrap_or(0),
        "maxEventTs": max_ts,
    }))
}
