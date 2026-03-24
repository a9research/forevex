use crate::config::Config;
use anyhow::Context;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::time::Duration;

#[derive(Clone)]
pub struct Upstream {
    http: Client,
    cfg: Config,
}

impl Upstream {
    pub fn new(cfg: Config) -> anyhow::Result<Self> {
        let http = Client::builder()
            .user_agent("forevex/0.1")
            .timeout(cfg.http_timeout)
            .connect_timeout(Duration::from_secs(15))
            .no_proxy()
            .build()?;
        Ok(Self { http, cfg })
    }

    async fn sleep_rate(&self) {
        if self.cfg.rate_limit_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.cfg.rate_limit_ms)).await;
        }
    }

    /// `GET /public-profile?address=` or `?username=` (Gamma).
    pub async fn gamma_public_profile(
        &self,
        address: Option<&str>,
        username: Option<&str>,
    ) -> anyhow::Result<Value> {
        let url = format!("{}/public-profile", self.cfg.gamma_origin);
        let mut req = self.http.get(&url);
        if let Some(a) = address.filter(|s| !s.is_empty()) {
            req = req.query(&[("address", a)]);
        } else if let Some(u) = username.filter(|s| !s.is_empty()) {
            req = req.query(&[("username", u)]);
        } else {
            anyhow::bail!("gamma public-profile: need address or username");
        }
        let resp = req.send().await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        self.sleep_rate().await;
        if !status.is_success() {
            anyhow::bail!("gamma public-profile HTTP {status}: {body}");
        }
        serde_json::from_str(&body).context("gamma public-profile JSON")
    }

    /// Best-effort JSON GET; returns Err on non-2xx.
    pub async fn get_json_optional(&self, url: &str) -> anyhow::Result<Value> {
        let resp = self.http.get(url).send().await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        self.sleep_rate().await;
        if !status.is_success() {
            anyhow::bail!("GET {url} HTTP {status}: {body}");
        }
        serde_json::from_str(&body).with_context(|| format!("parse JSON {url}"))
    }

    pub async fn data_value(&self, user: &str) -> anyhow::Result<Value> {
        let url = format!("{}/value?user={}", self.cfg.data_api_origin, user);
        self.get_json_optional(&url).await
    }

    pub async fn data_traded(&self, user: &str) -> anyhow::Result<Value> {
        let url = format!("{}/traded?user={}", self.cfg.data_api_origin, user);
        self.get_json_optional(&url).await
    }

    pub async fn user_stats(&self, proxy: &str) -> anyhow::Result<Value> {
        let url = format!(
            "{}?proxyAddress={}",
            self.cfg.user_stats_path,
            urlencoding::encode(proxy)
        );
        self.get_json_optional(&url).await
    }

    pub async fn user_pnl_default_series(&self, user_address: &str) -> anyhow::Result<Value> {
        let url = format!(
            "{}?user_address={}&interval=all&fidelity=12h",
            self.cfg.user_pnl_origin,
            urlencoding::encode(user_address)
        );
        self.get_json_optional(&url).await
    }

    pub async fn positions_page<T: DeserializeOwned>(
        &self,
        user: &str,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<T>> {
        let url = format!("{}/positions", self.cfg.data_api_origin);
        let resp = self
            .http
            .get(&url)
            .query(&[
                ("user", user),
                ("limit", &limit.to_string()),
                ("offset", &offset.to_string()),
            ])
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("data-api /positions HTTP {status}: {body}");
        }
        let v = resp.json::<Vec<T>>().await.unwrap_or_default();
        self.sleep_rate().await;
        Ok(v)
    }

    pub async fn closed_positions_page<T: DeserializeOwned>(
        &self,
        user: &str,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<T>> {
        let url = format!("{}/closed-positions", self.cfg.data_api_origin);
        let resp = self
            .http
            .get(&url)
            .query(&[
                ("user", user),
                ("limit", &limit.to_string()),
                ("offset", &offset.to_string()),
            ])
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("data-api /closed-positions HTTP {status}: {body}");
        }
        let v = resp.json::<Vec<T>>().await.unwrap_or_default();
        self.sleep_rate().await;
        Ok(v)
    }

    pub async fn activity_page<T: DeserializeOwned>(
        &self,
        user: &str,
        market: Option<&str>,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<T>> {
        let url = format!("{}/activity", self.cfg.data_api_origin);
        let mut req = self.http.get(&url).query(&[
            ("user", user),
            ("limit", &limit.to_string()),
            ("offset", &offset.to_string()),
        ]);
        if let Some(m) = market.filter(|s| !s.is_empty()) {
            req = req.query(&[("market", m)]);
        }
        let resp = req.send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("data-api /activity HTTP {status}: {body}");
        }
        let v = resp.json::<Vec<T>>().await.unwrap_or_default();
        self.sleep_rate().await;
        Ok(v)
    }
}
