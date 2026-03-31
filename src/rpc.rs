use crate::config::Config;
use anyhow::Context;
use reqwest::Client;
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Very small JSON-RPC client with endpoint rotation + backoff.
#[derive(Clone)]
pub struct RotatingRpc {
    client: Client,
    urls: Arc<Vec<String>>,
    idx: Arc<AtomicUsize>,
    max_retries: u32,
    backoff_ms: u64,
}

impl RotatingRpc {
    pub fn from_cfg(cfg: &Config) -> anyhow::Result<Self> {
        if cfg.polygon_rpc_urls.is_empty() {
            anyhow::bail!("Polygon RPC 未配置：设置 PIPELINE_POLYGON_RPC_URLS（逗号分隔多个 URL）");
        }
        let client = Client::builder()
            .timeout(cfg.http_timeout)
            .user_agent("polymarket-pipeline/rotating-rpc")
            .build()?;
        Ok(Self {
            client,
            urls: Arc::new(cfg.polygon_rpc_urls.clone()),
            idx: Arc::new(AtomicUsize::new(0)),
            max_retries: cfg.polygon_rpc_max_retries,
            backoff_ms: cfg.polygon_rpc_backoff_ms,
        })
    }

    fn next_url(&self) -> String {
        let i = self.idx.fetch_add(1, Ordering::Relaxed);
        self.urls[i % self.urls.len()].clone()
    }

    pub async fn call(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        let mut last_err: Option<anyhow::Error> = None;
        let attempts = self.max_retries.max(1);
        for attempt in 0..attempts {
            let url = self.next_url();
            let req = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            });
            let res = self.client.post(&url).json(&req).send().await;
            match res {
                Ok(resp) => {
                    let status = resp.status();
                    let v: Value = resp
                        .json()
                        .await
                        .with_context(|| format!("json-rpc: invalid json from {url}"))?;
                    if !status.is_success() {
                        last_err = Some(anyhow::anyhow!(
                            "json-rpc http error: status={status} url={url} body={v}"
                        ));
                    } else if let Some(e) = v.get("error") {
                        last_err = Some(anyhow::anyhow!("json-rpc error from {url}: {e}"));
                    } else if let Some(r) = v.get("result") {
                        return Ok(r.clone());
                    } else {
                        last_err =
                            Some(anyhow::anyhow!("json-rpc: missing result from {url}: {v}"));
                    }
                }
                Err(e) => {
                    last_err = Some(anyhow::anyhow!("json-rpc request failed url={url}: {e}"));
                }
            }

            if attempt + 1 < attempts && self.backoff_ms > 0 {
                let pow2 = 1u64.checked_shl(attempt.min(6)).unwrap_or(64);
                let ms = self.backoff_ms.saturating_mul(pow2);
                tokio::time::sleep(Duration::from_millis(ms)).await;
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("json-rpc failed")))
    }
}
