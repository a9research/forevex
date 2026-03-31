//! 下载 PMA 预置包 `data.tar.zst`，解压到 `PIPELINE_PMA_DATA_DIR`。
//! 若配置了对象存储（`PIPELINE_OSS_BUCKET` / `PIPELINE_S3_BUCKET`），解压后 **自动上传并删除本地 `polymarket/`**（与 `ingest-pma` 键一致）。

use crate::config::Config;
use crate::pma::{resolve_polymarket_data_root, sync_local_polymarket_to_oss_and_delete};
use tokio::io::AsyncWriteExt;

pub struct BootstrapOptions {
    pub url: Option<String>,
    pub force: bool,
    /// 即使已配置 bucket 也 **不** 上传到 OSS、不删本地（调试用）
    pub no_upload: bool,
}

pub async fn run(cfg: &Config, opts: BootstrapOptions) -> anyhow::Result<()> {
    let out = &cfg.pma_data_dir;
    tokio::fs::create_dir_all(out).await?;

    let url = opts
        .url
        .clone()
        .unwrap_or_else(|| cfg.bootstrap_download_url.clone());
    let archive_path = out.join("data.tar.zst");
    let sentinel = out.join(".pma_download_complete");

    let should_upload = cfg.s3_bucket.is_some() && !opts.no_upload;

    if sentinel.exists() && !opts.force {
        tracing::info!(
            path = %sentinel.display(),
            "dataset marker exists; skip download (use --force to re-download)"
        );
        if should_upload {
            let n = sync_local_polymarket_to_oss_and_delete(cfg).await?;
            tracing::info!(
                uploaded = n,
                "bootstrap: synced local parquet to OSS and removed disk copy"
            );
        }
        return Ok(());
    }

    tracing::info!(%url, dest = %archive_path.display(), "downloading PMA archive (streaming to disk)");

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(86400))
        .build()?;
    let mut resp = client.get(&url).send().await?.error_for_status()?;
    let mut file = tokio::fs::File::create(&archive_path).await?;
    while let Some(chunk) = resp.chunk().await? {
        file.write_all(&chunk).await?;
    }
    file.flush().await?;

    tracing::info!(path = %archive_path.display(), "extracting zstd + tar");
    let out_clone = out.clone();
    let arch_clone = archive_path.clone();
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let f = std::fs::File::open(&arch_clone)?;
        let dec = zstd::Decoder::new(f)?;
        let mut archive = tar::Archive::new(dec);
        archive.unpack(&out_clone)?;
        Ok(())
    })
    .await??;

    if archive_path.exists() {
        tokio::fs::remove_file(&archive_path).await.ok();
    }

    tokio::fs::write(&sentinel, b"ok\n").await?;
    tracing::info!(dir = %out.display(), "PMA dataset extracted");

    if should_upload {
        // 确保能解析到 polymarket 根再上传（与 ingest 一致）
        if resolve_polymarket_data_root(out).is_ok() {
            let n = sync_local_polymarket_to_oss_and_delete(cfg).await?;
            tracing::info!(
                uploaded = n,
                "bootstrap: uploaded parquet to object store and removed local polymarket/"
            );
        } else {
            tracing::warn!(
                dir = %out.display(),
                "bootstrap: extracted tree has no polymarket/trades — skip OSS upload"
            );
        }
    }

    Ok(())
}
