use crate::{
    digest,
    egress::Proxy,
    jobs::{Failure, Lease},
    network::source_url,
    process::{Tool, ToolFailure, confined_file},
    storage::{Store, now},
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::File,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MediaMetadata {
    pub id: String,
    pub extractor: String,
    pub title: Option<String>,
    pub advisory_duration_seconds: Option<f64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MediaCandidate {
    pub schema_version: u32,
    pub source_revision: String,
    pub config_id: String,
    pub metadata: MediaMetadata,
    pub blob_digest: String,
    pub byte_length: u64,
    pub producer_digest: String,
    pub tools: Value,
    pub final_path_name: String,
    pub created_at: i64,
    pub pending_finalization: bool,
    pub order: usize,
}
pub struct MediaDownloader {
    pub store: Store,
    pub relay_executable: PathBuf,
}

fn metadata(value: &Value) -> Result<MediaMetadata> {
    let id = value["id"].as_str().ok_or(Failure::InvalidOutput)?;
    let extractor = value["extractor_key"]
        .as_str()
        .or_else(|| value["extractor"].as_str())
        .ok_or(Failure::InvalidOutput)?;
    ensure!(
        !id.is_empty() && id.len() <= 256 && !extractor.is_empty() && extractor.len() <= 256,
        Failure::InvalidOutput
    );
    let title = value["title"]
        .as_str()
        .map(|s| s.chars().filter(|c| !c.is_control()).take(512).collect());
    let duration = value["duration"]
        .as_f64()
        .filter(|d| d.is_finite() && *d > 0.0);
    Ok(MediaMetadata {
        id: id.into(),
        extractor: extractor.into(),
        title,
        advisory_duration_seconds: duration,
    })
}
fn executable_digest(path: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    std::io::copy(&mut File::open(path)?, &mut hash)?;
    Ok(format!("{:x}", hash.finalize()))
}
fn failure(error: crate::process::ToolError) -> anyhow::Error {
    match error.kind {
        ToolFailure::Cancelled => Failure::Cancelled,
        ToolFailure::Timeout => Failure::Timeout,
        ToolFailure::Missing => Failure::ToolMissing,
        ToolFailure::Nonzero => Failure::ToolExit,
        _ => Failure::InvalidOutput,
    }
    .into()
}

impl MediaDownloader {
    fn check(&self, lease: &Lease, playlist_limit: Option<usize>) -> Result<Instant> {
        self.store.heartbeat(lease)?;
        ensure!(
            lease.config.sources.media_enabled && self.relay_executable.is_absolute(),
            Failure::InvalidInput
        );
        if let Some(limit) = playlist_limit {
            ensure!(
                limit > 0 && limit <= lease.config.limits.graph_nodes,
                Failure::InvalidInput
            );
        }
        let seconds = lease.config.limits.download_seconds;
        ensure!(
            lease
                .job
                .deadline
                .is_some_and(|d| d - now() >= seconds as i64),
            Failure::Timeout
        );
        Ok(Instant::now() + Duration::from_secs(seconds))
    }

    async fn execute(
        &self,
        lease: &Lease,
        url: &str,
        workspace: &Path,
        options: Vec<OsString>,
        deadline: Instant,
        cancel: &CancellationToken,
    ) -> Result<String> {
        source_url(url)?;
        let budget = deadline.saturating_duration_since(Instant::now());
        ensure!(!budget.is_zero(), Failure::Timeout);
        let proxy = Proxy::start(lease.config.limits.work_bytes, budget)?;
        let mut args: Vec<OsString> = [
            "--unshare-all",
            "--die-with-parent",
            "--ro-bind",
            "/",
            "/",
            "--proc",
            "/proc",
            "--tmpfs",
            "/tmp",
            "--bind",
        ]
        .into_iter()
        .map(Into::into)
        .collect();
        args.extend([
            workspace.as_os_str().into(),
            workspace.as_os_str().into(),
            "--ro-bind".into(),
            proxy.directory().as_os_str().into(),
            proxy.directory().as_os_str().into(),
            "--chdir".into(),
            workspace.as_os_str().into(),
            "--".into(),
            self.relay_executable.as_os_str().into(),
            "media-egress-relay".into(),
            proxy.socket().into_os_string(),
            lease.config.tools.yt_dlp.as_os_str().into(),
            "--".into(),
        ]);
        let mut payload: Vec<OsString> = [
            "--ignore-config",
            "--no-plugin-dirs",
            "--no-js-runtimes",
            "--no-remote-components",
            "--no-cookies",
            "--no-cache-dir",
            "--no-progress",
            "--quiet",
            "--retries",
            "0",
            "--fragment-retries",
            "0",
            "--extractor-retries",
            "0",
            "--file-access-retries",
            "0",
            "--concurrent-fragments",
            "1",
            "--socket-timeout",
            "20",
            "--downloader",
            "native",
        ]
        .into_iter()
        .map(Into::into)
        .collect();
        payload.extend(options);
        payload.extend(["--".into(), url.into()]);
        args.extend(payload);
        let tool = Tool {
            executable: lease.config.tools.bubblewrap.clone(),
            args,
            environment: BTreeMap::new(),
            directory: workspace.into(),
            timeout: budget,
            log_bytes: lease.config.limits.log_bytes,
            stdout_bytes: Some(usize::try_from(lease.config.limits.input_bytes)?),
            work_bytes: lease.config.limits.work_bytes,
            secrets: vec![],
        };
        // Namespace setup and payload failures both fail closed; never retry on the host network.
        let output = tool.run(cancel).await;
        proxy.close().await?;
        let output = output.map_err(failure)?;
        self.store.heartbeat(lease)?;
        Ok(output.stdout)
    }

    async fn inspect(
        &self,
        lease: &Lease,
        url: &str,
        playlist_limit: Option<usize>,
        deadline: Instant,
        cancel: &CancellationToken,
    ) -> Result<Value> {
        let workspace = self.store.workspace(&lease.job.id)?;
        let mut options: Vec<OsString> = vec!["--dump-single-json".into(), "--simulate".into()];
        if let Some(limit) = playlist_limit {
            options.extend([
                "--yes-playlist".into(),
                "--playlist-end".into(),
                limit.to_string().into(),
            ]);
        } else {
            options.push("--no-playlist".into());
        }
        let output = self
            .execute(lease, url, workspace.path(), options, deadline, cancel)
            .await?;
        serde_json::from_str(&output).map_err(|_| Failure::InvalidOutput.into())
    }

    pub async fn probe(
        &self,
        lease: &Lease,
        url: &str,
        cancel: &CancellationToken,
    ) -> Result<MediaMetadata> {
        let deadline = self.check(lease, None)?;
        let value = self.inspect(lease, url, None, deadline, cancel).await?;
        ensure!(
            value.get("entries").is_none() && value["_type"] != "playlist",
            Failure::InvalidOutput
        );
        metadata(&value)
    }

    pub async fn download(
        &self,
        lease: &Lease,
        source_revision: &str,
        url: &str,
        playlist_limit: Option<usize>,
        cancel: &CancellationToken,
    ) -> Result<Vec<String>> {
        let deadline = self.check(lease, playlist_limit)?;
        self.store.revision(source_revision)?;
        let value = self
            .inspect(lease, url, playlist_limit, deadline, cancel)
            .await?;
        let items: Vec<(&Value, String)> = if let Some(entries) = value.get("entries") {
            let limit = playlist_limit.ok_or(Failure::InvalidInput)?;
            let entries = entries.as_array().ok_or(Failure::InvalidOutput)?;
            ensure!(
                !entries.is_empty() && entries.len() <= limit,
                Failure::InvalidOutput
            );
            entries
                .iter()
                .map(|entry| {
                    ensure!(entry.get("entries").is_none(), Failure::InvalidOutput);
                    let url = entry["webpage_url"]
                        .as_str()
                        .or_else(|| entry["original_url"].as_str())
                        .or_else(|| entry["url"].as_str())
                        .ok_or(Failure::InvalidOutput)?;
                    source_url(url)?;
                    metadata(entry)?;
                    Ok((entry, url.to_string()))
                })
                .collect::<Result<_>>()?
        } else {
            ensure!(
                value["_type"] != "playlist" && value["_type"] != "multi_video",
                Failure::InvalidInput
            );
            vec![(&value, url.into())]
        };
        let producer = executable_digest(&lease.config.tools.yt_dlp)?;
        let mut selected = vec![];
        for (order, (info, url)) in items.iter().enumerate() {
            let metadata = metadata(info)?;
            let workspace = self.store.workspace(&lease.job.id)?;
            let options = vec![
                "--no-simulate".into(),
                "--no-playlist".into(),
                "--format".into(),
                "bestaudio/best".into(),
                "--extract-audio".into(),
                "--audio-format".into(),
                "best".into(),
                "--ffmpeg-location".into(),
                lease.config.tools.ffmpeg.as_os_str().into(),
                "--max-filesize".into(),
                lease.config.limits.work_bytes.to_string().into(),
                "--output".into(),
                "candidate.%(ext)s".into(),
                "--print".into(),
                "after_move:%(filepath)j".into(),
            ];
            let output = self
                .execute(lease, url, workspace.path(), options, deadline, cancel)
                .await?;
            let lines: Vec<_> = output.lines().filter(|s| !s.trim().is_empty()).collect();
            ensure!(lines.len() == 1, Failure::InvalidOutput);
            let reported: String =
                serde_json::from_str(lines[0]).map_err(|_| Failure::InvalidOutput)?;
            let path = confined_file(workspace.path(), Path::new(&reported))
                .map_err(|_| Failure::InvalidOutput)?;
            let length = path.metadata()?.len();
            ensure!(
                length > 0 && length <= lease.config.limits.work_bytes,
                Failure::InvalidOutput
            );
            let blob = self
                .store
                .publish_reader(File::open(&path)?, lease.config.limits.work_bytes)?;
            let candidate = MediaCandidate {
                schema_version: 1,
                source_revision: source_revision.into(),
                config_id: lease.job.config_id.clone(),
                metadata,
                blob_digest: blob.digest,
                byte_length: length,
                producer_digest: producer.clone(),
                tools: lease.tools.clone(),
                final_path_name: path.file_name().unwrap().to_string_lossy().into_owned(),
                created_at: now(),
                pending_finalization: true,
                order,
            };
            let key = digest(serde_json::to_vec(
                &serde_json::json!({"version":"media-candidate-v1","source":source_revision,"config":lease.job.config_id,"order":order,"blob":candidate.blob_digest,"producer":producer}),
            )?);
            selected.push(self.store.publish_derivative(
                lease,
                &key,
                "media_candidate",
                source_revision,
                &candidate,
            )?);
        }
        Ok(selected)
    }
}
