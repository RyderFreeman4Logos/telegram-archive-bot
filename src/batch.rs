use crate::{
    config::Config,
    delivery::{Delivery, DeliveryWorker},
    digest,
    jobs::{JobState, Scope},
    storage::{Store, private_dir, sync_dir},
    telegram::{Operation, Request, Telegram},
};
use anyhow::{Context, Result, ensure};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde::Serialize;
use serde_json::json;
use std::{
    fs::{File, OpenOptions},
    io::Read,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

#[derive(Serialize)]
pub struct BatchResult {
    pub complete: usize,
    pub failed: usize,
    pub results: Vec<InputResult>,
}

#[derive(Serialize)]
pub struct InputResult {
    input: String,
    job_id: Option<String>,
    state: String,
    files: Vec<PathBuf>,
    error: Option<String>,
}

fn selected_config(config: &Config, commit: bool, send: bool) -> Result<Config> {
    let mut selected = config.clone();
    // Local input commands authorize local commits; remote pushes use project/run explicitly.
    selected.archive.push = false;
    if send {
        ensure!(config.telegram.enabled, "Telegram is disabled");
        ensure!(
            config.telegram.allowed_chats.len() == 1 && config.telegram.allowed_users.len() == 1,
            "operator sending requires exactly one authorized chat and user"
        );
    } else {
        selected.telegram.enabled = false;
        selected.telegram.bot_id = 0;
    }
    if commit {
        ensure!(
            config.archive.directory.is_some(),
            "configure the independent archive directory"
        );
    } else {
        selected.archive.directory = None;
    }
    Ok(selected)
}

fn admit(store: &Store, config: &Config, request: &Request) -> Result<(String, Scope)> {
    let scope = Scope {
        bot_id: config.telegram.bot_id,
        chat_id: if config.telegram.enabled {
            *config
                .telegram
                .allowed_chats
                .first()
                .context("missing chat")?
        } else {
            0
        },
        user_id: if config.telegram.enabled {
            *config
                .telegram
                .allowed_users
                .first()
                .context("missing user")?
        } else {
            0
        },
    };
    let config_json = serde_json::to_string(&request.job_config(config))?;
    let tools = json!({"application":env!("CARGO_PKG_VERSION"),"mode":"operator-v1"}).to_string();
    let config_id = digest(format!("{config_json}\n{tools}"));
    let request_json = serde_json::to_string(request)?;
    let requests = serde_json::to_string(&[request])?;
    let key = digest(format!(
        "{}:{}:{}:{config_id}:{request_json}",
        scope.bot_id, scope.chat_id, scope.user_id
    ));
    // Negative IDs reserve a deterministic operator namespace without advancing Telegram cursors.
    let update = -(i64::from_str_radix(&key[..15], 16)? + 1);
    let id = digest(format!("{}:{update}:0", scope.bot_id));
    let mut conn = store.connection()?;
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let existing = tx
        .query_row(
            "SELECT chat_id,user_id,payload FROM inbox WHERE bot_id=?1 AND update_id=?2",
            params![scope.bot_id, update],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;
    if let Some(existing) = existing {
        ensure!(
            existing == (scope.chat_id, scope.user_id, requests),
            "operator identity collision"
        );
        let saved: String = tx.query_row("SELECT config_id FROM jobs WHERE id=?1", [&id], |r| {
            r.get(0)
        })?;
        ensure!(saved == config_id, "operator configuration collision");
    } else {
        tx.execute(
            "INSERT INTO config_snapshots VALUES(?1,?2,?3) ON CONFLICT DO NOTHING",
            params![config_id, config_json, tools],
        )?;
        tx.execute(
            "INSERT INTO inbox VALUES(?1,?2,?3,?4,?5)",
            params![scope.bot_id, update, scope.chat_id, scope.user_id, requests],
        )?;
        tx.execute("INSERT INTO jobs(id,bot_id,update_id,ordinal,request,config_id,state) VALUES(?1,?2,?3,0,?4,?5,'queued')", params![id,scope.bot_id,update,request_json,config_id])?;
    }
    tx.commit()?;
    Ok((id, scope))
}

fn local_request(store: &Store, path: &Path, limit: u64, rename: bool) -> Result<Request> {
    let extension = path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    ensure!(
        matches!(extension.as_str(), "md" | "markdown" | "txt"),
        "expected a Markdown or text file"
    );
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file() && metadata.len() <= limit,
        "input must be a bounded regular file"
    );
    let blob = store.publish_reader(file, limit)?;
    let name = crate::documents::display_name(
        &path
            .file_name()
            .context("missing file name")?
            .to_string_lossy(),
    );
    Ok(Request::Transform {
        operation: Operation::Summarize,
        source: Box::new(Request::LocalDocument {
            digest: blob.digest,
            name,
            rename,
        }),
    })
}

fn copy_blob(store: &Store, digest: &str, destination: &Path) -> Result<()> {
    use sha2::{Digest, Sha256};
    let blob = store.verify_blob(digest)?;
    let verify_existing = || -> Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(destination)?;
        ensure!(
            file.metadata()?.is_file() && file.metadata()?.len() == blob.size,
            "output already exists with different content"
        );
        let mut input = file.take(blob.size + 1);
        let mut hasher = Sha256::new();
        std::io::copy(&mut input, &mut hasher)?;
        ensure!(
            format!("{:x}", hasher.finalize()) == digest,
            "output already exists with different content"
        );
        Ok(())
    };
    if destination.symlink_metadata().is_ok() {
        return verify_existing();
    }
    let parent = destination.parent().context("missing output directory")?;
    let mut stage = tempfile::NamedTempFile::new_in(parent)?;
    std::io::copy(&mut File::open(store.blob_path(digest)?)?, &mut stage)?;
    stage.as_file().sync_all()?;
    match stage.persist_noclobber(destination) {
        Ok(_) => {}
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => verify_existing()?,
        Err(error) => return Err(error.error.into()),
    }
    sync_dir(parent)
}

fn materialize(store: &Store, id: &str) -> Result<Vec<PathBuf>> {
    let directory = store.root().join("operator-results");
    private_dir(&directory)?;
    let directory = directory.join(id);
    private_dir(&directory)?;
    let rows = store
        .connection()?
        .prepare(
            "SELECT ordinal,payload FROM outbox WHERE job_id=?1 AND ordinal>=0 ORDER BY ordinal",
        )?
        .query_map([id], |r| {
            Ok((r.get::<_, usize>(0)?, r.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut files = Vec::new();
    for (ordinal, payload) in rows {
        let blobs = match serde_json::from_str::<Delivery>(&payload)? {
            Delivery::Document {
                blob_digest, name, ..
            } => vec![(blob_digest, name)],
            Delivery::Audio { artifact_id, .. } => {
                let artifact = store.artifact(&artifact_id)?;
                let extension = if artifact.mime == "audio/ogg" {
                    "ogg"
                } else {
                    "mp3"
                };
                let mut blobs = vec![(artifact.blob_digest, format!("audio.{extension}"))];
                if let Some(subtitle) = artifact.subtitle_digest {
                    blobs.push((subtitle, "audio.srt".into()));
                }
                blobs
            }
            Delivery::Text { .. } => vec![],
        };
        for (digest, name) in blobs {
            let path = directory.join(format!(
                "{ordinal:02}-{}",
                crate::documents::display_name(&name)
            ));
            copy_blob(store, &digest, &path)?;
            files.push(path);
        }
    }
    Ok(files)
}

async fn process(
    store: &Store,
    config: &Config,
    request: Request,
    result: &mut InputResult,
    cancel: &CancellationToken,
) -> Result<()> {
    let (id, scope) = admit(store, config, &request)?;
    result.job_id = Some(id.clone());
    let relay = std::env::current_exe()?;
    let deadline = Instant::now() + Duration::from_secs(config.limits.job_seconds);
    let job = loop {
        let job = store.job_for(scope, &id)?;
        if !matches!(
            job.state,
            JobState::Queued | JobState::Running | JobState::Retryable
        ) {
            break job;
        }
        ensure!(!store.recovery_paused()?, "recovery hold is active");
        ensure!(
            !cancel.is_cancelled(),
            "operator cancelled; persisted job retained"
        );
        ensure!(
            Instant::now() < deadline,
            "operator wait expired; persisted job retained"
        );
        if !crate::runtime::step_job(store, &id, &relay, cancel).await? {
            tokio::select! { _=cancel.cancelled()=>{}, _=tokio::time::sleep(Duration::from_millis(100))=>{} }
        }
    };
    result.state = serde_json::to_value(job.state)?
        .as_str()
        .context("invalid job state")?
        .into();
    result.error = job.error;
    if matches!(job.state, JobState::Complete | JobState::Partial) {
        result.files = materialize(store, &id)?;
    }
    if config.telegram.enabled {
        let worker = DeliveryWorker {
            store: store.clone(),
            bot_id: scope.bot_id,
            telegram: Telegram::new(
                config
                    .telegram
                    .token
                    .as_ref()
                    .context("missing Telegram token")?
                    .read()?,
            )?,
        };
        loop {
            let states = store
                .connection()?
                .prepare("SELECT state FROM outbox WHERE job_id=?1")?
                .query_map([&id], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            if states.iter().all(|s| s == "confirmed") {
                break;
            }
            ensure!(
                states.iter().all(|s| matches!(
                    s.as_str(),
                    "pending" | "retryable" | "sending" | "confirmed"
                )),
                "delivery needs reconciliation; inspect outbox"
            );
            ensure!(
                !cancel.is_cancelled() && Instant::now() < deadline,
                "delivery wait ended; inspect outbox before retrying"
            );
            ensure!(!store.recovery_paused()?, "recovery hold is active");
            if !worker.send_job(&id, cancel).await? {
                tokio::select! { _=cancel.cancelled()=>{}, _=tokio::time::sleep(Duration::from_millis(100))=>{} }
            }
        }
    }
    Ok(())
}

async fn run(
    config: &Config,
    inputs: impl IntoIterator<Item = (String, Result<Request>)>,
    store: &Store,
    cancel: &CancellationToken,
) -> BatchResult {
    let mut report = BatchResult {
        complete: 0,
        failed: 0,
        results: vec![],
    };
    for (input, request) in inputs {
        let mut result = InputResult {
            input,
            job_id: None,
            state: "failed".into(),
            files: vec![],
            error: None,
        };
        let outcome = match request {
            Ok(request) if !cancel.is_cancelled() => {
                process(store, config, request, &mut result, cancel).await
            }
            Ok(_) => Err(anyhow::anyhow!("operator cancelled before admission")),
            Err(error) => Err(error),
        };
        if let Err(error) = outcome {
            result.state = "failed".into();
            result.error = Some(error.to_string());
        }
        if result.state == "complete" {
            report.complete += 1;
        } else {
            report.failed += 1;
        }
        report.results.push(result);
    }
    report
}

pub async fn capture_url(
    config: &Config,
    url: &str,
    cancel: &CancellationToken,
) -> Result<BatchResult> {
    ensure!(
        url.len() as u64 <= config.limits.input_bytes,
        "URL exceeds input limit"
    );
    let config = selected_config(config, config.archive.directory.is_some(), false)?;
    let store = Store::open(&config.state_dir)?;
    let request = crate::network::source_url(url)
        .map(|url| Request::Url { url: url.into() })
        .map_err(Into::into);
    Ok(run(&config, vec![(url.into(), request)], &store, cancel).await)
}

pub async fn summarize(
    config: &Config,
    paths: Vec<PathBuf>,
    rename: bool,
    auto_commit: bool,
    send_telegram: bool,
    cancel: &CancellationToken,
) -> Result<BatchResult> {
    ensure!(config.model.enabled, "summary requires an enabled model");
    ensure!(!paths.is_empty(), "no input files");
    let config = selected_config(config, auto_commit, send_telegram)?;
    let store = Store::open(&config.state_dir)?;
    let inputs = paths.into_iter().map(|path| {
        (
            path.display().to_string(),
            if cancel.is_cancelled() {
                Err(anyhow::anyhow!("operator cancelled before reading input"))
            } else {
                local_request(&store, &path, config.limits.input_bytes, rename)
            },
        )
    });
    Ok(run(&config, inputs, &store, cancel).await)
}
