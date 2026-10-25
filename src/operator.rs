//! Offline diagnostics and a deliberately allowlisted structured log surface.
use crate::{config::Config, process::Tool, storage::Store};
use anyhow::Result;
use serde_json::{Value, json};
use std::{collections::BTreeMap, ffi::CString, os::unix::ffi::OsStrExt, time::Duration};
use tokio_util::sync::CancellationToken;

pub fn event(kind: &str, lease: Option<&crate::jobs::Lease>, detail: Value) -> Value {
    json!({"event":kind,"job_id":lease.map(|l|&l.job.id),"phase":lease.map(|l|l.job.phase),"attempt":lease.map(|l|l.job.attempt),"detail":detail})
}
pub(crate) fn log(kind: &str, lease: Option<&crate::jobs::Lease>, detail: Value) {
    eprintln!("{}", event(kind, lease, detail));
}
pub fn metrics(store: &Store) -> Result<Value> {
    let conn = store.connection()?;
    let mut counts = BTreeMap::new();
    for table in ["jobs", "outbox", "daily_windows"] {
        let state = if table == "jobs" {
            "CASE WHEN state='complete' AND partial=1 THEN 'partial' ELSE state END"
        } else {
            "state"
        };
        let rows = conn
            .prepare(&format!("SELECT {state},count(*) FROM {table} GROUP BY 1"))?
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?)))?
            .collect::<rusqlite::Result<BTreeMap<_, _>>>()?;
        counts.insert(table, rows);
    }
    let (blobs, bytes): (u64, u64) = conn.query_row(
        "SELECT count(*),coalesce(sum(size),0) FROM blobs",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let (events,cursor):(u64,u64)=conn.query_row("SELECT (SELECT coalesce(max(id),0) FROM projection_events),(SELECT coalesce(min(event_id),0) FROM projection_cursors)",[],|r|Ok((r.get(0)?,r.get(1)?)))?;
    let (runs,uses):(u64,u64)=conn.query_row("SELECT (SELECT count(*) FROM transformation_runs),(SELECT count(*) FROM transformation_uses)",[],|r|Ok((r.get(0)?,r.get(1)?)))?;
    let retries: u64 = conn.query_row(
        "SELECT coalesce(sum(max(attempts-1,0)),0) FROM phase_attempts",
        [],
        |r| r.get(0),
    )?;
    Ok(
        json!({"failure_reporting":store.failure_report_status(20)?,"notion_publication":store.notion_metrics()?,"queues":counts,"retained_blobs":blobs,"retained_bytes":bytes,"projection_event_lag":events.saturating_sub(cursor),"transformation_runs":runs,"cache_reuses":uses,"observed_host_cache_hit_fraction":if runs+uses>0{Some(uses as f64/(runs+uses) as f64)}else{None},"job_retries":retries,"token_cost_estimate":null,"token_cost_estimate_status":"provider-not-reported","external_effects_paused":store.recovery_paused()?}),
    )
}

pub async fn doctor(config: &Config, store: &Store, cancel: &CancellationToken) -> Result<Value> {
    config.validate()?;
    let mut secrets = vec![];
    for reference in [
        &config.telegram.token,
        &config.model.key,
        &config.translation.key,
        &config.sources.x_bearer,
        &config.sources.x_guest_bearer,
    ]
    .into_iter()
    .flatten()
    {
        if let Ok(value) = reference.read() {
            secrets.push(value);
        }
    }
    let secret_ready = config.check_secrets().is_ok();
    use std::os::unix::fs::PermissionsExt;
    let work = tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .prefix("doctor-")
        .tempdir_in(store.root().join("work"))?;
    let mut tools = BTreeMap::new();
    let mut ready = secret_ready;
    for (name, path, enabled, arg) in [
        (
            "ffmpeg",
            &config.tools.ffmpeg,
            config.synthesis.enabled || config.sources.media_enabled,
            "-version",
        ),
        (
            "ffprobe",
            &config.tools.ffprobe,
            config.synthesis.enabled || config.sources.media_enabled,
            "-version",
        ),
        (
            "yt_dlp",
            &config.tools.yt_dlp,
            config.sources.media_enabled,
            "--version",
        ),
        (
            "bubblewrap",
            &config.tools.bubblewrap,
            config.sources.media_enabled || config.sources.browser_enabled,
            "--version",
        ),
        (
            "browser",
            &config.tools.browser,
            config.sources.browser_enabled,
            "--version",
        ),
        (
            "md2audio",
            &config.tools.md2audio,
            config.synthesis.enabled,
            "--help",
        ),
        (
            "hymt_bridge",
            &config.tools.hymt_bridge,
            config.translation.enabled,
            "--version",
        ),
        (
            "git",
            &config.tools.git,
            config.archive.directory.is_some(),
            "--version",
        ),
        ("pdfinfo", &config.tools.pdfinfo, false, "-v"),
        ("pdftotext", &config.tools.pdftotext, false, "-v"),
    ] {
        let identity = if name == "browser" {
            crate::browser::system_path(path)
                .and_then(|path| crate::transforms::executable(&path))
                .ok()
        } else {
            crate::transforms::executable(path).ok()
        };
        let mut item = json!({"enabled":enabled,"identity":identity,"available":identity.is_some(),"diagnosis":if identity.is_some(){"present"}else if enabled{"enabled-feature-missing-tool"}else{"optional-feature-unavailable"}});
        if identity.is_some() {
            let result = Tool {
                executable: path.clone(),
                args: vec![arg.into()],
                environment: BTreeMap::new(),
                directory: work.path().to_owned(),
                timeout: Duration::from_secs(10),
                log_bytes: 65536,
                stdout_bytes: Some(65536),
                work_bytes: 1_000_000,
                secrets: secrets.clone(),
            }
            .run(cancel)
            .await;
            match result {
                Ok(output) => {
                    let text = if output.stdout.is_empty() {
                        &output.stderr
                    } else {
                        &output.stdout
                    };
                    item["version_or_help"] = json!(text.lines().take(3).collect::<Vec<_>>());
                    item["probe_passed"] = json!(true);
                }
                Err(_) => {
                    item["probe_passed"] = json!(false);
                    item["diagnosis"] = json!("version-probe-failed");
                    if enabled {
                        ready = false;
                    }
                }
            }
        } else if enabled {
            ready = false;
        }
        tools.insert(name, item);
    }
    let namespace = if config.sources.media_enabled || config.sources.browser_enabled {
        Some(
            Tool {
                executable: config.tools.bubblewrap.clone(),
                args: [
                    "--unshare-all",
                    "--ro-bind",
                    "/",
                    "/",
                    "--proc",
                    "/proc",
                    "--",
                    "/usr/bin/true",
                ]
                .into_iter()
                .map(Into::into)
                .collect(),
                environment: BTreeMap::new(),
                directory: work.path().to_owned(),
                timeout: Duration::from_secs(10),
                log_bytes: 65536,
                stdout_bytes: Some(65536),
                work_bytes: 1_000_000,
                secrets: vec![],
            }
            .run(cancel)
            .await
            .is_ok(),
        )
    } else {
        None
    };
    if namespace == Some(false) {
        ready = false;
    }
    let codecs = Tool {
        executable: config.tools.ffmpeg.clone(),
        args: vec!["-v".into(), "error".into(), "-encoders".into()],
        environment: BTreeMap::new(),
        directory: work.path().to_owned(),
        timeout: Duration::from_secs(10),
        log_bytes: 65536,
        stdout_bytes: Some(65536),
        work_bytes: 1_000_000,
        secrets,
    }
    .run(cancel)
    .await
    .ok();
    let encoders: BTreeMap<_, _> = ["libmp3lame", "aac", "libopus"]
        .into_iter()
        .map(|codec| {
            (
                codec,
                codecs
                    .as_ref()
                    .is_some_and(|o| o.stdout.split_whitespace().any(|w| w == codec)),
            )
        })
        .collect();
    if config.synthesis.enabled || config.sources.media_enabled {
        ready &= encoders.values().all(|v| *v);
    }
    let integrity = store.integrity();
    ready &= integrity.is_ok();
    let path = CString::new(store.root().as_os_str().as_bytes())?;
    let mut disk = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let free = if unsafe { libc::statvfs(path.as_ptr(), disk.as_mut_ptr()) } == 0 {
        let d = unsafe { disk.assume_init() };
        Some(d.f_bavail.saturating_mul(d.f_frsize))
    } else {
        None
    };
    ready &= free.is_some_and(|b| b >= config.limits.work_bytes);
    let schema: u32 = store
        .connection()?
        .pragma_query_value(None, "user_version", |r| r.get(0))?;
    // DA05: publication prerequisites do not gate otherwise usable local intake.
    let notion = crate::notion::readiness(&config.notion);
    Ok(
        json!({"schema_version":1,"application_version":env!("CARGO_PKG_VERSION"),"ready":ready,"notion":notion,"failure_reporting":crate::failure_reporting::readiness(&config.failure_reporting),"network_probes_performed":false,"telegram_messages_sent":0,"secrets_resolvable":secret_ready,"database_schema":schema,"integrity_passed":integrity.is_ok(),"free_bytes":free,"minimum_work_headroom":config.limits.work_bytes,"tools":tools,"encoders":encoders,"media_namespace_probe_passed":if config.sources.media_enabled { namespace } else { None },"browser_namespace_probe_passed":if config.sources.browser_enabled { namespace } else { None },"effective":{"config_fingerprint":config.fingerprint()?,"model_enabled":config.model.enabled,"translation_enabled":config.translation.enabled,"synthesis_enabled":config.synthesis.enabled,"synthesis_provider":if config.synthesis.local{config.synthesis.local_model.as_str()}else{"edge"},"media_enabled":config.sources.media_enabled,"browser_enabled":config.sources.browser_enabled,"x_enabled":config.sources.x_enabled,"x_fallback_enabled":config.sources.x_fallback_enabled,"x_guest_enabled":config.sources.x_guest_enabled,"archive_enabled":config.archive.directory.is_some(),"push_enabled":config.archive.push,"narration_mode":config.telegram.narration_mode,"limits":config.limits},"metrics":metrics(store)?}),
    )
}
