//! DA09 / #76: real pre-019 schema and headless autonomous runtime boundaries.
use anyhow::{Result, ensure};
use std::{
    fs,
    os::unix::{fs::PermissionsExt, process::CommandExt},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use telegram_archive_bot::{
    config::{Config, WatchAccount},
    storage::Store,
};

#[test]
fn actual_historical_fourteen_upgrades_without_optional_schema_placeholders() -> Result<()> {
    historical_schema_upgrades(14)
}
#[test]
fn actual_historical_eighteen_upgrades_with_all_real_producer_tables() -> Result<()> {
    historical_schema_upgrades(18)
}
fn historical_schema_upgrades(version: u32) -> Result<()> {
    let root = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()?;
    let state = root.path().join("state");
    fs::create_dir(&state)?;
    fs::set_permissions(&state, fs::Permissions::from_mode(0o700))?;
    let conn = rusqlite::Connection::open(state.join("archive.sqlite"))?;
    for sql in [
        include_str!("../migrations/001_canonical.sql"),
        include_str!("../migrations/002_revision_quality.sql"),
        include_str!("../migrations/003_ingress.sql"),
        include_str!("../migrations/004_job_recovery.sql"),
        include_str!("../migrations/005_derivatives.sql"),
        include_str!("../migrations/006_immutable_artifacts.sql"),
        include_str!("../migrations/007_delivery_attempts.sql"),
        include_str!("../migrations/008_projection.sql"),
        include_str!("../migrations/009_recovery.sql"),
        include_str!("../migrations/010_transforms.sql"),
        include_str!("../migrations/011_message_origins.sql"),
        include_str!("../migrations/012_observation_source.sql"),
        include_str!("../migrations/013_account_activities.sql"),
        include_str!("../migrations/014_daily_planner.sql"),
    ] {
        conn.execute_batch(sql)?;
    }
    if version == 18 {
        for sql in [
            include_str!("../migrations/015_notion_publication.sql"),
            include_str!("../migrations/016_daily_capture.sql"),
            include_str!("../migrations/017_notion_pdf.sql"),
            include_str!("../migrations/018_failure_reporting.sql"),
        ] {
            conn.execute_batch(sql)?;
        }
    }
    conn.pragma_update(None, "user_version", version)?;
    drop(conn);
    let store = Store::open(&state)?;
    let conn = rusqlite::Connection::open(state.join("archive.sqlite"))?;
    assert_eq!(
        conn.query_row("PRAGMA user_version", [], |r| r.get::<_, u32>(0))?,
        19
    );
    assert_eq!(
        conn.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))?,
        "ok"
    );
    assert_eq!(
        conn.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| r
            .get::<_, u64>(
            0
        ))?,
        0
    );
    assert_eq!(store.reader_status()?["heads"], 0);
    let config = Config {
        state_dir: state,
        ..Config::default()
    };
    assert_eq!(store.reader_tick(&config, 1)?.errors, 0);
    Ok(())
}

struct Service(Child);
impl Drop for Service {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            unsafe {
                libc::kill(-(self.0.id() as i32), libc::SIGKILL);
            }
            let _ = self.0.wait();
        }
    }
}
fn wait(
    service: &mut Service,
    log: &std::path::Path,
    mut predicate: impl FnMut() -> bool,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        ensure!(
            service.0.try_wait()?.is_none(),
            "fixture runtime exited: {}",
            fs::read_to_string(log)?
        );
        if predicate() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    anyhow::bail!(
        "bounded fixture precondition timed out: {}",
        fs::read_to_string(log)?
    )
}
#[test]
fn run_autonomously_builds_after_atomic_reader_reload_with_all_remote_capabilities_off()
-> Result<()> {
    let root = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()?;
    let mut config = Config {
        state_dir: root.path().join("state"),
        ..Config::default()
    };
    config.daily.enabled = true;
    config.daily.accounts = vec![WatchAccount {
        key: "fixture".into(),
        user_id: Some("1".into()),
        ..WatchAccount::default()
    }];
    config.notion.enabled = false;
    config.reader.enabled = false;
    config.archive.directory = None;
    config.archive.push = false;
    for path in [
        &mut config.tools.ffmpeg,
        &mut config.tools.ffprobe,
        &mut config.tools.yt_dlp,
        &mut config.tools.md2audio,
        &mut config.tools.pdfinfo,
        &mut config.tools.pdftotext,
        &mut config.tools.git,
        &mut config.tools.hymt_bridge,
        &mut config.tools.bubblewrap,
        &mut config.tools.browser,
    ] {
        *path = "/nonexistent/da09-fixture".into();
    }
    let store = Store::open(&config.state_dir)?;
    store.backfill_daily(
        &config,
        "2026-03-08".parse()?,
        "2026-03-08".parse()?,
        1_790_000_000,
    )?;
    config.daily.enabled = false;
    let path = root.path().join("config.toml");
    fs::write(&path, toml::to_string(&config)?)?;
    let log = root.path().join("runtime.log");
    let mut service = Service(
        Command::new(env!("CARGO_BIN_EXE_telegram-archive-bot"))
            .arg("--config")
            .arg(&path)
            .arg("run")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(fs::File::create(&log)?)
            .process_group(0)
            .spawn()?,
    );
    wait(&mut service, &log, || {
        fs::read_to_string(&log)
            .unwrap()
            .contains("service_started")
    })?;
    assert_eq!(
        store.reader_status()?["heads"],
        0,
        "disabled reader does not drain"
    );
    config.reader.enabled = true;
    fs::write(&path, toml::to_string(&config)?)?;
    assert_eq!(
        unsafe { libc::kill(service.0.id() as i32, libc::SIGHUP) },
        0
    );
    wait(&mut service, &log, || {
        store
            .reader_heads("2026-03-08", "America/Los_Angeles", 100, None)
            .unwrap()
            .len()
            == 2
    })?;
    let before = store
        .reader_heads("2026-03-08", "America/Los_Angeles", 100, None)?
        .into_iter()
        .find(|h| h.window_id.is_none())
        .unwrap();
    config.reader.sop.version = "invalid-empty-next".into();
    config.reader.max_activities = 0;
    fs::write(&path, toml::to_string(&config)?)?;
    assert_eq!(
        unsafe { libc::kill(service.0.id() as i32, libc::SIGHUP) },
        0
    );
    wait(&mut service, &log, || {
        fs::read_to_string(&log)
            .unwrap()
            .contains("reload_rejected")
    })?;
    assert_eq!(
        store
            .reader_heads("2026-03-08", "America/Los_Angeles", 100, None)?
            .into_iter()
            .find(|h| h.window_id.is_none())
            .unwrap()
            .manifest_id,
        before.manifest_id
    );
    assert_eq!(
        unsafe { libc::kill(service.0.id() as i32, libc::SIGTERM) },
        0
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut exited = false;
    while Instant::now() < deadline {
        if let Some(exit) = service.0.try_wait()? {
            ensure!(exit.success(), "fixture runtime failure");
            exited = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(exited, "runtime must naturally shut down");
    let conn = rusqlite::Connection::open(config.state_dir.join("archive.sqlite"))?;
    for table in ["polling_cursors", "delivery_attempts", "notion_attempts"] {
        assert_eq!(
            conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                .get::<_, u64>(0))?,
            0,
            "no disabled remote work: {table}"
        );
    }
    assert!(
        !root.path().join("archive").exists(),
        "no export/push requested by manifest worker"
    );
    Ok(())
}
