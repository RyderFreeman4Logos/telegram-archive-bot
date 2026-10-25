//! DA02 / #69: public configuration, CLI, durable planning and offline runtime.
use anyhow::Result;
use serde_json::Value;
use std::{fs, os::unix::fs::PermissionsExt, process::Command};
use telegram_archive_bot::config::Config;

fn fixture() -> Result<(tempfile::TempDir, std::path::PathBuf, Config)> {
    let root = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()?;
    let mut config: Config = toml::from_str(
        r#"
[daily]
enabled=true
interval_seconds=1
first_run_lookback_days=1
[[daily.accounts]]
key="alice"
user_id="101"
handle="alice"
"#,
    )?;
    config.state_dir = root.path().join("state");
    let path = root.path().join("config.toml");
    fs::write(&path, toml::to_string(&config)?)?;
    Ok((root, path, config))
}
fn cli(path: &std::path::Path, args: &[&str]) -> Result<Value> {
    let output = Command::new(env!("CARGO_BIN_EXE_telegram-archive-bot"))
        .arg("--config")
        .arg(path)
        .args(args)
        .output()?;
    anyhow::ensure!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(serde_json::from_slice(&output.stdout)?)
}

#[test]
fn da02_public_backfill_is_idempotent_bounded_and_dst_aware() -> Result<()> {
    let (_root, path, config) = fixture()?;
    let preview = cli(
        &path,
        &[
            "daily-backfill",
            "--from",
            "2024-03-10",
            "--through",
            "2024-03-10",
            "--dry-run",
        ],
    )?;
    assert!(
        !config.state_dir.exists(),
        "dry-run must not initialize state"
    );
    assert_eq!(preview["windows"][0]["utc_start"], 1710057600_i64);
    assert_eq!(preview["windows"][0]["utc_end"], 1710140400_i64); // 23 hours
    let args = [
        "daily-backfill",
        "--from",
        "2024-03-10",
        "--through",
        "2024-03-10",
    ];
    assert_eq!(cli(&path, &args)?["created"], 1);
    assert_eq!(cli(&path, &args)?["created"], 0);
    let fall = cli(
        &path,
        &[
            "daily-backfill",
            "--from",
            "2024-11-03",
            "--through",
            "2024-11-03",
        ],
    )?;
    assert_eq!(fall["windows"][0]["utc_start"], 1730617200_i64);
    assert_eq!(fall["windows"][0]["utc_end"], 1730707200_i64); // 25 hours
    assert_eq!(
        cli(&path, &["daily-status", "--limit", "10"])?["windows"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let failed = Command::new(env!("CARGO_BIN_EXE_telegram-archive-bot"))
        .arg("--config")
        .arg(&path)
        .args([
            "daily-backfill",
            "--from",
            "2000-01-01",
            "--through",
            "2024-11-03",
        ])
        .output()?;
    assert!(!failed.status.success());
    assert_eq!(
        cli(&path, &["daily-status", "--limit", "10"])?["windows"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    Ok(())
}

#[test]
fn da02_actual_run_plans_and_reloads_without_telegram_or_models() -> Result<()> {
    use std::time::{Duration, Instant};
    let (root, path, mut config) = fixture()?;
    config.daily.reconciliation_days = 0;
    for tool in [
        &mut config.tools.ffmpeg,
        &mut config.tools.ffprobe,
        &mut config.tools.git,
        &mut config.tools.pdfinfo,
        &mut config.tools.pdftotext,
        &mut config.tools.bubblewrap,
        &mut config.tools.browser,
        &mut config.tools.yt_dlp,
        &mut config.tools.md2audio,
        &mut config.tools.hymt_bridge,
    ] {
        *tool = "/nonexistent/DA02-disabled-tool".into();
    }
    fs::write(&path, toml::to_string(&config)?)?;
    let log = root.path().join("runtime.log");
    struct Guard(std::process::Child);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut child = Guard(
        Command::new(env!("CARGO_BIN_EXE_telegram-archive-bot"))
            .arg("--config")
            .arg(&path)
            .arg("run")
            .stdin(std::process::Stdio::null())
            .stdout(fs::File::create(root.path().join("stdout"))?)
            .stderr(fs::File::create(&log)?)
            .spawn()?,
    );
    fn wait(child: &mut Guard, condition: impl Fn() -> bool) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if condition() {
                return Ok(());
            }
            anyhow::ensure!(
                child.0.try_wait()?.is_none(),
                "daily-only run exited before durable planner work"
            );
            anyhow::ensure!(
                Instant::now() < deadline,
                "daily runtime durable side effect timed out"
            );
            std::thread::sleep(Duration::from_millis(25));
        }
    }
    let count = || -> usize {
        rusqlite::Connection::open_with_flags(
            config.state_dir.join("archive.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .and_then(|c| c.query_row("SELECT COUNT(*) FROM daily_windows", [], |r| r.get(0)))
        .unwrap_or(0)
    };
    wait(&mut child, || count() == 1)?;
    let mut second = config.daily.accounts[0].clone();
    second.key = "bob".into();
    second.user_id = Some("202".into());
    second.handle = Some("bob".into());
    config.daily.accounts.push(second);
    let replace = |bytes: &str| -> Result<()> {
        let next = path.with_extension("next");
        fs::write(&next, bytes)?;
        fs::rename(next, &path)?;
        Ok(())
    };
    replace(&toml::to_string(&config)?)?;
    unsafe {
        libc::kill(child.0.id() as i32, libc::SIGHUP);
    }
    wait(&mut child, || count() == 2)?;
    replace("invented='SYNTHETIC_MUST_NOT_LEAK'")?;
    unsafe {
        libc::kill(child.0.id() as i32, libc::SIGHUP);
    }
    wait(&mut child, || {
        fs::read_to_string(&log).is_ok_and(|s| s.contains("reload_rejected"))
    })?;
    assert_eq!(count(), 2);
    let conn = rusqlite::Connection::open(config.state_dir.join("archive.sqlite"))?;
    assert_eq!(conn.query_row("SELECT COUNT(*) FROM daily_windows WHERE attempts=0 AND state='open_current_day' AND reason='timeline_provider_not_installed'",[],|r|r.get::<_,usize>(0))?,2);
    for table in ["polling_cursors", "delivery_attempts", "projection_pushes"] {
        assert_eq!(
            conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r
                .get::<_, usize>(0))?,
            0
        );
    }
    unsafe {
        libc::kill(child.0.id() as i32, libc::SIGTERM);
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.0.try_wait()? {
            assert!(status.success());
            break;
        }
        anyhow::ensure!(Instant::now() < deadline, "graceful shutdown timed out");
        std::thread::sleep(Duration::from_millis(25));
    }
    let logged = fs::read_to_string(log)?;
    assert!(logged.contains("reload_applied") && logged.contains("service_stopped"));
    assert!(
        !logged.contains("poll_error")
            && !logged.contains("delivery_error")
            && !logged.contains("SYNTHETIC_MUST_NOT_LEAK")
    );
    Ok(())
}

fn clock() -> i64 {
    chrono::DateTime::parse_from_rfc3339("2024-11-10T12:00:00Z")
        .unwrap()
        .timestamp()
}
fn progress(
    state: telegram_archive_bot::planner::DayState,
    cursor: Option<&str>,
) -> telegram_archive_bot::planner::DailyProgress {
    telegram_archive_bot::planner::DailyProgress {
        state,
        cursor: cursor.map(str::to_owned),
        reason: None,
        evidence: serde_json::json!({"synthetic_coverage":true}),
        coverage_complete: true,
    }
}

#[test]
fn da02_reconciliation_resumes_with_a_one_window_tick_budget() -> Result<()> {
    use telegram_archive_bot::{planner::DayState, storage::Store};
    let (_root, _path, mut config) = fixture()?;
    config.daily.first_run_lookback_days = 4;
    config.daily.reconciliation_days = 3;
    config.daily.max_windows_per_tick = 1;
    let store = Store::open(&config.state_dir)?;
    for _ in 0..4 {
        let report = store.daily_tick(&config, clock())?;
        assert!(report.visited <= 1);
    }
    assert_eq!(store.daily_status(10, None, clock())?.len(), 4);
    for _ in 0..4 {
        let lease = store.claim_daily_window(&config, clock() + 1)?.unwrap();
        store.finish_daily_window(&lease, progress(DayState::Complete, None), clock() + 2)?;
    }
    for _ in 0..3 {
        store.daily_tick(&config, clock() + 20)?;
    }
    let windows = store.daily_status(10, None, clock() + 20)?;
    assert_eq!(
        windows
            .iter()
            .find(|w| w.plan.local_date == "2024-11-09")
            .unwrap()
            .state,
        DayState::Blocked,
        "reconciliation must advance beyond the first already-visited day"
    );
    Ok(())
}

#[test]
fn da02_interruptions_checkpoint_cursor_fencing_and_retry_budgets_are_durable() -> Result<()> {
    use telegram_archive_bot::{planner::DayState, storage::Store};
    let (_root, _path, mut config) = fixture()?;
    config.daily.provider.max_pages = 1;
    config.daily.max_attempts = 2;
    let store = Store::open(&config.state_dir)?;
    let date = "2024-11-01".parse()?;
    let id = store.backfill_daily(&config, date, date, clock())?.windows[0]
        .id
        .clone();
    let first = store.claim_daily_window(&config, clock())?.unwrap();
    assert_eq!(
        store.daily_window(&id, clock())?.state,
        DayState::Collecting
    );
    store.checkpoint_daily(
        &first,
        "cursor-A",
        &serde_json::json!({"page":1}),
        clock() + 1,
    )?;
    assert!(
        store
            .checkpoint_daily(&first, "cursor-A", &serde_json::json!({}), clock() + 2)
            .is_err()
    );
    drop(store);
    let store = Store::open(&config.state_dir)?;
    let later = clock() + config.limits.lease_seconds as i64 + 2;
    let resumed = store.claim_daily_window(&config, later)?.unwrap();
    assert_eq!(resumed.window.cursor.as_deref(), Some("cursor-A"));
    assert!(resumed.window.generation > first.window.generation);
    assert!(
        store
            .finish_daily_window(&first, progress(DayState::Complete, None), later)
            .is_err()
    );
    store.checkpoint_daily(
        &resumed,
        "cursor-B",
        &serde_json::json!({"page":2}),
        later + 1,
    )?;
    assert!(
        store
            .checkpoint_daily(
                &resumed,
                "cursor-C",
                &serde_json::json!({"page":3}),
                later + 1
            )
            .is_err()
    );
    store.finish_daily_window(
        &resumed,
        progress(DayState::Partial, Some("cursor-B")),
        later + 1,
    )?;
    assert_eq!(store.daily_window(&id, later + 1)?.state, DayState::Partial);
    assert!(store.claim_daily_window(&config, later + 1)?.is_none());
    let stalled = store.claim_daily_window(&config, later + 2)?.unwrap();
    store.finish_daily_window(
        &stalled,
        progress(DayState::Partial, Some("cursor-B")),
        later + 2,
    )?;
    assert_eq!(
        store.daily_window(&id, later + 2)?.reason.as_deref(),
        Some("cursor_stalled")
    );
    let failed = store.claim_daily_window(&config, later + 3)?.unwrap();
    store.finish_daily_window(
        &failed,
        progress(DayState::Failed, Some("cursor-B")),
        later + 3,
    )?;
    assert!(store.claim_daily_window(&config, later + 4)?.is_none());
    assert_eq!(store.daily_window(&id, later + 4)?.state, DayState::Failed);
    assert_eq!(
        store.daily_window(&id, later + 4)?.reason.as_deref(),
        Some("attempt_budget_exhausted")
    );
    config.daily.max_windows_per_tick = 1;
    let next = "2024-11-02".parse()?;
    store.backfill_daily(&config, next, next, later + 4)?;
    assert!(
        store.claim_daily_window(&config, later + 5)?.is_some(),
        "an exhausted old window must not starve newer work"
    );
    Ok(())
}

#[test]
fn da02_reload_renames_removal_and_attempt_snapshots_preserve_identity() -> Result<()> {
    use telegram_archive_bot::{config::ConfigManager, planner::DayState, storage::Store};
    let (_root, path, config) = fixture()?;
    let store = Store::open(&config.state_dir)?;
    let date = "2024-11-01".parse()?;
    let original = store.backfill_daily(&config, date, date, clock())?.windows[0].clone();
    let first = store.claim_daily_window(&config, clock())?.unwrap();
    let mut manager = ConfigManager::new(config.clone())?;
    let pinned = manager.snapshot();
    let mut next = config.clone();
    next.daily.accounts[0].handle = Some("renamed".into());
    next.daily.provider.page_size = 20;
    fs::write(&path, toml::to_string(&next)?)?;
    manager.reload(&path)?;
    store.validate_daily_config(&manager.snapshot())?;
    assert_eq!(pinned.daily.accounts[0].handle.as_deref(), Some("alice"));
    assert_eq!(first.watch.handle.as_deref(), Some("alice"));
    let repeated = store.backfill_daily(&next, date, date, clock())?;
    assert_eq!(repeated.created, 0);
    assert_eq!(repeated.windows[0].config_id, original.config_id);
    assert_eq!(repeated.windows[0].watch.handle.as_deref(), Some("alice"));
    assert_eq!(
        store.daily_window(&original.id, clock())?.plan.config_id,
        original.config_id
    );
    store.finish_daily_window(
        &first,
        progress(DayState::Partial, Some("next")),
        clock() + 1,
    )?;
    let second = store.claim_daily_window(&next, clock() + 2)?.unwrap();
    assert_eq!(second.watch.handle.as_deref(), Some("renamed"));
    assert_ne!(second.config_id, first.config_id);
    fs::write(&path, "[daily]\ninvented='private-invalid'")?;
    assert!(manager.reload(&path).is_err());
    assert_eq!(manager.snapshot().fingerprint()?, next.fingerprint()?);
    let mut conflict = next.clone();
    conflict.daily.accounts[0].user_id = Some("999".into());
    assert!(store.validate_daily_config(&conflict).is_err());
    assert!(
        store
            .backfill_daily(&conflict, date, date, clock())
            .is_err()
    );
    next.daily.accounts.clear();
    assert_eq!(store.backfill_daily(&next, date, date, clock())?.created, 0);
    assert!(store.claim_daily_window(&next, clock() + 3)?.is_none());
    assert_eq!(store.daily_status(10, None, clock())?.len(), 1);
    // Disabling new admissions does not mutate an already-running lease.
    store.finish_daily_window(&second, progress(DayState::Complete, None), clock() + 3)?;
    assert_eq!(
        store.daily_window(&original.id, clock() + 3)?.state,
        DayState::Complete
    );
    Ok(())
}

#[test]
fn da02_backup_restore_keeps_cursor_and_fences_collection_under_hold() -> Result<()> {
    use telegram_archive_bot::{planner::DayState, storage::Store};
    let (root, _path, config) = fixture()?;
    let store = Store::open(&config.state_dir)?;
    let date = "2024-11-01".parse()?;
    let id = store.backfill_daily(&config, date, date, clock())?.windows[0]
        .id
        .clone();
    let lease = store.claim_daily_window(&config, clock())?.unwrap();
    store.checkpoint_daily(
        &lease,
        "retained-cursor",
        &serde_json::json!({"fixture":true}),
        clock() + 1,
    )?;
    let backup = root.path().join("backup");
    store.backup(&backup)?;
    Store::verify_backup(&backup)?;
    let destination = root.path().join("restored");
    Store::restore_backup(&backup, &destination)?;
    let rebuilt = Store::open(&destination)?;
    assert_eq!(
        rebuilt.daily_window(&id, clock() + 1)?.state,
        DayState::Partial
    );
    assert_eq!(
        rebuilt.daily_window(&id, clock() + 1)?.cursor.as_deref(),
        Some("retained-cursor")
    );
    let mut restored = config;
    restored.state_dir = destination;
    assert!(rebuilt.daily_tick(&restored, clock() + 2)?.paused);
    assert!(
        rebuilt
            .claim_daily_window(&restored, clock() + 2)?
            .is_none()
    );
    rebuilt.release_recovery_hold()?;
    let resumed = rebuilt.claim_daily_window(&restored, clock() + 3)?.unwrap();
    assert!(resumed.window.generation > lease.window.generation);
    assert!(
        rebuilt
            .finish_daily_window(&lease, progress(DayState::Complete, None), clock() + 3)
            .is_err()
    );
    assert_eq!(rebuilt.integrity()?, 0);
    Ok(())
}

#[test]
fn da02_strong_identity_resolution_is_fenced_and_does_not_rekey_old_plans() -> Result<()> {
    use telegram_archive_bot::{activities::AccountIdentity, planner::DayState, storage::Store};
    let (_root, _path, mut config) = fixture()?;
    config.daily.accounts[0].user_id = None;
    let store = Store::open(&config.state_dir)?;
    let date = "2024-11-01".parse()?;
    let id = store.backfill_daily(&config, date, date, clock())?.windows[0]
        .id
        .clone();
    let first = store.claim_daily_window(&config, clock())?.unwrap();
    let resolved = store.bind_daily_account(
        &first,
        "101",
        &serde_json::json!({"synthetic_provider_identity":true}),
        clock() + 1,
    )?;
    assert!(matches!(resolved,AccountIdentity::ProviderId {ref user_id,..} if user_id=="101"));
    assert!(
        store
            .bind_daily_account(&first, "999", &serde_json::json!({}), clock() + 1)
            .is_err()
    );
    store.finish_daily_window(
        &first,
        progress(DayState::Partial, Some("next")),
        clock() + 1,
    )?;
    assert!(
        store
            .bind_daily_account(&first, "101", &serde_json::json!({}), clock() + 2)
            .is_err()
    );
    config.daily.accounts[0].handle = Some("renamed".into());
    assert_eq!(
        store
            .backfill_daily(&config, date, date, clock() + 2)?
            .created,
        0
    );
    let next = store.claim_daily_window(&config, clock() + 2)?.unwrap();
    assert_eq!(next.resolved_account, Some(resolved));
    assert_eq!(next.window.plan.id, id);
    assert!(matches!(
        next.window.plan.account,
        AccountIdentity::Unresolved { .. }
    ));
    Ok(())
}

#[test]
fn da02_checkpoint_refuses_mutated_inflight_watch_or_window() -> Result<()> {
    use telegram_archive_bot::storage::Store;
    let (_root, _path, config) = fixture()?;
    let store = Store::open(&config.state_dir)?;
    let date = "2024-11-01".parse()?;
    store.backfill_daily(&config, date, date, clock())?;
    let lease = store.claim_daily_window(&config, clock())?.unwrap();
    let mut altered = lease.clone();
    altered.watch.handle = Some("wrong_target".into());
    assert!(
        store
            .checkpoint_daily(&altered, "untrusted", &serde_json::json!({}), clock() + 1)
            .is_err()
    );
    let mut altered = lease;
    altered.window.plan.utc_end += 86400;
    assert!(
        store
            .checkpoint_daily(&altered, "untrusted", &serde_json::json!({}), clock() + 1)
            .is_err()
    );
    Ok(())
}

#[test]
fn da02_checkpoint_retains_each_page_receipt() -> Result<()> {
    use telegram_archive_bot::storage::Store;
    let (_root, _path, config) = fixture()?;
    let store = Store::open(&config.state_dir)?;
    let date = "2024-11-01".parse()?;
    store.backfill_daily(&config, date, date, clock())?;
    let lease = store.claim_daily_window(&config, clock())?.unwrap();
    store.checkpoint_daily(
        &lease,
        "A",
        &serde_json::json!({"synthetic_page":1}),
        clock() + 1,
    )?;
    store.checkpoint_daily(
        &lease,
        "B",
        &serde_json::json!({"synthetic_page":2}),
        clock() + 2,
    )?;
    let conn = rusqlite::Connection::open(config.state_dir.join("archive.sqlite"))?;
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='daily_pages'",
            [],
            |r| r.get::<_, usize>(0)
        )?,
        1,
        "each checkpoint needs immutable page evidence"
    );
    let pages = conn
        .prepare("SELECT cursor,evidence FROM daily_pages ORDER BY page")?
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    assert_eq!(pages.len(), 2);
    assert_eq!(pages[0].0, "A");
    assert_eq!(pages[1].0, "B");
    assert_eq!(
        serde_json::from_str::<Value>(&pages[0].1)?["synthetic_page"],
        1
    );
    assert!(
        store
            .checkpoint_daily(&lease, "A", &serde_json::json!({"cycle":true}), clock() + 3)
            .is_err()
    );
    Ok(())
}

#[test]
fn da02_five_hundred_accounts_have_bounded_fair_admissions() -> Result<()> {
    use telegram_archive_bot::{planner, storage::Store};
    let (_root, _path, mut config) = fixture()?;
    config.daily.max_accounts = 500;
    config.daily.max_windows_per_tick = 3;
    config.daily.reconciliation_days = 0;
    let prototype = config.daily.accounts[0].clone();
    config.daily.accounts = (0..500)
        .map(|i| {
            let mut w = prototype.clone();
            w.key = format!("synthetic_{i}");
            w.user_id = Some(format!("{}", 1000 + i));
            w.handle = Some(format!("account_{i}"));
            w
        })
        .collect();
    config.validate()?;
    let date = "2024-11-01".parse()?;
    assert_eq!(
        planner::preview_backfill(&config, date, date, clock())?
            .windows
            .len(),
        500
    );
    let store = Store::open(&config.state_dir)?;
    assert_eq!(store.daily_tick(&config, clock())?.created, 3);
    assert_eq!(store.daily_tick(&config, clock())?.created, 3);
    let accounts = store
        .daily_status(10, None, clock())?
        .into_iter()
        .map(|w| w.plan.account.id())
        .collect::<Result<std::collections::BTreeSet<_>>>()?;
    assert_eq!(accounts.len(), 6);
    assert!(store.daily_status(0, None, clock()).is_err());
    // A skipped IANA civil date must be explicit, never a fabricated 24h day.
    assert!(planner::local_day_bounds("Pacific/Apia", "2011-12-30".parse()?).is_err());
    Ok(())
}

#[test]
fn da02_public_retry_preserves_progress_and_refuses_active_or_completed_windows() -> Result<()> {
    use telegram_archive_bot::{planner::DayState, storage::Store};
    let (_root, path, mut config) = fixture()?;
    config.daily.max_attempts = 1;
    fs::write(&path, toml::to_string(&config)?)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    let store = Store::open(&config.state_dir)?;
    let date = "2024-11-01".parse()?;
    let id = store.backfill_daily(&config, date, date, now)?.windows[0]
        .id
        .clone();
    let first = store.claim_daily_window(&config, now)?.unwrap();
    assert!(cli(&path, &["daily-retry", &id]).is_err());
    store.checkpoint_daily(
        &first,
        "preserved-cursor",
        &serde_json::json!({"raw":"synthetic-page"}),
        now,
    )?;
    store.finish_daily_window(
        &first,
        progress(DayState::Failed, Some("preserved-cursor")),
        now,
    )?;
    assert!(store.claim_daily_window(&config, now + 2)?.is_none());
    let retried = cli(&path, &["daily-retry", &id])?;
    assert_eq!(retried["state"], "planned");
    assert_eq!(retried["failures"], 0);
    assert_eq!(retried["attempts"], 1);
    assert_eq!(retried["cursor"], "preserved-cursor");
    assert!(
        store
            .finish_daily_window(&first, progress(DayState::Complete, None), now + 2)
            .is_err()
    );
    let next = store.claim_daily_window(&config, now + 2)?.unwrap();
    assert_eq!(next.window.cursor.as_deref(), Some("preserved-cursor"));
    assert_eq!(
        rusqlite::Connection::open(config.state_dir.join("archive.sqlite"))?.query_row(
            "SELECT count(*) FROM daily_pages",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        1
    );
    store.finish_daily_window(&next, progress(DayState::Complete, None), now + 2)?;
    assert!(cli(&path, &["daily-retry", &id]).is_err());
    Ok(())
}

#[test]
fn da02_typed_watchlist_accepts_disabled_provider() {
    let parsed = toml::from_str::<Config>(
        r#"
[daily]
enabled = true
timezone = "America/Los_Angeles"
[[daily.accounts]]
key = "synthetic-alice"
provider = "x"
user_id = "101"
handle = "alice"
enabled = true
[daily.provider]
kind = "disabled"
max_pages = 20
page_size = 40
"#,
    );
    assert!(parsed.is_ok(), "DA02 typed daily configuration is missing");
    parsed.unwrap().validate().unwrap();
}
