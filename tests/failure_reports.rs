//! DA08 / #75: synthetic fixtures only; never invoke the installed gh.
use serde_json::{Value, json};
use std::{fs, os::unix::fs::PermissionsExt};
use telegram_archive_bot::{config::Config, storage::Store};
use telegram_archive_bot::{
    config::{FailureReporting, SecretRef},
    failure_report_worker::tick,
    failure_reporting::*,
};
use tokio_util::sync::CancellationToken;

struct Fixture {
    root: tempfile::TempDir,
    store: Store,
    config: FailureReporting,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let exe = root.path().join("gh.py");
        fs::write(&exe, include_str!("fixtures/fake_gh_reports.py")).unwrap();
        fs::set_permissions(&exe, fs::Permissions::from_mode(0o700)).unwrap();
        let token = root.path().join("token");
        fs::write(&token, "SYNTHETIC_GH_DA08_TOKEN").unwrap();
        fs::set_permissions(&token, fs::Permissions::from_mode(0o600)).unwrap();
        let store = Store::open(root.path().join("state")).unwrap();
        let config = FailureReporting {
            executable: Some(exe),
            token: Some(SecretRef::File(token)),
            threshold: 2,
            debounce_seconds: 2,
            update_seconds: 10,
            retry_seconds: 1,
            timeout_seconds: 2,
            ..FailureReporting::default()
        };
        Self {
            root,
            store,
            config,
        }
    }
    fn mode(&self, mode: &str) {
        fs::write(self.root.path().join("mode"), mode).unwrap();
    }
    fn event(&self, id: u64, stream: &str) -> FailureEvent {
        FailureEvent{event_id:format!("event-{id}"),stream_id:stream.into(),subsystem:Subsystem::Capture,provider:"HTTP".into(),operation:"extract".into(),error_class:ErrorClass::Extraction,provider_schema:"html-v1".into(),observed_at:100+id as i64,outcome:Outcome::Failed,window_id:Some(format!("private-window-{}",id%2)),source:Source::PublicUrl("https://example.com/article.pdf?id=7&token=SECRET_QUERY&X-Amz-Signature=SIGNED_PRIVATE#private".into()),public_excerpt:Some("Parser returned an empty article at parse.rs:17.\nAuthorization: Bearer SECRET_HEADER\nCookie: SECRET_COOKIE\nProvider said SECRET_DIAGNOSTIC\nUnrelated https://private.internal/protected?token=SECRET_LINK".into()),private_details:"PROTECTED_BODY PRIVATE_ACCOUNT private exact stack".into(),redactions:vec!["SECRET_DIAGNOSTIC".into()]}
    }
    fn record(&self, id: u64, stream: &str) {
        self.store.record_failure(&self.event(id, stream)).unwrap();
    }
    async fn tick(&self, time: i64) -> bool {
        let started = std::time::Instant::now();
        let result = tick(&self.store, &self.config, time, &CancellationToken::new()).await;
        eprintln!(
            "DA08_FIXTURE_TICK {}",
            json!({"time":time,"worked":result.as_ref().ok(),"elapsed_ms":started.elapsed().as_millis(),"calls":self.calls(),"status":self.status()})
        );
        result.unwrap()
    }
    async fn work_tick(&self, time: i64) -> usize {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let mut idle_polls = 0;
        while !self.tick(time).await {
            idle_polls += 1;
            assert!(
                std::time::Instant::now() < deadline,
                "expected a reporter work attempt within the bounded idle wait"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        idle_polls
    }
    fn remote(&self) -> Value {
        let p = self.root.path().join("remote.json");
        if p.exists() {
            serde_json::from_slice(&fs::read(p).unwrap()).unwrap()
        } else {
            json!({"issues":[]})
        }
    }
    fn calls(&self) -> Vec<Value> {
        fs::read_to_string(self.root.path().join("calls.jsonl"))
            .unwrap_or_default()
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect()
    }
    fn actions(&self, action: &str) -> usize {
        self.calls()
            .iter()
            .filter(|v| v["action"] == action)
            .count()
    }
    fn status(&self) -> Value {
        self.store.failure_report_status(20).unwrap()
    }
}

#[tokio::test]
async fn expected_work_waits_for_busy_lock_without_repeating_a_dispatched_attempt() {
    use std::os::unix::fs::OpenOptionsExt;
    let f = Fixture::new();
    f.record(1, "busy");
    f.record(2, "busy");
    let guard = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .open(f.store.root().join("failure-report.lock"))
        .unwrap();
    fs2::FileExt::try_lock_exclusive(&guard).unwrap();
    let (idle_polls, ()) = tokio::join!(f.work_tick(110), async {
        // Whichever branch is polled first, the first worker pass observes the held lock.
        tokio::task::yield_now().await;
        drop(guard);
    });
    assert_eq!(
        f.actions("create"),
        1,
        "expected work must survive a busy pass"
    );
    assert!(idle_polls > 0, "the fixture must exercise lock contention");
    assert_eq!(f.status()["reports"][0]["state"], "idle");
    assert_eq!(
        f.calls()
            .iter()
            .map(|c| c["action"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["list", "create", "list", "view"],
        "a completed worker attempt must never be repeated by fixture polling"
    );
}

#[test]
fn reporting_is_explicitly_authorized_by_default_and_has_durable_accounting() {
    let config = serde_json::to_value(Config::default()).unwrap();
    assert_eq!(config["failure_reporting"]["enabled"], true);
    assert_eq!(
        config["failure_reporting"]["repository"],
        "RyderFreeman4Logos/telegram-archive-bot"
    );
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path().join("state")).unwrap();
    let conn = rusqlite::Connection::open(store.root().join("archive.sqlite")).unwrap();
    let tables: i64 = conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('failure_events','failure_incidents','failure_reports','failure_report_attempts')",
        [], |r| r.get(0),
    ).unwrap();
    assert_eq!(tables, 4);
}

#[test]
fn event_identity_fingerprint_and_independent_recovery_survive_reopen() {
    let f = Fixture::new();
    let a = f.event(1, "first");
    let mut b = f.event(2, "second");
    b.provider = "http".into();
    b.source = Source::Private("private-user-list".into());
    assert_eq!(a.fingerprint().unwrap(), b.fingerprint().unwrap());
    f.store.record_failure(&a).unwrap();
    f.store.record_failure(&a).unwrap();
    f.store.record_failure(&b).unwrap();
    let mut c = f.event(3, "first");
    c.outcome = Outcome::Recovered;
    f.store.record_failure(&c).unwrap();
    let reopened = Store::open(f.store.root()).unwrap();
    assert_eq!(reopened.failure_report_status(10).unwrap()["events"], 3);
    let conn = rusqlite::Connection::open(f.store.root().join("archive.sqlite")).unwrap();
    let v: (i64, i64, String) = conn
        .query_row(
            "SELECT occurrences,consecutive,state FROM failure_incidents",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(v, (2, 1, "active".into()));
    let mut collision = a;
    collision.private_details = "different".into();
    assert!(f.store.record_failure(&collision).is_err());
    let mut recursive = f.event(4, "reporter");
    recursive.subsystem = Subsystem::Reporter;
    assert!(f.store.record_failure(&recursive).unwrap().is_none());
    assert_eq!(f.status()["events"], 3);
}

#[tokio::test]
async fn threshold_debounce_public_diagnostics_and_private_boundary() {
    let f = Fixture::new();
    f.record(1, "article");
    f.tick(110).await;
    assert!(f.calls().is_empty());
    f.record(2, "article");
    f.tick(102).await;
    assert!(f.calls().is_empty());
    f.work_tick(103).await;
    assert_eq!(f.actions("create"), 1);
    assert_eq!(f.status()["reports"][0]["state"], "idle");
    let body = f.remote()["issues"][0]["body"].as_str().unwrap().to_owned();
    assert!(body.contains("https://example.com/article.pdf?id=7"));
    assert!(body.contains("Parser returned an empty article at parse.rs:17."));
    assert!(body.contains("Occurrences: 2") && body.contains("Affected windows: 2"));
    for secret in [
        "SECRET_QUERY",
        "SIGNED_PRIVATE",
        "SECRET_HEADER",
        "SECRET_COOKIE",
        "SECRET_DIAGNOSTIC",
        "SECRET_LINK",
        "PROTECTED_BODY",
        "PRIVATE_ACCOUNT",
        "private-window",
        "private.internal",
    ] {
        assert!(!body.contains(secret), "leaked {secret}");
    }
    let mut private = f.event(3, "private");
    private.source = Source::Private("PRIVATE_ACCOUNT protected original URL".into());
    private.public_excerpt = Some("PROTECTED_BODY".into());
    f.store.record_failure(&private).unwrap();
    f.work_tick(114).await;
    let remote = f.remote();
    let comment = remote["issues"][0]["comments"][0]["body"].as_str().unwrap();
    assert!(comment.contains("Private incident:"));
    assert!(!comment.contains("PRIVATE_ACCOUNT") && !comment.contains("PROTECTED_BODY"));
}

#[tokio::test]
async fn configured_credential_cannot_leak_from_a_public_exception() {
    let f = Fixture::new();
    let mut event = f.event(1, "article");
    event.public_excerpt =
        Some("Transport returned SYNTHETIC_GH_DA08_TOKEN at request.rs:18".into());
    f.store.record_failure(&event).unwrap();
    event.event_id = "second".into();
    f.store.record_failure(&event).unwrap();
    f.work_tick(110).await;
    assert_eq!(
        f.remote()["issues"].as_array().unwrap().len(),
        1,
        "public body must scrub the explicitly configured credential before gh dispatch"
    );
    assert!(!f.remote().to_string().contains("SYNTHETIC_GH_DA08_TOKEN"));
}

#[tokio::test]
async fn workers_share_one_issue_cadence_and_reopen_closed_recurrences() {
    let f = Fixture::new();
    f.record(1, "a");
    f.record(2, "b");
    let other = Store::open(f.store.root()).unwrap();
    let cancel = CancellationToken::new();
    let (a, b) = tokio::join!(
        tick(&f.store, &f.config, 110, &cancel),
        tick(&other, &f.config, 110, &cancel)
    );
    a.unwrap();
    b.unwrap();
    assert_eq!(f.actions("create"), 1);
    f.record(3, "b");
    let calls = f.calls().len();
    f.tick(111).await;
    assert_eq!(f.calls().len(), calls);
    f.work_tick(120).await;
    assert_eq!(f.actions("comment"), 1);
    assert!(
        f.remote()["issues"][0]["comments"][0]["body"]
            .as_str()
            .unwrap()
            .contains("Occurrences: 3")
    );
    let mut remote = f.remote();
    remote["issues"][0]["state"] = json!("CLOSED");
    fs::write(
        f.root.path().join("remote.json"),
        serde_json::to_vec(&remote).unwrap(),
    )
    .unwrap();
    f.record(4, "a");
    f.work_tick(130).await;
    assert_eq!(f.actions("reopen"), 1);
    f.work_tick(131).await;
    assert_eq!(f.actions("comment"), 2);
    assert_eq!(f.actions("create"), 1);
    assert_eq!(f.remote()["issues"][0]["state"], "OPEN");
}

#[tokio::test]
async fn lost_create_and_comment_responses_reconcile_without_repeating_writes() {
    let mut f = Fixture::new();
    f.config.timeout_seconds = 1;
    f.record(1, "a");
    f.record(2, "a");
    f.mode("hold_create");
    f.work_tick(110).await;
    assert_eq!(f.actions("create"), 1);
    assert_eq!(f.status()["reports"][0]["state"], "reconcile");
    let pid: i32 = fs::read_to_string(f.root.path().join("accepted"))
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        unsafe { libc::kill(pid, 0) },
        -1,
        "owned fixture descendant survived timeout"
    );
    f.store = Store::open(f.store.root()).unwrap();
    f.mode("hidden");
    f.work_tick(112).await;
    assert_eq!(f.actions("create"), 1);
    assert_eq!(f.status()["reports"][0]["state"], "reconcile");
    f.mode("normal");
    f.work_tick(114).await;
    assert_eq!(f.status()["reports"][0]["state"], "idle");
    assert_eq!(f.actions("create"), 1);
    f.record(3, "a");
    f.mode("hold_comment");
    f.work_tick(124).await;
    assert_eq!(f.actions("comment"), 1);
    assert_eq!(f.status()["reports"][0]["state"], "reconcile");
    f.mode("normal");
    f.work_tick(126).await;
    assert_eq!(f.status()["reports"][0]["state"], "idle");
    assert_eq!(f.actions("comment"), 1);
}

#[tokio::test]
async fn disabled_missing_auth_expected_gaps_and_transient_recovery_never_create() {
    let mut f = Fixture::new();
    f.record(1, "a");
    f.record(2, "a");
    f.config.enabled = false;
    f.tick(110).await;
    assert!(f.calls().is_empty());
    f.config.enabled = true;
    let token = f.config.token.take();
    f.tick(112).await;
    assert!(f.calls().is_empty());
    assert_eq!(f.status()["reports"][0]["state"], "pending_configuration");
    f.config.token = token;
    f.mode("auth");
    f.work_tick(114).await;
    assert_eq!(f.actions("create"), 0);
    assert_eq!(f.status()["reports"][0]["state"], "pending_configuration");
    f.mode("normal");
    f.work_tick(116).await;
    assert_eq!(f.actions("create"), 1);
    let gaps = Fixture::new();
    for i in 1..8 {
        let mut event = gaps.event(i, "limited");
        event.error_class = ErrorClass::RateLimited;
        gaps.store.record_failure(&event).unwrap();
    }
    let mut recovered = gaps.event(8, "limited");
    recovered.error_class = ErrorClass::RateLimited;
    recovered.outcome = Outcome::Recovered;
    gaps.store.record_failure(&recovered).unwrap();
    gaps.record(9, "temporary");
    let mut recovered = gaps.event(10, "temporary");
    recovered.outcome = Outcome::Recovered;
    gaps.store.record_failure(&recovered).unwrap();
    gaps.tick(200).await;
    assert!(gaps.calls().is_empty());
    assert_eq!(gaps.status()["events"], 10);
}

#[tokio::test]
async fn existing_issue_is_reused_but_ambiguous_or_changed_remote_evidence_fails_closed() {
    let f = Fixture::new();
    f.record(1, "a");
    f.record(2, "a");
    let marker = format!(
        "<!-- da08-fingerprint:{} -->",
        f.event(1, "a").fingerprint().unwrap()
    );
    let issue = json!({"number":7,"url":"https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/7","state":"OPEN","body":format!("{marker}\nExisting operator report"),"comments":[]});
    fs::write(
        f.root.path().join("remote.json"),
        serde_json::to_vec(&json!({"issues":[issue]})).unwrap(),
    )
    .unwrap();
    f.work_tick(110).await;
    assert_eq!(f.actions("create"), 0);
    assert_eq!(f.actions("comment"), 1);
    let g = Fixture::new();
    g.record(1, "a");
    g.record(2, "a");
    let mut second = issue.clone();
    second["number"] = json!(8);
    second["url"] = json!("https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/8");
    fs::write(
        g.root.path().join("remote.json"),
        serde_json::to_vec(&json!({"issues":[issue,second]})).unwrap(),
    )
    .unwrap();
    g.work_tick(110).await;
    assert_eq!(g.actions("create"), 0);
    assert_eq!(g.actions("comment"), 0);
    assert_eq!(g.status()["reports"][0]["error"], "ambiguous_issue_search");
    let bad = Fixture::new();
    bad.record(1, "a");
    bad.record(2, "a");
    bad.mode("wrong_body");
    bad.work_tick(110).await;
    assert_eq!(bad.status()["reports"][0]["state"], "reconcile");
    assert_eq!(bad.actions("create"), 1);
    bad.work_tick(112).await;
    assert_eq!(bad.actions("create"), 1);
}

#[tokio::test]
async fn full_backup_retains_private_events_and_reconciles_after_recovery_hold() {
    let mut f = Fixture::new();
    f.record(1, "a");
    f.record(2, "a");
    f.config.enabled = false;
    f.tick(110).await;
    let backup = f.root.path().join("backup");
    f.store.backup(&backup).unwrap();
    f.config.enabled = true;
    f.work_tick(112).await;
    assert_eq!(f.actions("create"), 1);
    let restored = f.root.path().join("restored");
    Store::restore_backup(&backup, &restored).unwrap();
    let store = Store::open(&restored).unwrap();
    let count = f.calls().len();
    tick(&store, &f.config, 114, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(f.calls().len(), count);
    assert_eq!(store.failure_report_status(10).unwrap()["events"], 2);
    store.release_recovery_hold().unwrap();
    tick(&store, &f.config, 116, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(f.actions("create"), 1);
    assert_eq!(
        store.failure_report_status(10).unwrap()["reports"][0]["issue_number"],
        1
    );
    let conn = rusqlite::Connection::open(restored.join("archive.sqlite")).unwrap();
    let record: String = conn
        .query_row(
            "SELECT private_record FROM failure_events LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(record.contains("PROTECTED_BODY"));
}

#[test]
fn actual_headless_service_starts_reporter_and_cancels_on_disable() {
    use std::{
        os::unix::process::CommandExt,
        process::{Child, Command, Stdio},
        time::{Duration, Instant},
    };
    struct Service(Child);
    impl Drop for Service {
        fn drop(&mut self) {
            if self.0.try_wait().ok().flatten().is_none() {
                unsafe {
                    libc::kill(-(self.0.id() as i32), libc::SIGKILL);
                };
                let _ = self.0.wait();
            }
        }
    }
    fn wait(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while !condition() {
            assert!(Instant::now() < deadline, "service condition timed out");
            std::thread::sleep(Duration::from_millis(25));
        }
    }
    fn write(path: &std::path::Path, config: &Config) {
        let pending = path.with_extension("new");
        fs::write(&pending, toml::to_string(config).unwrap()).unwrap();
        fs::rename(pending, path).unwrap();
    }
    let f = Fixture::new();
    f.record(1, "a");
    f.record(2, "a");
    let mut config = Config {
        state_dir: f.store.root().to_owned(),
        failure_reporting: f.config.clone(),
        ..Config::default()
    };
    config.notion.enabled = false;
    config.failure_reporting.enabled = false;
    config.failure_reporting.update_seconds = 1;
    config.failure_reporting.timeout_seconds = 30;
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
        *path = "/nonexistent/da08-fixture".into();
    }
    let path = f.root.path().join("config.toml");
    write(&path, &config);
    let log_path = f.root.path().join("service.log");
    let start = || {
        Service(
            Command::new(env!("CARGO_BIN_EXE_telegram-archive-bot"))
                .args(["--config", path.to_str().unwrap(), "run"])
                .env_clear()
                .env("PATH", "/usr/bin:/bin")
                .env("HOME", f.root.path())
                .env("TMPDIR", f.root.path())
                .env("DA08_PRIVATE_SEED", "PROTECTED_ACCOUNT_LIST")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(
                    fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&log_path)
                        .unwrap(),
                )
                .process_group(0)
                .spawn()
                .unwrap(),
        )
    };
    let mut service = start();
    wait(|| {
        assert!(
            service.0.try_wait().unwrap().is_none(),
            "{}",
            fs::read_to_string(&log_path).unwrap()
        );
        fs::read_to_string(&log_path)
            .unwrap()
            .contains("service_started")
    });
    assert!(f.calls().is_empty());
    config.failure_reporting.enabled = true;
    write(&path, &config);
    assert_eq!(
        unsafe { libc::kill(service.0.id() as i32, libc::SIGHUP) },
        0
    );
    wait(|| f.status()["reports"][0]["state"] == "idle");
    assert_eq!(f.actions("create"), 1);
    let mut second = f.event(3, "b");
    second.operation = "download".into();
    f.store.record_failure(&second).unwrap();
    second.event_id = "fourth".into();
    f.store.record_failure(&second).unwrap();
    f.mode("hold_create");
    wait(|| f.root.path().join("accepted").exists());
    config.failure_reporting.enabled = false;
    write(&path, &config);
    assert_eq!(
        unsafe { libc::kill(service.0.id() as i32, libc::SIGHUP) },
        0
    );
    wait(|| {
        f.status()["reports"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["state"] == "reconcile")
    });
    let calls = f.calls().len();
    std::thread::sleep(Duration::from_millis(1100));
    assert_eq!(f.calls().len(), calls);
    assert_eq!(
        unsafe { libc::kill(service.0.id() as i32, libc::SIGTERM) },
        0
    );
    wait(|| service.0.try_wait().unwrap().is_some());
    assert!(service.0.wait().unwrap().success());
    f.mode("normal");
    config.failure_reporting.enabled = true;
    write(&path, &config);
    let mut restarted = start();
    wait(|| {
        f.status()["reports"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["state"] == "idle")
    });
    assert_eq!(f.actions("create"), 2);
    assert_eq!(
        unsafe { libc::kill(restarted.0.id() as i32, libc::SIGTERM) },
        0
    );
    wait(|| restarted.0.try_wait().unwrap().is_some());
    assert!(restarted.0.wait().unwrap().success());
    let logs = fs::read_to_string(&log_path).unwrap();
    for secret in [
        "SYNTHETIC_GH_DA08_TOKEN",
        "PROTECTED_ACCOUNT_LIST",
        "PROTECTED_BODY",
        "SECRET_QUERY",
    ] {
        assert!(!logs.contains(secret), "log leaked {secret}");
    }
}

#[tokio::test]
async fn long_valid_samples_keep_bounded_reports_and_complete_occurrence_counts() {
    let f = Fixture::new();
    for id in 1..=6 {
        let mut event = f.event(id, "long");
        event.source = Source::PublicUrl(format!("https://example.com/{}/{id}", "a".repeat(3900)));
        event.public_excerpt = Some(format!("{} {}", "🌍".repeat(256), "🌏".repeat(256)));
        f.store.record_failure(&event).unwrap();
    }
    f.work_tick(120).await;
    assert_eq!(
        f.remote()["issues"].as_array().unwrap().len(),
        1,
        "bounded valid samples must not prevent reporting the whole incident"
    );
    let remote = f.remote();
    let body = remote["issues"][0]["body"].as_str().unwrap();
    assert!(
        body.len() <= 32768
            && body.contains("Occurrences: 6")
            && body.contains("https://example.com/")
            && body.contains("omitted")
    );
}

#[test]
fn actual_schema_fifteen_upgrade_preserves_capture_and_notion_intents() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("state");
    fs::create_dir(&state).unwrap();
    fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).unwrap();
    let conn = rusqlite::Connection::open(state.join("archive.sqlite")).unwrap();
    let mut migrations =
        fs::read_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| {
                p.file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .split('_')
                    .next()
                    .unwrap()
                    .parse::<u32>()
                    .is_ok_and(|v| v <= 15)
            })
            .collect::<Vec<_>>();
    migrations.sort();
    assert_eq!(migrations.len(), 15);
    for path in migrations {
        conn.execute_batch(&fs::read_to_string(path).unwrap())
            .unwrap();
    }
    conn.execute_batch("INSERT INTO sources(id,provider,external_id) VALUES('fixture-source','x','100'); INSERT INTO observations(source_id,observed_at,status,extractor,evidence) VALUES('fixture-source',100,'unavailable','migration-fixture','{\"retained\":true}'); PRAGMA user_version=15;").unwrap();
    let before: String = conn
        .query_row("SELECT id||':'||state FROM notion_queue", [], |r| r.get(0))
        .unwrap();
    drop(conn);
    let store = Store::open(&state).unwrap();
    let conn = rusqlite::Connection::open(store.root().join("archive.sqlite")).unwrap();
    assert_eq!(
        conn.query_row("SELECT evidence FROM observations", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "{\"retained\":true}"
    );
    assert_eq!(
        conn.query_row("SELECT id||':'||state FROM notion_queue", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        before
    );
    assert_eq!(store.failure_report_status(10).unwrap()["events"], 0);
    assert!(store.integrity().is_ok());
}
