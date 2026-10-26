//! Actual service process, synthetic ntn process, no provider or Telegram requests.
#![cfg(feature = "ntn")]
use serde_json::{Value, json};
use std::{
    fs::Permissions,
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use telegram_archive_bot::{config::*, storage::Store};

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
fn wait(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "durable service condition timed out"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}
fn write_config(path: &Path, config: &Config) {
    let pending = path.with_extension("new");
    std::fs::write(&pending, toml::to_string(config).unwrap()).unwrap();
    std::fs::rename(pending, path).unwrap();
}
fn signal(service: &Service, signal: i32) {
    assert_eq!(unsafe { libc::kill(service.0.id() as i32, signal) }, 0);
}
fn cli(path: &Path, args: &[&str]) -> Value {
    let out = Command::new(env!("CARGO_BIN_EXE_telegram-archive-bot"))
        .arg("--config")
        .arg(path)
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).unwrap()
}
fn replay(root: &Path, config: &Path, id: i64) {
    let path = root.join("updates.json");
    std::fs::write(
        &path,
        serde_json::to_vec(&json!([{
            "update_id":id,"message":{"message_id":id,"chat":{"id":2},"from":{"id":3},
            "text":format!("Synthetic retained source {id}. 🌍")}
        }]))
        .unwrap(),
    )
    .unwrap();
    cli(config, &["replay", path.to_str().unwrap()]);
}

// #101: assert the actual service's private log after durable settlement, so the
// baseline fails for a missing diagnostic rather than waiting for that new log.
fn limit_service_case(mode: &str, expected_reason: Option<&str>) {
    use telegram_archive_bot::{
        notion::{Provenance, Record, RecordData},
        sources::FetchStatus,
    };
    let root = tempfile::Builder::new()
        .permissions(Permissions::from_mode(0o700))
        .tempdir()
        .unwrap();
    let executable = root.path().join("ntn.py");
    std::fs::write(&executable, include_str!("fixtures/fake_ntn_worker.py")).unwrap();
    std::fs::set_permissions(&executable, Permissions::from_mode(0o700)).unwrap();
    let token = root.path().join("token");
    std::fs::write(&token, "SYNTHETIC_NOTION_WORKER_TOKEN").unwrap();
    std::fs::set_permissions(&token, Permissions::from_mode(0o600)).unwrap();
    let mut config = Config {
        state_dir: root.path().join("store"),
        ..Config::default()
    };
    config.telegram.enabled = false;
    config.daily.enabled = false;
    config.daily.capture.enabled = false;
    config.failure_reporting.enabled = false;
    config.model.enabled = false;
    config.translation.enabled = false;
    config.synthesis.enabled = false;
    config.notion.executable = Some(executable);
    config.notion.token = Some(SecretRef::File(token));
    config.notion.destination = Some(NotionDestination::Page {
        id: "11111111-1111-1111-1111-111111111111".into(),
    });
    config.notion.retry_seconds = 3600;
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
        *path = "/nonexistent/issue101-fixture".into();
    }
    let store = Store::open(&config.state_dir).unwrap();
    store
        .record_notion_document(&Record {
            schema_version: 1,
            id: "PRIVATE_RECORD_ID".into(),
            title: "PRIVATE_TITLE".into(),
            captured_at: "2026-10-26T00:00:00Z".into(),
            original_urls: vec!["https://example.org/PRIVATE_SOURCE_URI".into()],
            provenance: Provenance {
                provider: "fixture".into(),
                extractor: "synthetic-v1".into(),
                retrieved_at: "2026-10-26T00:00:00Z".into(),
                completeness: FetchStatus::Complete,
                original_bytes_sha256: None,
            },
            data: RecordData::AgentOutput {
                manifest_id: "PRIVATE_MANIFEST".into(),
                sop_version: "PRIVATE_SOP".into(),
                level: 1,
                sources: vec![],
            },
            content: "PRIVATE_CAPTURED_BODY".into(),
        })
        .unwrap();
    let conn = rusqlite::Connection::open(config.state_dir.join("archive.sqlite")).unwrap();
    let id: String = conn
        .query_row("SELECT id FROM notion_queue", [], |r| r.get(0))
        .unwrap();
    let config_path = root.path().join("config.toml");
    let launch = |log_path: &Path| {
        Service(
            Command::new(env!("CARGO_BIN_EXE_telegram-archive-bot"))
                .arg("--config")
                .arg(&config_path)
                .arg("run")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(std::fs::File::create(log_path).unwrap())
                .process_group(0)
                .spawn()
                .unwrap(),
        )
    };
    let settled = |attempt: i64| {
        conn.query_row(
        "SELECT count(*) FROM notion_attempts WHERE queue_id=?1 AND number=?2 AND finished_at IS NOT NULL",
        rusqlite::params![id, attempt], |r| r.get::<_, i64>(0),
    ).unwrap() == 1
    };
    std::fs::write(root.path().join("mode"), "accepted_lost").unwrap();
    write_config(&config_path, &config);
    let seed_log = root.path().join("seed.log");
    let mut service = launch(&seed_log);
    wait(|| {
        assert!(
            service.0.try_wait().unwrap().is_none(),
            "{}",
            std::fs::read_to_string(&seed_log).unwrap()
        );
        settled(1)
    });
    signal(&service, libc::SIGTERM);
    wait(|| service.0.try_wait().unwrap().is_some());
    assert!(service.0.wait().unwrap().success());
    let calls = || {
        std::fs::read_to_string(root.path().join("calls"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Vec<String>>(line).unwrap())
            .collect::<Vec<_>>()
    };
    let before_calls = calls();
    let create_count = |calls: &[Vec<String>]| {
        calls
            .iter()
            .filter(|a| a.get(1).is_some_and(|p| p == "v1/pages") && a.iter().any(|v| v == "POST"))
            .count()
    };
    assert_eq!(create_count(&before_calls), 1);
    store.reconcile_notion(&id, None).unwrap();
    config.notion.max_requests = 4;
    config.notion.response_bytes = 16384;
    std::fs::write(root.path().join("mode"), mode).unwrap();
    write_config(&config_path, &config);
    let log_path = root.path().join("diagnostic.log");
    service = launch(&log_path);
    wait(|| {
        assert!(
            service.0.try_wait().unwrap().is_none(),
            "{}",
            std::fs::read_to_string(&log_path).unwrap()
        );
        settled(2)
    });
    signal(&service, libc::SIGTERM);
    wait(|| service.0.try_wait().unwrap().is_some());
    assert!(service.0.wait().unwrap().success());
    let (state, may_remote, creates, error): (String, bool, i64, String) = conn
        .query_row(
            "SELECT state,may_have_remote,create_attempts,error FROM notion_queue WHERE id=?1",
            [&id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
    assert!(may_remote);
    assert_eq!(creates, 1);
    assert_eq!(
        (state.as_str(), error.as_str()),
        if expected_reason.is_some() {
            ("reconcile", "limit")
        } else {
            ("blocked_auth", "blocked_auth")
        }
    );
    let after_calls = calls();
    assert_eq!(
        create_count(&after_calls),
        1,
        "unknown create must never be retried"
    );
    let new_calls = &after_calls[before_calls.len()..];
    assert_eq!(
        new_calls
            .iter()
            .filter(|a| a.as_slice() == ["--version"])
            .count(),
        1
    );
    assert_eq!(
        new_calls
            .iter()
            .filter(|a| a.get(1).is_some_and(|p| p == "v1/search"))
            .count(),
        if mode == "search_pages" { 3 } else { 1 }
    );
    assert_eq!(new_calls.len(), if mode == "search_pages" { 4 } else { 2 });
    let logs: Vec<Value> = std::fs::read_to_string(&log_path)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|v| v["event"] == "notion_limit")
        .collect();
    let Some(reason) = expected_reason else {
        assert!(
            logs.is_empty(),
            "parsed remote auth error must retain precedence"
        );
        return;
    };
    assert_eq!(
        logs.len(),
        1,
        "settled real limit must emit one private reason"
    );
    let detail = &logs[0]["detail"];
    assert_eq!(detail["reason"], reason);
    assert_eq!(
        detail["requests_used"],
        if mode == "search_pages" { 4 } else { 2 }
    );
    assert_eq!(detail["max_requests"], 4);
    let serialized = serde_json::to_string(&logs).unwrap();
    for forbidden in [
        "PRIVATE_",
        "SYNTHETIC_NOTION_WORKER_TOKEN",
        "https://",
        "query",
        "cursor",
        "page_id",
        "record_id",
        root.path().to_str().unwrap(),
        &id,
    ] {
        assert!(
            !serialized.contains(forbidden),
            "private diagnostic leaked {forbidden}"
        );
    }
}

#[test]
fn issue101_request_budget_limit_is_private_and_keeps_unknown_create() {
    limit_service_case("search_pages", Some("request_budget"));
}

#[test]
fn issue101_stdout_limit_is_private() {
    limit_service_case("search_stdout_limit", Some("tool_stdout_or_stderr"));
}

#[test]
fn issue101_stderr_limit_is_private() {
    limit_service_case("search_stderr_limit", Some("tool_stdout_or_stderr"));
}

#[test]
fn issue101_work_limit_is_private() {
    limit_service_case("search_work_limit", Some("tool_workdir"));
}

#[test]
fn issue101_remote_auth_error_precedes_tool_limit() {
    limit_service_case("search_auth_overflow", None);
}

#[test]
fn headless_service_drains_on_readiness_change_without_sync_and_keeps_destination_pins() {
    let root = tempfile::Builder::new()
        .permissions(Permissions::from_mode(0o700))
        .tempdir()
        .unwrap();
    let executable = root.path().join("ntn.py");
    std::fs::write(&executable, include_str!("fixtures/fake_ntn_worker.py")).unwrap();
    std::fs::set_permissions(&executable, Permissions::from_mode(0o700)).unwrap();
    let token = root.path().join("token");
    std::fs::write(&token, "SYNTHETIC_NOTION_WORKER_TOKEN").unwrap();
    std::fs::set_permissions(&token, Permissions::from_mode(0o600)).unwrap();
    std::fs::write(root.path().join("mode"), "normal").unwrap();
    let mut config = Config {
        state_dir: root.path().join("store"),
        ..Config::default()
    };
    config.telegram.bot_id = 1;
    config.telegram.allowed_chats = vec![2];
    config.telegram.allowed_users = vec![3];
    config.notion.executable = Some(executable);
    config.notion.retry_seconds = 1;
    // Disable unrelated readiness probes in the service fixture.
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
        *path = "/nonexistent/da06-fixture".into();
    }
    let config_path = root.path().join("config.toml");
    write_config(&config_path, &config);
    let store = Store::open(&config.state_dir).unwrap();
    let log_path = root.path().join("service.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut service = Service(
        Command::new(env!("CARGO_BIN_EXE_telegram-archive-bot"))
            .arg("--config")
            .arg(&config_path)
            .arg("run")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .process_group(0)
            .spawn()
            .unwrap(),
    );
    wait(|| {
        let log = std::fs::read_to_string(&log_path).unwrap();
        assert!(
            service.0.try_wait().unwrap().is_none(),
            "headless service exited: {log}"
        );
        log.contains("service_started")
    });
    replay(root.path(), &config_path, 1);
    wait(|| store.notion_metrics().unwrap()["pending"] == 2);
    assert!(!root.path().join("calls").exists());
    config.notion.token = Some(SecretRef::File(token));
    config.notion.destination = Some(NotionDestination::Page {
        id: "11111111-1111-1111-1111-111111111111".into(),
    });
    write_config(&config_path, &config);
    signal(&service, libc::SIGHUP);
    wait(|| store.notion_metrics().unwrap()["states"]["complete"] == 2);
    let calls = std::fs::read(root.path().join("calls")).unwrap();
    config.notion.enabled = false;
    config.notion.destination = Some(NotionDestination::Page {
        id: "22222222-2222-2222-2222-222222222222".into(),
    });
    write_config(&config_path, &config);
    signal(&service, libc::SIGHUP);
    let conn = rusqlite::Connection::open(config.state_dir.join("archive.sqlite")).unwrap();
    wait(|| {
        conn.query_row("SELECT destination_key FROM notion_binding", [], |r| {
            r.get::<_, Option<String>>(0)
        })
        .unwrap()
        .is_some_and(|k| {
            Some(k)
                == config
                    .notion
                    .destination
                    .as_ref()
                    .and_then(NotionDestination::identity)
        })
    });
    replay(root.path(), &config_path, 2);
    wait(|| store.notion_metrics().unwrap()["pending"] == 2);
    assert_eq!(std::fs::read(root.path().join("calls")).unwrap(), calls);
    config.notion.enabled = true;
    write_config(&config_path, &config);
    signal(&service, libc::SIGHUP);
    wait(|| store.notion_metrics().unwrap()["states"]["complete"] == 4);
    let destinations: i64 = conn
        .query_row(
            "SELECT count(DISTINCT destination_key) FROM notion_queue",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(destinations, 2);
    let status = cli(&config_path, &["status"]);
    assert_eq!(status["notion_publication"]["pending"], 0);
    // A remote create is accepted before its response is delivered. Disabling must
    // cancel/settle it, and process restart must reconcile rather than create again.
    std::fs::remove_file(root.path().join("accepted")).unwrap();
    std::fs::write(root.path().join("mode"), "hold_after_create").unwrap();
    replay(root.path(), &config_path, 3);
    wait(|| root.path().join("accepted").exists());
    config.notion.enabled = false;
    write_config(&config_path, &config);
    signal(&service, libc::SIGHUP);
    wait(|| store.notion_metrics().unwrap()["states"]["reconcile"] == 1);
    let stopped_calls = std::fs::read(root.path().join("calls")).unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM notion_attempts WHERE finished_at IS NULL",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM notion_queue WHERE state='reconcile' AND may_have_remote=1",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
    signal(&service, libc::SIGTERM);
    wait(|| service.0.try_wait().unwrap().is_some());
    assert!(service.0.wait().unwrap().success());
    let log = std::fs::File::create(&log_path).unwrap();
    service = Service(
        Command::new(env!("CARGO_BIN_EXE_telegram-archive-bot"))
            .arg("--config")
            .arg(&config_path)
            .arg("run")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(log)
            .process_group(0)
            .spawn()
            .unwrap(),
    );
    wait(|| {
        assert!(service.0.try_wait().unwrap().is_none());
        std::fs::read_to_string(&log_path)
            .unwrap()
            .contains("service_started")
    });
    assert_eq!(
        std::fs::read(root.path().join("calls")).unwrap(),
        stopped_calls
    );
    std::fs::write(root.path().join("mode"), "normal").unwrap();
    config.notion.enabled = true;
    write_config(&config_path, &config);
    signal(&service, libc::SIGHUP);
    wait(|| store.notion_metrics().unwrap()["states"]["complete"] == 6);
    let remote: Value =
        serde_json::from_slice(&std::fs::read(root.path().join("remote.json")).unwrap()).unwrap();
    assert_eq!(
        remote["pages"].as_object().unwrap().len(),
        6,
        "restart must not duplicate an accepted create"
    );
    signal(&service, libc::SIGTERM);
    wait(|| service.0.try_wait().unwrap().is_some());
    assert!(service.0.wait().unwrap().success());
}
