//! DA06 / #73: local durable queue and synthetic ntn/service lifecycle only.
use serde_json::json;
use std::{fs::Permissions, os::unix::fs::PermissionsExt};
use telegram_archive_bot::{sources::*, storage::Store};

fn root() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}
fn capture(id: &str, body: &str) -> Capture {
    Capture {
        identity: Identity::x(id).unwrap(),
        submitted_url: Some(format!("https://x.com/fixture/status/{id}")),
        content: Some(Content {
            body: body.into(),
            metadata: ContentMetadata::default(),
        }),
        status: FetchStatus::Complete,
        extractor: "da06-fixture".into(),
        evidence: json!({"fixture":true}),
        raw_digest: None,
        relations: vec![],
    }
}

#[test]
fn canonical_capture_commits_notion_intents_atomically() {
    let root = root();
    let store = Store::open(root.path()).unwrap();
    store.capture(&capture("100", "captured source")).unwrap();
    let conn = rusqlite::Connection::open(root.path().join("archive.sqlite")).unwrap();
    let present: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='notion_queue')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        present,
        "successful canonical intake must durably enqueue Notion work"
    );
    let count = || {
        conn.query_row("SELECT count(*) FROM notion_queue", [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap()
    };
    assert_eq!(count(), 2); // one immutable revision and one source observation
    store.capture(&capture("100", "captured source")).unwrap();
    assert_eq!(count(), 3); // identical revision is not duplicated
    let before = store.source_counts().unwrap();
    conn.execute_batch("CREATE TRIGGER fixture_notion_failure BEFORE INSERT ON notion_queue BEGIN SELECT RAISE(ABORT,'synthetic queue failure'); END;").unwrap();
    assert!(store.capture(&capture("200", "must roll back")).is_err());
    assert_eq!(store.source_counts().unwrap(), before);
    assert_eq!(count(), 3);
}

#[test]
fn preexisting_corpus_bootstrap_is_bounded_and_resumes_after_reopen() {
    let root = root();
    let store = Store::open(root.path()).unwrap();
    for id in ["100", "200", "300"] {
        store.capture(&capture(id, "original")).unwrap();
    }
    let conn = rusqlite::Connection::open(root.path().join("archive.sqlite")).unwrap();
    // Model existing pre-015 content: canonical records survive, no publication intents yet.
    conn.execute("DELETE FROM notion_queue", []).unwrap();
    assert_eq!(store.notion_bootstrap(1).unwrap(), 1);
    let count = || {
        conn.query_row("SELECT count(*) FROM notion_queue", [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap()
    };
    assert_eq!(count(), 2);
    drop(store);
    let store = Store::open(root.path()).unwrap();
    for _ in 0..8 {
        assert!(store.notion_bootstrap(1).unwrap() <= 1);
    }
    assert_eq!(count(), 6);
    assert_eq!(store.notion_bootstrap(1).unwrap(), 0);
    assert_eq!(count(), 6);
}

fn worker_fixture() -> (
    tempfile::TempDir,
    Store,
    telegram_archive_bot::config::Config,
) {
    use telegram_archive_bot::config::{Config, Notion, NotionDestination, SecretRef};
    let root = root();
    let executable = root.path().join("ntn.py");
    std::fs::write(&executable, include_str!("fixtures/fake_ntn_worker.py")).unwrap();
    std::fs::set_permissions(&executable, Permissions::from_mode(0o700)).unwrap();
    let token = root.path().join("token");
    std::fs::write(&token, "SYNTHETIC_NOTION_WORKER_TOKEN").unwrap();
    std::fs::set_permissions(&token, Permissions::from_mode(0o600)).unwrap();
    std::fs::write(root.path().join("mode"), "normal").unwrap();
    let config = Config {
        state_dir: root.path().join("store"),
        notion: Notion {
            executable: Some(executable),
            token: Some(SecretRef::File(token)),
            destination: Some(NotionDestination::Page {
                id: "11111111-1111-1111-1111-111111111111".into(),
            }),
            retry_seconds: 1,
            ..Notion::default()
        },
        ..Config::default()
    };
    let store = Store::open(&config.state_dir).unwrap();
    (root, store, config)
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn notion_settlement_records_private_failure_and_recovery_without_gh() {
    let (root, store, config) = worker_fixture();
    store
        .capture(&capture("888", "PRIVATE_NOTION_BODY"))
        .unwrap();
    std::fs::write(root.path().join("mode"), "accepted_lost").unwrap();
    telegram_archive_bot::notion_worker::tick(
        &store,
        &config,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    let conn = rusqlite::Connection::open(store.root().join("archive.sqlite")).unwrap();
    let failures: i64 = conn
        .query_row(
            "SELECT count(*) FROM failure_events WHERE outcome='failed'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(failures, 1);
    let sample: String = conn
        .query_row("SELECT public_sample FROM failure_events", [], |r| r.get(0))
        .unwrap();
    assert!(!sample.contains("PRIVATE_NOTION_BODY") && !sample.contains("x.com"));
    assert_eq!(
        store.failure_report_status(20).unwrap()["reports"],
        serde_json::json!([])
    );
    std::fs::write(root.path().join("mode"), "normal").unwrap();
    conn.execute("UPDATE notion_queue SET available_at=0", [])
        .unwrap();
    drain_until(&store, &config, |m| m["states"]["complete"] == 2).await;
    let recovered: i64 = conn
        .query_row(
            "SELECT count(*) FROM failure_events WHERE outcome='recovered'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(recovered, 1);
    assert_eq!(
        conn.query_row("SELECT consecutive FROM failure_incidents", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn retrying_the_same_invalid_notion_record_keeps_accounting_idempotent() {
    let (_root, store, mut config) = worker_fixture();
    store
        .capture(&capture("889", "Private canonical content"))
        .unwrap();
    let conn = rusqlite::Connection::open(store.root().join("archive.sqlite")).unwrap();
    // One fixture intent isolates repeated no-dispatch validation of the same immutable record.
    conn.execute("DELETE FROM notion_queue WHERE kind='revision'", [])
        .unwrap();
    config.notion.record_bytes = 1;
    telegram_archive_bot::notion_worker::tick(
        &store,
        &config,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    let id: String = conn
        .query_row("SELECT id FROM notion_queue", [], |r| r.get(0))
        .unwrap();
    store.retry_notion(&id).unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    telegram_archive_bot::notion_worker::tick(
        &store,
        &config,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(store.failure_report_status(10).unwrap()["events"], 1);
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn worker_publishes_actual_source_changes_and_verifies_receipts() {
    let (root, store, config) = worker_fixture();
    let mut captured = capture("100", "完整原文 🌍\n");
    captured.content.as_mut().unwrap().metadata = ContentMetadata {
        title: Some("Fixture title".into()),
        author: Some("Original author".into()),
        authored_at: Some("2026-01-01T00:00:00Z".into()),
        language: Some("zh".into()),
    };
    let revision = store.capture(&captured).unwrap().unwrap();
    drain_until(&store, &config, |m| m["states"]["complete"] == 2).await;
    let conn = rusqlite::Connection::open(config.state_dir.join("archive.sqlite")).unwrap();
    let complete: i64 = conn
        .query_row(
            "SELECT count(*) FROM notion_queue WHERE state='complete'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        complete, 2,
        "worker must publish and remotely verify source/revision intents"
    );
    let record: String = conn
        .query_row(
            "SELECT record FROM notion_queue WHERE kind='revision'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let record: telegram_archive_bot::notion::Record = serde_json::from_str(&record).unwrap();
    assert_eq!(record.content, captured.content.unwrap().body);
    assert_eq!(record.id, revision);
    let observation: String = conn
        .query_row(
            "SELECT record FROM notion_queue WHERE kind='observation'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let observation: telegram_archive_bot::notion::Record =
        serde_json::from_str(&observation).unwrap();
    let observation: serde_json::Value = serde_json::from_str(&observation.content).unwrap();
    assert_eq!(
        observation["revision_metadata"]["author"],
        "Original author"
    );
    assert_eq!(observation["revision_metadata"]["language"], "zh");
    let receipts: Vec<String> = conn
        .prepare("SELECT receipt FROM notion_queue WHERE state='complete'")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    for receipt in receipts {
        let v: serde_json::Value = serde_json::from_str(&receipt).unwrap();
        assert_eq!(v["schema_version"], 1);
        assert!(!v["canonical_digest"].as_str().unwrap().is_empty());
    }
    let remote: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("remote.json")).unwrap()).unwrap();
    assert_eq!(remote["pages"].as_object().unwrap().len(), 2);
}

#[cfg(feature = "ntn")]
fn due(config: &telegram_archive_bot::config::Config) {
    rusqlite::Connection::open(config.state_dir.join("archive.sqlite"))
        .unwrap()
        .execute(
            "UPDATE notion_queue SET available_at=0 WHERE state!='complete'",
            [],
        )
        .unwrap();
}

#[tokio::test]
async fn disabled_and_compiled_out_worker_never_launches_a_probe() {
    let (root, store, mut config) = worker_fixture();
    store.capture(&capture("100", "retained locally")).unwrap();
    config.notion.enabled = false;
    for _ in 0..3 {
        telegram_archive_bot::notion_worker::tick(
            &store,
            &config,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
    }
    assert!(!root.path().join("calls").exists());
    assert_eq!(store.notion_metrics().unwrap()["pending"], 2);
    if !cfg!(feature = "ntn") {
        config.notion.enabled = true;
        telegram_archive_bot::notion_worker::tick(
            &store,
            &config,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(!root.path().join("calls").exists());
    }
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn lost_creation_response_reconciles_marker_without_recreating_after_empty_search() {
    let (root, store, config) = worker_fixture();
    store
        .capture(&capture("100", "small complete source"))
        .unwrap();
    std::fs::write(root.path().join("mode"), "accepted_lost").unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    drain_until(&store, &config, |m| m["states"]["reconcile"] == 2).await;
    let page_count = || {
        serde_json::from_slice::<serde_json::Value>(
            &std::fs::read(root.path().join("remote.json")).unwrap(),
        )
        .unwrap()["pages"]
            .as_object()
            .unwrap()
            .len()
    };
    assert_eq!(page_count(), 2);
    std::fs::write(root.path().join("mode"), "empty_search").unwrap();
    for _ in 0..3 {
        due(&config);
        telegram_archive_bot::notion_worker::tick(&store, &config, &cancel)
            .await
            .unwrap();
    }
    assert_eq!(store.notion_metrics().unwrap()["states"]["reconcile"], 2);
    assert_eq!(
        page_count(),
        2,
        "empty search is never authorization to create again"
    );
    drop(store);
    std::fs::write(root.path().join("mode"), "normal").unwrap();
    let reopened = Store::open(&config.state_dir).unwrap();
    due(&config);
    drain_until(&reopened, &config, |m| m["states"]["complete"] == 2).await;
    assert_eq!(
        reopened.notion_metrics().unwrap()["states"]["complete"],
        2,
        "restart must find and verify the actually created pages"
    );
    assert_eq!(page_count(), 2);
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn known_page_and_progress_survive_cancellation_before_readback() {
    let (root, store, config) = worker_fixture();
    store.capture(&capture("100", "body")).unwrap();
    std::fs::write(root.path().join("mode"), "hold_before_readback").unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    let handle = {
        let (store, config, cancel) = (store.clone(), config.clone(), cancel.clone());
        tokio::spawn(async move {
            telegram_archive_bot::notion_worker::tick(&store, &config, &cancel).await
        })
    };
    let observed = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !root.path().join("reading").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    let pinned: Option<(Option<String>, Option<String>)> =
        rusqlite::Connection::open(config.state_dir.join("archive.sqlite"))
            .unwrap()
            .query_row(
                "SELECT page_id,progress FROM notion_queue WHERE state='creating'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .ok();
    cancel.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(10), handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        observed.is_ok(),
        "actual child must reach the blocked readback boundary"
    );
    let (page, progress) = pinned.unwrap();
    assert!(
        page.is_some() && progress.is_some(),
        "persist known remote identity/progress before awaiting readback"
    );
    assert_eq!(store.notion_metrics().unwrap()["states"]["reconcile"], 1);
    std::fs::write(root.path().join("mode"), "normal").unwrap();
    due(&config);
    drain_until(&store, &config, |m| m["states"]["complete"] == 2).await;
    let receipts: Vec<String> = rusqlite::Connection::open(config.state_dir.join("archive.sqlite"))
        .unwrap()
        .prepare("SELECT receipt FROM notion_queue WHERE state='complete'")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(receipts.len(), 2);
    for receipt in receipts {
        let receipt: telegram_archive_bot::notion::VerifiedPage =
            serde_json::from_str(&receipt).unwrap();
        assert!(!receipt.block_ids.is_empty());
        assert!(!receipt.canonical_digest.is_empty());
    }
}

fn document() -> telegram_archive_bot::notion::Record {
    use telegram_archive_bot::notion::*;
    Record {
        schema_version: 1,
        id: "daily-output-fixture-v1".into(),
        title: "Daily output".into(),
        captured_at: "2026-10-25T00:00:00Z".into(),
        original_urls: vec![],
        provenance: Provenance {
            provider: "archive".into(),
            extractor: "agent-v1".into(),
            retrieved_at: "2026-10-25T00:00:00Z".into(),
            completeness: FetchStatus::Partial,
            original_bytes_sha256: None,
        },
        data: RecordData::AgentOutput {
            manifest_id: "manifest-v1".into(),
            sop_version: "sop-v1".into(),
            level: 1,
            sources: vec![],
        },
        content: "Exact generated output 🌍\nwith explicit coverage gaps.".into(),
    }
}

#[test]
fn derived_document_is_immutable_and_enqueues_in_same_transaction() {
    let (_root, store, config) = worker_fixture();
    let record = document();
    let id = store.record_notion_document(&record).unwrap();
    assert_eq!(store.record_notion_document(&record).unwrap(), id);
    assert_eq!(store.notion_metrics().unwrap()["pending"], 1);
    let mut changed = record.clone();
    changed.content.push_str("changed");
    assert!(store.record_notion_document(&changed).is_err());
    let conn = rusqlite::Connection::open(config.state_dir.join("archive.sqlite")).unwrap();
    let saved: String = conn
        .query_row("SELECT record FROM notion_documents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        serde_json::from_str::<telegram_archive_bot::notion::Record>(&saved).unwrap(),
        record
    );
    conn.execute_batch("CREATE TRIGGER fixture_notion_failure BEFORE INSERT ON notion_queue BEGIN SELECT RAISE(ABORT,'synthetic queue failure'); END;").unwrap();
    changed.id = "must-rollback".into();
    assert!(store.record_notion_document(&changed).is_err());
    assert_eq!(
        conn.query_row("SELECT count(*) FROM notion_documents", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn backup_restore_preserves_intents_and_requires_reconciliation_even_if_snapshot_predates_write() {
    let (root, store, config) = worker_fixture();
    store.capture(&capture("100", "raw source")).unwrap();
    let backup = root.path().join("backup");
    store.backup(&backup).unwrap();
    let restored = root.path().join("restored");
    Store::restore_backup(&backup, &restored).unwrap();
    let restored = Store::open(&restored).unwrap();
    assert!(restored.recovery_paused().unwrap());
    let conn = rusqlite::Connection::open(restored.root().join("archive.sqlite")).unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM notion_queue WHERE state='reconcile' AND may_have_remote=1",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        2
    );
    assert_eq!(
        store.source_counts().unwrap(),
        restored.source_counts().unwrap()
    );
    assert_eq!(restored.notion_metrics().unwrap()["pending"], 2);
    assert_eq!(config.state_dir, store.root());
}

#[tokio::test]
async fn derived_document_survives_git_only_restore_without_receipts_or_automatic_creation() {
    let (root, store, mut config) = worker_fixture();
    let record = document();
    let key = record.plan(&config.notion).unwrap().publication_key;
    // Seed the existing semantic table directly to exercise projection before the public save API exists.
    let conn = rusqlite::Connection::open(config.state_dir.join("archive.sqlite")).unwrap();
    conn.execute(
        "INSERT INTO notion_documents(id,record) VALUES(?1,?2)",
        rusqlite::params![key, serde_json::to_string(&record).unwrap()],
    )
    .unwrap();
    config.archive.directory = Some(root.path().join("archive"));
    config.archive.branch = Some("main".into());
    let cancel = tokio_util::sync::CancellationToken::new();
    store.project(&config, 100, &cancel).await.unwrap();
    let archive = config.archive.directory.as_ref().unwrap();
    let path = archive.join(format!("archive-v1/notion-documents/{key}.json"));
    assert_eq!(
        serde_json::from_slice::<telegram_archive_bot::notion::Record>(
            &std::fs::read(&path).unwrap()
        )
        .unwrap(),
        record
    );
    let target = root.path().join("git-restored");
    Store::restore_git(archive, &target, &config, &cancel)
        .await
        .unwrap();
    let restored = Store::open(&target).unwrap();
    let conn = rusqlite::Connection::open(target.join("archive.sqlite")).unwrap();
    let saved: String = conn
        .query_row(
            "SELECT record FROM notion_documents WHERE id=?1",
            [key],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<telegram_archive_bot::notion::Record>(&saved).unwrap(),
        record
    );
    assert!(restored.recovery_paused().unwrap());
    assert!(
        !telegram_archive_bot::notion_worker::tick(&restored, &config, &cancel)
            .await
            .unwrap()
    );
    assert!(!root.path().join("calls").exists());
    assert_eq!(conn.query_row("SELECT count(*) FROM notion_queue WHERE state='reconcile' AND may_have_remote=1 AND receipt IS NULL",[],|r|r.get::<_,i64>(0)).unwrap(),1);
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn oversized_record_is_a_visible_terminal_conflict_not_queue_starvation() {
    let (_root, store, mut config) = worker_fixture();
    config.notion.record_bytes = 4096;
    store.capture(&capture("100", &"X".repeat(10000))).unwrap();
    store
        .capture(&capture("200", "small valid source"))
        .unwrap();
    drain_until(&store, &config, |m| {
        m["states"]["complete"] == 3 && m["states"]["conflict"] == 1
    })
    .await;
    let metrics = store.notion_metrics().unwrap();
    assert_eq!(metrics["states"]["conflict"], 1);
    assert_eq!(metrics["states"]["complete"], 3);
}

#[cfg(feature = "ntn")]
async fn drain_until(
    store: &Store,
    config: &telegram_archive_bot::config::Config,
    condition: impl Fn(&serde_json::Value) -> bool,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut ticks = vec![];
    while !condition(&store.notion_metrics().unwrap()) {
        assert!(
            std::time::Instant::now() < deadline,
            "drain timed out; ticks={ticks:?}; metrics={}",
            store.notion_metrics().unwrap()
        );
        ticks.push(
            telegram_archive_bot::notion_worker::tick(
                store,
                config,
                &tokio_util::sync::CancellationToken::new(),
            )
            .await
            .unwrap(),
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn operator_candidate_is_untrusted_until_body_verification_and_human_edits_are_conflicts() {
    use std::process::Command;
    let (root, store, config) = worker_fixture();
    let config_path = root.path().join("config.toml");
    std::fs::write(&config_path, toml::to_string(&config).unwrap()).unwrap();
    let cli = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_telegram-archive-bot"))
            .arg("--config")
            .arg(&config_path)
            .args(args)
            .output()
            .unwrap()
    };
    let listed = cli(&["notion-list"]);
    assert!(
        listed.status.success(),
        "{}",
        String::from_utf8_lossy(&listed.stderr)
    );
    store
        .capture(&capture("100", "unchanged captured source"))
        .unwrap();
    std::fs::write(root.path().join("mode"), "accepted_lost").unwrap();
    drain_until(&store, &config, |m| m["states"]["reconcile"] == 2).await;
    let conn = rusqlite::Connection::open(config.state_dir.join("archive.sqlite")).unwrap();
    let (id, key): (String, String) = conn
        .query_row(
            "SELECT id,publication_key FROM notion_queue ORDER BY id LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    let remote: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("remote.json")).unwrap()).unwrap();
    let pages = remote["pages"].as_object().unwrap();
    let correct = pages
        .iter()
        .find(|(_, p)| p["properties"].to_string().contains(&key))
        .unwrap()
        .0;
    let wrong = pages.keys().find(|p| *p != correct).unwrap();
    assert!(
        cli(&["notion-reconcile", &id, "--page-id", wrong])
            .status
            .success()
    );
    assert!(
        conn.query_row(
            "SELECT page_id IS NULL FROM notion_queue WHERE id=?1",
            [&id],
            |r| r.get::<_, bool>(0)
        )
        .unwrap()
    );
    std::fs::write(root.path().join("mode"), "normal").unwrap();
    drain_until(&store, &config, |m| m["states"]["conflict"] == 1).await;
    assert!(
        conn.query_row(
            "SELECT page_id IS NULL FROM notion_queue WHERE id=?1",
            [&id],
            |r| r.get::<_, bool>(0)
        )
        .unwrap(),
        "wrong operator candidate must never become a known remote identity"
    );
    assert!(
        !cli(&["notion-retry", &id]).status.success(),
        "uncertain creation must never be reset to create"
    );
    assert!(
        cli(&["notion-reconcile", &id, "--page-id", correct])
            .status
            .success()
    );
    drain_until(&store, &config, |m| m["states"]["complete"] == 2).await;
    assert_eq!(
        conn.query_row("SELECT page_id FROM notion_queue WHERE id=?1", [&id], |r| r
            .get::<_, String>(0))
            .unwrap(),
        *correct
    );
    let mut edited: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("remote.json")).unwrap()).unwrap();
    edited["pages"][correct]["blocks"][2]["paragraph"]["rich_text"][0]["text"]["content"] =
        json!("human changed the managed body");
    let bytes = serde_json::to_vec(&edited).unwrap();
    std::fs::write(root.path().join("remote.json"), &bytes).unwrap();
    assert!(cli(&["notion-reconcile", &id]).status.success());
    drain_until(&store, &config, |m| m["states"]["conflict"] == 1).await;
    assert_eq!(
        std::fs::read(root.path().join("remote.json")).unwrap(),
        bytes
    );
    let listed: serde_json::Value =
        serde_json::from_slice(&cli(&["notion-list", "--limit", "10"]).stdout).unwrap();
    assert_eq!(listed.as_array().unwrap().len(), 2);
    assert!(!listed.to_string().contains("unchanged captured source"));
    let calls = std::fs::read_to_string(root.path().join("calls")).unwrap();
    assert_eq!(
        calls
            .lines()
            .filter(|line| line.contains("\"v1/pages\""))
            .count(),
        2
    );
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn auth_rate_and_missing_tool_do_not_lose_local_content_or_claim_completion() {
    for (mode, state) in [("auth", "blocked_auth"), ("rate", "retry_wait")] {
        let (root, store, config) = worker_fixture();
        store.capture(&capture("100", "retained")).unwrap();
        std::fs::write(root.path().join("mode"), mode).unwrap();
        drain_until(&store, &config, |m| m["states"][state] == 2).await;
        let conn = rusqlite::Connection::open(config.state_dir.join("archive.sqlite")).unwrap();
        assert_eq!(conn.query_row("SELECT count(*) FROM notion_queue WHERE may_have_remote=0 AND receipt IS NULL AND available_at>0",[],|r|r.get::<_,i64>(0)).unwrap(),2);
        assert!(!root.path().join("remote.json").exists());
        std::fs::write(root.path().join("mode"), "normal").unwrap();
        due(&config);
        drain_until(&store, &config, |m| m["states"]["complete"] == 2).await;
    }
    let (root, store, mut config) = worker_fixture();
    store.capture(&capture("100", "retained")).unwrap();
    config.notion.executable = Some(root.path().join("missing-ntn"));
    for _ in 0..3 {
        assert!(
            !telegram_archive_bot::notion_worker::tick(
                &store,
                &config,
                &tokio_util::sync::CancellationToken::new()
            )
            .await
            .unwrap()
        );
    }
    assert!(!root.path().join("calls").exists());
    assert_eq!(store.notion_metrics().unwrap()["pending"], 2);
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn corrected_tool_readiness_automatically_resumes_without_consuming_create_budget() {
    let (root, store, mut config) = worker_fixture();
    config.notion.max_attempts = 1;
    store.capture(&capture("100", "body")).unwrap();
    std::fs::write(root.path().join("mode"), "unsupported").unwrap();
    let conn = rusqlite::Connection::open(config.state_dir.join("archive.sqlite")).unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while conn
        .query_row(
            "SELECT count(*) FROM notion_queue WHERE error='unsupported_tool'",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
        != 2
    {
        assert!(std::time::Instant::now() < deadline);
        telegram_archive_bot::notion_worker::tick(
            &store,
            &config,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(!root.path().join("remote.json").exists());
    std::fs::write(root.path().join("mode"), "normal").unwrap();
    due(&config);
    drain_until(&store, &config, |m| m["states"]["complete"] == 2).await;
    assert_eq!(
        conn.query_row("SELECT sum(create_attempts) FROM notion_queue", [], |r| r
            .get::<_, i64>(
            0
        ))
        .unwrap(),
        2
    );
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn partial_write_survives_restart_and_serializes_creators_without_overwriting_remote() {
    let (root, store, config) = worker_fixture();
    store.capture(&capture("100", &"x".repeat(250000))).unwrap();
    std::fs::write(root.path().join("mode"), "hold_before_append").unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    let task = {
        let (s, c, k) = (store.clone(), config.clone(), cancel.clone());
        tokio::spawn(async move {
            while !k.is_cancelled() {
                telegram_archive_bot::notion_worker::tick(&s, &c, &k)
                    .await
                    .unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
    };
    let observed = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        while !root.path().join("appending").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    let contender = telegram_archive_bot::notion_worker::tick(
        &store,
        &config,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await;
    let conn = rusqlite::Connection::open(config.state_dir.join("archive.sqlite")).unwrap();
    let progress: Option<String> = conn
        .query_row(
            "SELECT progress FROM notion_queue WHERE state='creating'",
            [],
            |r| r.get(0),
        )
        .ok()
        .flatten();
    cancel.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap();
    assert!(observed.is_ok());
    assert!(!contender.unwrap());
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&progress.unwrap()).unwrap()["written_blocks"],
        100
    );
    let remote = std::fs::read(root.path().join("remote.json")).unwrap();
    let backup = root.path().join("partial-backup");
    store.backup(&backup).unwrap();
    let restored_path = root.path().join("partial-restored");
    Store::restore_backup(&backup, &restored_path).unwrap();
    let restored = Store::open(&restored_path).unwrap();
    let restored_conn = rusqlite::Connection::open(restored_path.join("archive.sqlite")).unwrap();
    let saved_progress: Option<String> = restored_conn
        .query_row(
            "SELECT progress FROM notion_queue WHERE kind='revision'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(saved_progress.is_some());
    assert!(restored.recovery_paused().unwrap());
    drop(store);
    let reopened = Store::open(&config.state_dir).unwrap();
    std::fs::write(root.path().join("mode"), "normal").unwrap();
    due(&config);
    drain_until(&reopened, &config, |m| {
        m["states"]["complete"] == 1 && m["states"]["conflict"] == 1
    })
    .await;
    assert_eq!(
        std::fs::read(root.path().join("remote.json")).unwrap(),
        remote
    );
    assert_eq!(
        reopened.source_counts().unwrap(),
        restored.source_counts().unwrap()
    );
}

#[test]
fn queue_tick_query_plans_exclude_historical_work_and_temporary_sorts() {
    let root = root();
    let store = Store::open(root.path()).unwrap();
    let db = rusqlite::Connection::open(store.root().join("archive.sqlite")).unwrap();
    for (sql, index) in [
        (
            "UPDATE notion_attempts SET finished_at=1,outcome='reconcile' WHERE finished_at IS NULL",
            "notion_attempts_unfinished",
        ),
        (
            "SELECT id FROM notion_queue WHERE destination_key='fixture' AND state NOT IN ('complete','conflict','creating') AND available_at<=1 AND (may_have_remote=1 OR create_attempts<3) ORDER BY available_at,created_at,id LIMIT 1",
            "notion_queue_due",
        ),
        (
            "SELECT id FROM notion_queue WHERE destination_key IS NULL ORDER BY created_at,id LIMIT 64",
            "notion_queue_unbound",
        ),
    ] {
        let plan = db
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .unwrap()
            .query_map([], |r| r.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
            .join("\n");
        assert!(
            plan.contains(index) && !plan.contains("TEMP B-TREE"),
            "{sql}: {plan}"
        );
    }
}
