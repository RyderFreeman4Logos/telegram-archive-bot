//! DA09 / #76: production producer transactions feed the pinned reader corpus.
use anyhow::Result;
use futures_util::FutureExt;
use serde_json::json;
use std::{fs, os::unix::fs::PermissionsExt};
use telegram_archive_bot::{
    activities::*, config::*, daily_capture::Collector, jobs::Failure, reader::*, sources::*,
    storage::Store,
};
use tokio_util::sync::CancellationToken;

fn fixture() -> Result<(
    tempfile::TempDir,
    Config,
    Store,
    telegram_archive_bot::planner::DayPlan,
)> {
    let root = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()?;
    let mut config = Config {
        state_dir: root.path().join("state"),
        ..Config::default()
    };
    config.notion.enabled = false;
    config.failure_reporting.enabled = false;
    config.daily.enabled = true;
    config.daily.capture.enabled = true;
    config.limits.max_attempts = 1;
    config.daily.accounts = vec![WatchAccount {
        key: "synthetic".into(),
        user_id: Some("1".into()),
        ..WatchAccount::default()
    }];
    let store = Store::open(&config.state_dir)?;
    let day = store
        .backfill_daily(
            &config,
            "2026-03-08".parse()?,
            "2026-03-08".parse()?,
            1_790_000_000,
        )?
        .windows
        .remove(0);
    Ok((root, config, store, day))
}
fn discovered(day: &telegram_archive_bot::planner::DayPlan, identity: Identity) -> ActivityCapture {
    ActivityCapture {
        actor: day.account.clone(),
        handle: None,
        display_name: None,
        provider_event_id: Some(identity.external_id.clone()),
        kind: ActivityKind::Original,
        discovery: DiscoveryRun {
            provider: "x".into(),
            key: day.id.clone(),
            provenance: json!({"window_id": day.id}),
        },
        occurrence_key: "one".into(),
        observation_key: "timeline".into(),
        activity_time: TimeEvidence::Unknown {
            provenance: json!({}),
        },
        source_time: TimeEvidence::Unknown {
            provenance: json!({}),
        },
        retrieved_at: day.utc_end,
        provenance: json!({}),
        capture: Capture {
            identity,
            submitted_url: None,
            content: None,
            status: FetchStatus::Missing,
            extractor: "timeline-fixture".into(),
            evidence: json!({"reason":"awaiting_body_hydration"}),
            raw_digest: None,
            relations: vec![],
        },
        view_policy: ViewPolicy {
            max_nodes: 10,
            max_depth: 3,
        },
    }
}
fn settle(store: &Store, config: &Config) -> Result<PortableExport> {
    for _ in 0..12 {
        assert_eq!(store.reader_tick(config, 1)?.errors, 0);
    }
    let head = store
        .reader_heads("2026-03-08", "America/Los_Angeles", 100, None)?
        .into_iter()
        .find(|h| h.window_id.is_none())
        .unwrap();
    store.reader_export(config, &head.manifest_id)
}
struct CaptureFixture(bool);
impl Collector for CaptureFixture {
    fn collect<'a>(
        &'a self,
        _: &'a Store,
        _: &'a Config,
        source: &'a Identity,
        _: &'a CancellationToken,
    ) -> futures_util::future::BoxFuture<'a, Result<Vec<Capture>>> {
        async move {
            if self.0 {
                return Err(Failure::Network.into());
            }
            Ok(vec![Capture {
                identity: source.clone(),
                submitted_url: None,
                content: Some(Content {
                    body: "Pinned captured body".into(),
                    metadata: Default::default(),
                }),
                status: FetchStatus::Complete,
                extractor: "fixture".into(),
                evidence: json!({"limitations":["media_not_downloaded"]}),
                raw_digest: None,
                relations: vec![],
            }])
        }
        .boxed()
    }
}

#[tokio::test]
async fn actual_capture_failure_and_retained_media_gap_update_only_the_affected_reader()
-> Result<()> {
    let (_root, config, store, day) = fixture()?;
    let source = Identity::x("8")?;
    let origin = store.record_activity(&discovered(&day, source.clone()))?;
    let before = settle(&store, &config)?;
    assert_eq!(before.activities[0].disposition, "pending");
    telegram_archive_bot::daily_capture::step_with(
        &store,
        &config,
        &CaptureFixture(true),
        &CancellationToken::new(),
    )
    .await?;
    let failed = settle(&store, &config)?;
    assert_eq!(
        failed.activities[0].disposition, "failed",
        "actual durable capture failure must replace pending admission"
    );
    assert_eq!(
        serde_json::to_value(&failed.activities[0].sources[0].enrichment)?["capture"]["state"],
        "failed"
    );
    assert_eq!(before.activities[0].observation_id, origin.id);
    store.retry_activity_capture(&config, &source.id())?;
    telegram_archive_bot::daily_capture::step_with(
        &store,
        &config,
        &CaptureFixture(false),
        &CancellationToken::new(),
    )
    .await?;
    telegram_archive_bot::daily_capture::step_with(
        &store,
        &config,
        &CaptureFixture(false),
        &CancellationToken::new(),
    )
    .await?;
    let hydrated = settle(&store, &config)?;
    assert_ne!(hydrated.manifest_id, failed.manifest_id);
    assert_ne!(hydrated.activities[0].observation_id, origin.id);
    let enrichment = hydrated.activities[0].sources[0]
        .enrichment
        .as_ref()
        .unwrap();
    assert!(enrichment.gaps.contains(&"media_not_downloaded".into()));
    assert!(
        !hydrated.manifest.body_complete,
        "complete text cannot hide retained media limitations"
    );
    let reopened = Store::open(&config.state_dir)?;
    assert_eq!(
        settle(&reopened, &config)?.manifest_id,
        hydrated.manifest_id,
        "restart and unchanged producer state are a semantic no-op"
    );
    assert_eq!(
        reopened
            .reader_export(&config, &before.manifest_id)?
            .activities[0]
            .observation_id,
        origin.id,
        "old immutable manifests retain the original pin"
    );
    Ok(())
}

#[tokio::test]
async fn actual_reporter_readback_binds_only_matching_private_streams_and_survives_hold()
-> Result<()> {
    use telegram_archive_bot::{failure_report_worker, failure_reporting::*};
    let (root, mut config, store, day) = fixture()?;
    config.daily.accounts.push(WatchAccount {
        key: "empty".into(),
        user_id: Some("2".into()),
        ..WatchAccount::default()
    });
    let empty = store
        .backfill_daily(
            &config,
            "2026-03-08".parse()?,
            "2026-03-08".parse()?,
            1_790_000_000,
        )?
        .windows
        .into_iter()
        .find(|w| w.id != day.id)
        .unwrap();
    let a = store.record_activity(&discovered(&day, Identity::x("8")?))?;
    let b = store.record_activity(&discovered(&day, Identity::x("9")?))?;
    let before = settle(&store, &config)?;
    let tool = root.path().join("gh.py");
    fs::write(&tool, include_str!("fixtures/fake_gh_reports.py"))?;
    fs::set_permissions(&tool, fs::Permissions::from_mode(0o700))?;
    let token = root.path().join("token");
    fs::write(&token, "SYNTHETIC_GH_DA08_TOKEN")?;
    fs::set_permissions(&token, fs::Permissions::from_mode(0o600))?;
    config.failure_reporting = FailureReporting {
        executable: Some(tool),
        token: Some(SecretRef::File(token)),
        threshold: 1,
        debounce_seconds: 0,
        ..FailureReporting::default()
    };
    for (id, stream, subsystem, source, window) in [
        (
            "capture",
            format!("capture:{}", a.activity.source.id()),
            Subsystem::Capture,
            a.activity.source.id(),
            None,
        ),
        (
            "empty",
            format!("timeline:{}", empty.id),
            Subsystem::Timeline,
            empty.id.clone(),
            Some(empty.id.clone()),
        ),
    ] {
        store.record_failure(&FailureEvent {
            event_id: id.into(),
            stream_id: stream,
            subsystem,
            provider: "synthetic".into(),
            operation: "collection".into(),
            error_class: ErrorClass::Transport,
            provider_schema: "v1".into(),
            observed_at: 100,
            outcome: Outcome::Failed,
            window_id: window,
            source: Source::Private(source),
            public_excerpt: None,
            private_details: "PRIVATE_WATCHLIST_AND_TOKEN".into(),
            redactions: vec![],
        })?;
    }
    let cancel = CancellationToken::new();
    for _ in 0..4 {
        failure_report_worker::tick(&store, &config.failure_reporting, 1000, &cancel).await?;
    }
    let remote: serde_json::Value =
        serde_json::from_slice(&fs::read(root.path().join("remote.json"))?)?;
    assert_eq!(
        remote["issues"].as_array().unwrap().len(),
        2,
        "two actual private reporter readbacks"
    );
    let after = settle(&store, &config)?;
    let a_entry = after
        .activities
        .iter()
        .find(|e| e.activity_id == a.activity.id().unwrap())
        .unwrap();
    let links = a_entry.sources[0]
        .enrichment
        .as_ref()
        .map(|e| e.failure_report_urls.clone())
        .unwrap_or_default();
    assert_eq!(
        links.len(),
        1,
        "producer issue URL must flow automatically after exact remote readback"
    );
    let b_entry = after
        .activities
        .iter()
        .find(|e| e.activity_id == b.activity.id().unwrap())
        .unwrap();
    assert!(
        b_entry.sources[0]
            .enrichment
            .as_ref()
            .is_none_or(|e| e.failure_report_urls.is_empty()),
        "unrelated private source must not inherit a matching global fingerprint"
    );
    let window = after
        .manifest
        .windows
        .iter()
        .find(|w| w.window_id == empty.id)
        .unwrap();
    assert_eq!(
        serde_json::to_value(window)?["failure_report_urls"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "failed empty days retain their own report navigation"
    );
    assert!(!serde_json::to_string(&after)?.contains("PRIVATE_WATCHLIST_AND_TOKEN"));
    assert!(
        store
            .reader_export(&config, &before.manifest_id)?
            .activities
            .iter()
            .all(|a| a.sources.iter().all(|s| s
                .enrichment
                .as_ref()
                .is_none_or(|e| e.failure_report_urls.is_empty())))
    );
    let backup = root.path().join("backup");
    store.backup(&backup)?;
    let restored = root.path().join("restored");
    assert!(Store::restore_backup(&backup, &restored)?.external_side_effects_paused);
    let recovered = Store::open(&restored)?;
    assert!(recovered.reader_tick(&config, 1)?.paused);
    assert_eq!(
        recovered
            .reader_export(&config, &after.manifest_id)?
            .manifest_id,
        after.manifest_id,
        "restored immutable semantic payload survives operational holds"
    );
    recovered.release_recovery_hold()?;
    assert_eq!(settle(&recovered, &config)?.manifest_id, after.manifest_id);
    Ok(())
}

#[test]
fn dirty_input_admission_is_transactional_indexed_and_bounded_to_sixty_four_scopes() -> Result<()> {
    use telegram_archive_bot::{digest, failure_reporting::*};
    let (_root, config, store, day) = fixture()?;
    store.record_activity(&discovered(&day, Identity::x("8")?))?;
    let export = settle(&store, &config)?;
    let stream = "capture:flood-fixture";
    let mut conn = rusqlite::Connection::open(config.state_dir.join("archive.sqlite"))?;
    let tx = conn.transaction()?;
    for n in 0..129 {
        let scope = format!("flood-{n:03}");
        tx.execute(
            "INSERT INTO reader_heads VALUES(?1,NULL,'2026-03-08','America/Los_Angeles',?2)",
            rusqlite::params![scope, export.manifest_id],
        )?;
        tx.execute(
            "INSERT INTO reader_dependencies VALUES(?1,'report_stream',?2)",
            rusqlite::params![scope, digest(stream)],
        )?;
    }
    tx.commit()?;
    let event = FailureEvent {
        event_id: "flood".into(),
        stream_id: stream.into(),
        subsystem: Subsystem::Capture,
        provider: "synthetic".into(),
        operation: "capture".into(),
        error_class: ErrorClass::Transport,
        provider_schema: "v1".into(),
        observed_at: 100,
        outcome: Outcome::Failed,
        window_id: None,
        source: Source::Private("private fixture".into()),
        public_excerpt: None,
        private_details: "private fixture".into(),
        redactions: vec![],
    };
    conn.execute_batch("CREATE TRIGGER refuse_reader_input BEFORE INSERT ON reader_dirty_inputs BEGIN SELECT RAISE(ABORT,'synthetic rollback'); END;")?;
    assert!(store.record_failure(&event).is_err());
    assert_eq!(
        conn.query_row("SELECT count(*) FROM failure_events", [], |r| r
            .get::<_, u64>(0))?,
        0,
        "event and reader invalidation roll back together"
    );
    conn.execute_batch("DROP TRIGGER refuse_reader_input;")?;
    store.record_failure(&event)?;
    assert_eq!(
        conn.query_row("SELECT count(*) FROM reader_dirty", [], |r| r
            .get::<_, u64>(0))?,
        0,
        "producer transaction only enqueues a key, never fans out across129 scopes"
    );
    assert_eq!(
        conn.query_row("SELECT count(*) FROM reader_dirty_inputs", [], |r| r
            .get::<_, u64>(0))?,
        1
    );
    let tick = store.reader_tick(&config, 1)?;
    assert_eq!(tick.admitted, 64);
    assert!(tick.processed <= 1);
    assert!(
        conn.query_row("SELECT count(*) FROM reader_dirty", [], |r| r
            .get::<_, u64>(0))?
            <= 64
    );
    assert_eq!(
        conn.query_row("SELECT cursor FROM reader_dirty_inputs", [], |r| r
            .get::<_, String>(0))?,
        "flood-063"
    );
    for (sql, index) in [
        (
            "SELECT fingerprint FROM failure_streams WHERE stream_key='key' ORDER BY fingerprint LIMIT65",
            "reader_failure_stream_lookup",
        ),
        (
            "SELECT issue_url FROM failure_reports WHERE fingerprint='fp' AND notified_sequence>0 AND issue_url IS NOT NULL ORDER BY repository LIMIT65",
            "reader_report_fingerprint",
        ),
        (
            "SELECT q.record FROM notion_queue q JOIN notion_pdf p ON p.queue_id=q.id WHERE q.kind='pdf_attachment' AND q.observation_id=1 ORDER BY q.id LIMIT1",
            "reader_pdf_observation",
        ),
        (
            "SELECT scope FROM reader_dependencies WHERE kind='report' AND entity='fp' AND scope>'' ORDER BY scope LIMIT64",
            "reader_dependency_entity",
        ),
    ] {
        let sql = sql
            .replace("LIMIT65", "LIMIT 65")
            .replace("LIMIT1", "LIMIT 1")
            .replace("LIMIT64", "LIMIT 64");
        let plan = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?
            .query_map([], |r| r.get::<_, String>(3))?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .join("\n");
        assert!(
            plan.contains(index) && !plan.contains("TEMP B-TREE"),
            "bounded operational lookup must use {index}: {plan}"
        );
        assert!(
            !plan.contains("failure_events"),
            "reader must not scan private event JSON"
        );
    }
    Ok(())
}

fn remaining_producer_fanout(site: &str) -> Result<()> {
    let (_root, config, store, day) = fixture()?;
    let mut input = discovered(&day, Identity::x("8")?);
    let pin = store.record_activity(&input)?;
    let export = settle(&store, &config)?;
    let mut conn = rusqlite::Connection::open(config.state_dir.join("archive.sqlite"))?;
    let (kind, entity) = match site {
        "publication" => ("queue", format!("activity:{}", pin.id)),
        "activity" => ("activity", pin.activity.id()?),
        _ => ("source", pin.activity.source.id()),
    };
    let scopes = if site == "activity" {
        let through: chrono::NaiveDate = day.local_date.parse()?;
        let windows = store
            .backfill_daily(
                &config,
                through - chrono::Duration::days(128),
                through,
                1_790_000_000,
            )?
            .windows;
        assert_eq!(windows.len(), 129);
        for w in &windows {
            conn.execute(
                "INSERT OR IGNORE INTO reader_activity_windows VALUES(?1,?2)",
                rusqlite::params![w.id, entity],
            )?;
        }
        let mut scopes = windows
            .into_iter()
            .map(|w| format!("window:{}", w.id))
            .collect::<Vec<_>>();
        scopes.sort();
        scopes
    } else {
        let tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM reader_dependencies WHERE kind=?1 AND entity=?2",
            rusqlite::params![kind, entity],
        )?;
        let mut scopes = Vec::new();
        for n in 0..129 {
            let scope = format!("remaining-{n:03}");
            tx.execute(
                "INSERT INTO reader_heads VALUES(?1,NULL,'2026-03-08','America/Los_Angeles',?2)",
                rusqlite::params![scope, export.manifest_id],
            )?;
            tx.execute(
                "INSERT INTO reader_dependencies VALUES(?1,?2,?3)",
                rusqlite::params![scope, kind, entity],
            )?;
            scopes.push(scope);
        }
        tx.commit()?;
        scopes
    };
    conn.execute_batch("DELETE FROM reader_dirty; DELETE FROM reader_dirty_inputs; UPDATE reader_bootstrap SET done=1;")?;
    input.observation_key = "new-admission".into();
    input.retrieved_at += 1;
    let enrichment = SourceEnrichment {
        source_id: pin.activity.source.id(),
        revision_id: None,
        attachments: vec![],
        gaps: vec!["explicit manual gap".into()],
        failure_report_urls: vec![],
        capture: None,
    };
    let produce = || -> Result<()> {
        match site {
            "publication" => {
                conn.execute(
                    "UPDATE notion_queue SET state='conflict' WHERE id=?1",
                    [&entity],
                )?;
            }
            "source" => {
                store.capture(&input.capture)?;
            }
            "activity" => {
                store.record_activity(&input)?;
            }
            "note" => {
                store.reader_note_source(&enrichment)?;
            }
            _ => unreachable!(),
        }
        Ok(())
    };
    let before: (u64, u64, u64) = conn.query_row("SELECT (SELECT count(*) FROM observations),(SELECT count(*) FROM activity_observations),(SELECT count(*) FROM reader_enrichments)", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    conn.execute_batch(&format!("CREATE TRIGGER refuse_remaining_input BEFORE INSERT ON reader_dirty_inputs WHEN NEW.kind='{kind}' BEGIN SELECT RAISE(ABORT,'synthetic producer rollback'); END;"))?;
    // A failed invalidation must roll back the actual producer mutation with it.
    let rollback = produce();
    let after: (u64, u64, u64) = conn.query_row("SELECT (SELECT count(*) FROM observations),(SELECT count(*) FROM activity_observations),(SELECT count(*) FROM reader_enrichments)", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    conn.execute_batch("DROP TRIGGER refuse_remaining_input;")?;
    // First assert the fanout itself, so the unchanged implementation has a behavioral RED.
    if rollback.is_err() {
        assert_eq!(
            after, before,
            "{site}: producer and invalidation roll back atomically"
        );
        produce()?;
    }
    assert_eq!(
        conn.query_row("SELECT count(*) FROM reader_dirty", [], |r| r
            .get::<_, u64>(0))?,
        0,
        "{site}: producer must not synchronously fan out to129 dependent scopes"
    );
    assert!(
        rollback.is_err(),
        "{site}: invalidation failure must reject producer mutation"
    );
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM reader_dirty_inputs WHERE kind=?1 AND entity=?2",
            rusqlite::params![kind, entity],
            |r| r.get::<_, u64>(0)
        )?,
        1
    );
    // Composite activity capture also enqueues source evidence; isolate this cursor's page.
    conn.execute(
        "DELETE FROM reader_dirty_inputs WHERE kind!=?1 OR entity!=?2",
        rusqlite::params![kind, entity],
    )?;
    for scope in &scopes {
        let window = scope.strip_prefix("window:");
        conn.execute("INSERT INTO reader_dirty VALUES(?1,?2,'2026-03-08','America/Los_Angeles','previous_error')", rusqlite::params![scope,window])?;
    }
    conn.execute("INSERT INTO reader_dirty VALUES('unrelated',NULL,'2026-03-08','America/Los_Angeles','unrelated_error')", [])?;
    let tick = store.reader_tick(&config, 1)?;
    assert_eq!(
        tick.admitted, 64,
        "{site}: exactly one64-scope page per tick"
    );
    assert!(tick.processed <= 1);
    let expected_cursor = scopes[63].strip_prefix("window:").unwrap_or(&scopes[63]);
    assert_eq!(
        conn.query_row(
            "SELECT cursor FROM reader_dirty_inputs WHERE kind=?1 AND entity=?2",
            rusqlite::params![kind, entity],
            |r| r.get::<_, String>(0)
        )?,
        expected_cursor
    );
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM reader_dirty WHERE error='previous_error'",
            [],
            |r| r.get::<_, u64>(0)
        )?,
        65,
        "{site}: only the first64 selected scopes have their errors reset"
    );
    drop(store);
    let reopened = Store::open(&config.state_dir)?;
    assert_eq!(
        reopened.reader_tick(&config, 1)?.admitted,
        64,
        "{site}: restart resumes retained cursor"
    );
    assert_eq!(reopened.reader_tick(&config, 1)?.admitted, 1);
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM reader_dirty_inputs WHERE kind=?1 AND entity=?2",
            rusqlite::params![kind, entity],
            |r| r.get::<_, u64>(0)
        )?,
        0
    );
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM reader_dirty WHERE error='previous_error'",
            [],
            |r| r.get::<_, u64>(0)
        )?,
        0
    );
    assert_eq!(
        conn.query_row(
            "SELECT error FROM reader_dirty WHERE scope='unrelated'",
            [],
            |r| r.get::<_, String>(0)
        )?,
        "unrelated_error"
    );
    if site == "note" {
        reopened.reader_note_source(&enrichment)?;
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM reader_dirty_inputs WHERE kind=?1 AND entity=?2",
                rusqlite::params![kind, entity],
                |r| r.get::<_, u64>(0)
            )?,
            0,
            "same enrichment is a semantic no-op"
        );
    }
    if site == "activity" {
        let plan = conn.prepare("EXPLAIN QUERY PLAN SELECT window_id FROM reader_activity_windows WHERE activity_id=?1 AND window_id>?2 ORDER BY window_id LIMIT 64")?.query_map(rusqlite::params![entity,""],|r|r.get::<_,String>(3))?.collect::<rusqlite::Result<Vec<_>>>()?.join("\n");
        assert!(
            plan.contains("reader_activity_window_reverse") && !plan.contains("TEMP B-TREE"),
            "bounded reverse mapping must be indexed: {plan}"
        );
    }
    Ok(())
}
#[test]
fn bounded_remaining_producer_publication() -> Result<()> {
    remaining_producer_fanout("publication")
}
#[test]
fn bounded_remaining_producer_source() -> Result<()> {
    remaining_producer_fanout("source")
}
#[test]
fn bounded_remaining_producer_activity() -> Result<()> {
    remaining_producer_fanout("activity")
}
#[test]
fn bounded_remaining_producer_note() -> Result<()> {
    remaining_producer_fanout("note")
}

#[test]
fn actual_fx_media_references_remain_a_reader_gap_without_body_recapture() -> Result<()> {
    let (_root, config, store, day) = fixture()?;
    let post = telegram_archive_bot::x::parse_fx(
        "8",
        &json!({"code":200,"tweet":{"id":"8","text":"Exact original body","media":{"all":[{"type":"video","url":"https://example.org/video.mp4"}]}}}),
    )?;
    assert_eq!(
        post.evidence["media_text_status"],
        "references_only_not_transcribed"
    );
    let mut input = discovered(&day, Identity::x("8")?);
    input.capture.content = post.content;
    input.capture.status = post.status;
    input.capture.evidence = post.evidence;
    let pin = store.record_activity(&input)?;
    let export = settle(&store, &config)?;
    assert_eq!(export.activities[0].observation_id, pin.id);
    assert!(
        !export.manifest.body_complete,
        "actual provider media references do not establish media text completeness"
    );
    assert!(
        export.activities[0].sources[0]
            .enrichment
            .as_ref()
            .unwrap()
            .gaps
            .contains(&"references_only_not_transcribed".into())
    );
    Ok(())
}
