//! DA09 / #76: portable, absolute-day corpus handoffs through the public CLI.
use anyhow::Result;
use serde_json::{Value, json};
use std::{fs, os::unix::fs::PermissionsExt, process::Command};
use telegram_archive_bot::{activities::*, config::*, sources::*, storage::Store};

fn cli(config: &std::path::Path, args: &[&str]) -> Result<Value> {
    let out = Command::new(env!("CARGO_BIN_EXE_telegram-archive-bot"))
        .arg("--config")
        .arg(config)
        .args(args)
        .output()?;
    anyhow::ensure!(
        out.status.success(),
        "CLI {:?}: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(serde_json::from_slice(&out.stdout)?)
}

#[test]
fn absolute_day_manifests_enumerate_duplicates_old_reposts_missing_and_failed_windows() -> Result<()>
{
    let root = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()?;
    let mut config = Config {
        state_dir: root.path().join("state"),
        ..Config::default()
    };
    config.daily.enabled = true;
    config.daily.accounts = vec![
        WatchAccount {
            key: "fixture".into(),
            user_id: Some("10".into()),
            ..WatchAccount::default()
        },
        WatchAccount {
            key: "failed".into(),
            user_id: Some("20".into()),
            ..WatchAccount::default()
        },
    ];
    config.notion.enabled = false;
    let path = root.path().join("config.toml");
    fs::write(&path, toml::to_string(&config)?)?;
    let store = Store::open(&config.state_dir)?;
    let report = store.backfill_daily(
        &config,
        "2026-03-08".parse()?,
        "2026-03-08".parse()?,
        1_790_000_000,
    )?;
    let window = &report.windows[0];
    let input = ActivityCapture {
        actor: window.account.clone(),
        handle: Some("synthetic".into()),
        display_name: None,
        provider_event_id: Some("99".into()),
        kind: ActivityKind::Repost,
        discovery: DiscoveryRun {
            provider: "x".into(),
            key: window.id.clone(),
            provenance: json!({"window_id":window.id}),
        },
        occurrence_key: "first".into(),
        observation_key: "first".into(),
        activity_time: TimeEvidence::Instant {
            timestamp: Timestamp::seconds(window.utc_start + 1),
            precision: TimePrecision::Second,
            provenance: json!({"fixture":true}),
        },
        source_time: TimeEvidence::Instant {
            timestamp: Timestamp::seconds(100),
            precision: TimePrecision::Second,
            provenance: json!({"fixture":"old target"}),
        },
        retrieved_at: window.utc_end,
        provenance: json!({"window_id":window.id}),
        capture: Capture {
            identity: Identity::x("1")?,
            submitted_url: Some("https://x.com/i/status/1".into()),
            content: Some(Content {
                body: "Full Article body 🌍".into(),
                metadata: ContentMetadata::default(),
            }),
            status: FetchStatus::Complete,
            extractor: "synthetic-free".into(),
            evidence: json!({"article":true}),
            raw_digest: None,
            relations: vec![],
        },
        view_policy: ViewPolicy {
            max_nodes: 10,
            max_depth: 5,
        },
    };
    store.record_activity(&input)?;
    store.record_activity(&input)?;
    let mut missing = input.clone();
    missing.provider_event_id = Some("100".into());
    missing.observation_key = "missing".into();
    missing.capture.identity = Identity::x("2")?;
    missing.capture.submitted_url = Some("https://x.com/i/status/2".into());
    missing.capture.content = None;
    missing.capture.status = FetchStatus::Missing;
    store.record_activity(&missing)?;
    // Synthetic failed collection, not provider coverage. All updates use SQLite.
    let conn = rusqlite::Connection::open(config.state_dir.join("archive.sqlite"))?;
    conn.execute(
        "UPDATE daily_windows SET state='failed',reason='fixture_provider_unavailable' WHERE id=?1",
        [&report.windows[1].id],
    )?;
    drop(conn);
    for _ in 0..12 {
        cli(&path, &["reader-build", "--limit", "1"])?;
    }
    let heads = cli(
        &path,
        &[
            "reader-list",
            "--date",
            "2026-03-08",
            "--timezone",
            "America/Los_Angeles",
        ],
    )?;
    let heads = heads.as_array().unwrap();
    assert_eq!(heads.len(), 3, "two account windows and one combined date");
    let combined = heads.iter().find(|h| h["window_id"].is_null()).unwrap();
    let id = combined["manifest_id"].as_str().unwrap();
    let exported = cli(&path, &["reader-export", id])?;
    assert_eq!(
        exported["manifest"]["utc_end"].as_i64().unwrap()
            - exported["manifest"]["utc_start"].as_i64().unwrap(),
        23 * 3600
    );
    let items = exported["activities"].as_array().unwrap();
    assert_eq!(items.len(), 2, "every admitted event exactly once");
    assert_eq!(
        items
            .iter()
            .filter(|i| i["disposition"] == "captured")
            .count(),
        1
    );
    assert_eq!(
        items
            .iter()
            .filter(|i| i["disposition"] == "unavailable")
            .count(),
        1
    );
    assert_eq!(exported["manifest"]["collection_complete"], false);
    assert_eq!(exported["manifest"]["publication_verified"], false);
    assert_eq!(exported["manifest"]["analysis_complete"], false);
    assert_eq!(exported["sources"].as_object().unwrap().len(), 1);
    let before = serde_json::to_vec(&exported)?;
    for _ in 0..4 {
        cli(&path, &["reader-build", "--limit", "1"])?;
    }
    assert_eq!(cli(&path, &["reader-export", id])?, exported);
    let next = cli(
        &path,
        &[
            "reader-list",
            "--date",
            "2026-03-08",
            "--timezone",
            "America/Los_Angeles",
        ],
    )?;
    assert_eq!(
        next.as_array()
            .unwrap()
            .iter()
            .find(|h| h["window_id"].is_null())
            .unwrap()["manifest_id"],
        id
    );
    assert!(!String::from_utf8(before)?.contains("config_snapshots"));
    Ok(())
}

#[tokio::test]
async fn external_five_layers_resume_absolute_inputs_reject_metadata_reads_and_stale_changes()
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
        user_id: Some("10".into()),
        ..WatchAccount::default()
    }];
    config.notion.enabled = false;
    config.reader.sop.page_url =
        Some("https://www.notion.so/22222222222222222222222222222222".into());
    let path = root.path().join("config.toml");
    fs::write(&path, toml::to_string(&config)?)?;
    let store = Store::open(&config.state_dir)?;
    let window = store
        .backfill_daily(
            &config,
            "2026-11-01".parse()?,
            "2026-11-01".parse()?,
            1_800_000_000,
        )?
        .windows
        .remove(0);
    let mut input = ActivityCapture {
        actor: window.account.clone(),
        handle: None,
        display_name: None,
        provider_event_id: Some("8".into()),
        kind: ActivityKind::Original,
        discovery: DiscoveryRun {
            provider: "x".into(),
            key: window.id.clone(),
            provenance: json!({"window_id":window.id}),
        },
        occurrence_key: "one".into(),
        observation_key: "v1".into(),
        activity_time: TimeEvidence::Unknown {
            provenance: json!({"fixture":true}),
        },
        source_time: TimeEvidence::Unknown {
            provenance: json!({}),
        },
        retrieved_at: window.utc_end,
        provenance: json!({}),
        capture: Capture {
            identity: Identity::x("8")?,
            submitted_url: None,
            content: Some(Content {
                body: "Entire original captured body".into(),
                metadata: ContentMetadata::default(),
            }),
            status: FetchStatus::Complete,
            extractor: "fixture".into(),
            evidence: json!({}),
            raw_digest: None,
            relations: vec![],
        },
        view_policy: ViewPolicy {
            max_nodes: 10,
            max_depth: 3,
        },
    };
    let observation = store.record_activity(&input)?;
    for _ in 0..5 {
        assert_eq!(store.reader_tick(&config, 1)?.errors, 0);
    }
    let head = store
        .reader_heads("2026-11-01", "America/Los_Angeles", 100, None)?
        .into_iter()
        .find(|h| h.window_id.is_none())
        .unwrap();
    let export = store.reader_export(&config, &head.manifest_id)?;
    assert_eq!(
        export.manifest.utc_end - export.manifest.utc_start,
        25 * 3600
    );
    let input_hash = export
        .documents
        .iter()
        .find(|d| d.content == serde_json::to_string(&export.manifest).unwrap())
        .unwrap()
        .id
        .clone();
    let source = &export.activities[0].sources[0];
    let mut checkpoint = json!({"contract":"daily-reader-v1","session_id":"synthetic-session","layer":1,"specification_version":"1.0.0","manifest_id":head.manifest_id,"input_manifest_sha256":input_hash,"local_date":"2026-11-01","timezone":"America/Los_Angeles","utc_start":window.utc_start,"utc_end":window.utc_end,"sop":config.reader.sop,"state":"in_progress","outputs":[],"cursor":{"next":"paragraph-2"},"processed_activity_ids":[],"readings":[{"source_id":source.source_id,"revision_id":source.revision_id,"state":"metadata_only","reason":"only title inspected","evidence":null}],"unresolved_sources":[source.source_id],"provenance":{"worker":"synthetic external reader","model":null,"version":"fixture-v1","method":"read captured text"}});
    let cp = root.path().join("checkpoint.json");
    fs::write(&cp, serde_json::to_vec(&checkpoint)?)?;
    let saved = cli(&path, &["reader-checkpoint", cp.to_str().unwrap()])?;
    assert_eq!(saved["stale"], false);
    assert_eq!(
        cli(&path, &["reader-checkpoint", cp.to_str().unwrap()])?["id"],
        saved["id"],
        "retry idempotence"
    );
    checkpoint["state"] = json!("complete");
    checkpoint["cursor"] = Value::Null;
    checkpoint["processed_activity_ids"] = json!([observation.activity.id()?]);
    checkpoint["outputs"] =
        json!([{"page_id":"11111111-1111-1111-1111-111111111111","sha256":"a".repeat(64)}]);
    checkpoint["readings"][0]["state"] = json!("fully_read");
    fs::write(&cp, serde_json::to_vec(&checkpoint)?)?;
    let invalid = Command::new(env!("CARGO_BIN_EXE_telegram-archive-bot"))
        .arg("--config")
        .arg(&path)
        .arg("reader-checkpoint")
        .arg(&cp)
        .output()?;
    assert!(
        !invalid.status.success(),
        "metadata/filename without complete-content evidence cannot count as a read"
    );
    checkpoint["readings"][0]["evidence"] = json!({"kind":"captured_text","sha256":source.text_sha256,"reader":"external fixture","method":"entire captured text inspected"});
    checkpoint["unresolved_sources"] = json!([]);
    for layer in 1..=5 {
        checkpoint["layer"] = json!(layer);
        fs::write(&cp, serde_json::to_vec(&checkpoint)?)?;
        cli(&path, &["reader-checkpoint", cp.to_str().unwrap()])?;
    }
    assert_eq!(
        cli(&path, &["reader-checkpoints", &head.manifest_id])?["complete_summary"],
        true
    );
    assert_eq!(
        store
            .reader_export(&config, &head.manifest_id)?
            .checkpoints
            .len(),
        6
    );
    let saved = serde_json::to_value(store.reader_export(&config, &head.manifest_id)?)?;
    let backup = root.path().join("backup");
    store.backup(&backup)?;
    Store::verify_backup(&backup)?;
    let restored = root.path().join("restored");
    assert!(Store::restore_backup(&backup, &restored)?.external_side_effects_paused);
    let full = Store::open(&restored)?;
    assert!(full.reader_tick(&config, 1)?.paused);
    assert_eq!(
        serde_json::to_value(full.reader_export(&config, &head.manifest_id)?)?,
        saved,
        "full recovery preserves exact corpus and checkpoint history"
    );
    config.archive.directory = Some(root.path().join("archive"));
    config.archive.branch = Some("main".into());
    config.archive.push = false;
    let cancel = tokio_util::sync::CancellationToken::new();
    store.project(&config, 100, &cancel).await?;
    let semantic = root.path().join("semantic");
    assert!(
        Store::restore_git(
            config.archive.directory.as_ref().unwrap(),
            &semantic,
            &config,
            &cancel
        )
        .await?
        .external_side_effects_paused
    );
    let rebuilt = Store::open(&semantic)?;
    assert_eq!(
        serde_json::to_value(rebuilt.reader_export(&config, &head.manifest_id)?)?,
        saved,
        "semantic recovery preserves immutable corpus/checkpoints without operational receipts"
    );
    assert!(rebuilt.reader_tick(&config, 1)?.paused);
    input.observation_key = "v2".into();
    input.retrieved_at += 1;
    input.capture.content.as_mut().unwrap().body = "Corrected full body".into();
    store.record_activity(&input)?;
    for _ in 0..5 {
        assert_eq!(store.reader_tick(&config, 1)?.errors, 0);
    }
    let new = store
        .reader_heads("2026-11-01", "America/Los_Angeles", 100, None)?
        .into_iter()
        .find(|h| h.window_id.is_none())
        .unwrap();
    assert_ne!(new.manifest_id, head.manifest_id);
    assert_eq!(
        store
            .reader_export(&config, &head.manifest_id)?
            .sources
            .values()
            .next()
            .unwrap()
            .body,
        "Entire original captured body"
    );
    assert_eq!(
        cli(&path, &["reader-checkpoints", &head.manifest_id])?["stale"],
        true
    );
    let refused = Command::new(env!("CARGO_BIN_EXE_telegram-archive-bot"))
        .arg("--config")
        .arg(&path)
        .arg("reader-checkpoint")
        .arg(&cp)
        .output()?;
    assert!(
        !refused.status.success(),
        "input drift requires deliberate pinned resume"
    );
    assert_eq!(
        cli(
            &path,
            &["reader-checkpoint", cp.to_str().unwrap(), "--allow-stale"]
        )?["stale"],
        true
    );
    input.observation_key = "partial-v3".into();
    input.retrieved_at += 1;
    input.capture.content.as_mut().unwrap().body = "Preview only".into();
    input.capture.status = FetchStatus::Partial;
    store.record_activity(&input)?;
    for _ in 0..5 {
        assert_eq!(store.reader_tick(&config, 1)?.errors, 0);
    }
    let partial = store
        .reader_heads("2026-11-01", "America/Los_Angeles", 100, None)?
        .into_iter()
        .find(|h| h.window_id.is_none())
        .unwrap();
    let partial_export = store.reader_export(&config, &partial.manifest_id)?;
    checkpoint["manifest_id"] = json!(partial.manifest_id);
    checkpoint["input_manifest_sha256"] = json!(
        partial_export
            .documents
            .iter()
            .find(|r| r.content == serde_json::to_string(&partial_export.manifest).unwrap())
            .unwrap()
            .id
    );
    checkpoint["readings"][0]["revision_id"] =
        json!(partial_export.activities[0].sources[0].revision_id);
    checkpoint["readings"][0]["evidence"]["sha256"] =
        json!(partial_export.activities[0].sources[0].text_sha256);
    fs::write(&cp, serde_json::to_vec(&checkpoint)?)?;
    let refused = Command::new(env!("CARGO_BIN_EXE_telegram-archive-bot"))
        .arg("--config")
        .arg(&path)
        .arg("reader-checkpoint")
        .arg(&cp)
        .output()?;
    assert!(
        !refused.status.success(),
        "a hash of a partial preview cannot certify a completed original-source read"
    );
    Ok(())
}

#[test]
fn scoped_dirty_fifo_does_not_starve_later_accounts() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut config = Config {
        state_dir: root.path().join("state"),
        ..Config::default()
    };
    config.daily.enabled = true;
    config.daily.accounts = vec![
        WatchAccount {
            key: "one".into(),
            user_id: Some("1".into()),
            ..WatchAccount::default()
        },
        WatchAccount {
            key: "two".into(),
            user_id: Some("2".into()),
            ..WatchAccount::default()
        },
    ];
    let store = Store::open(&config.state_dir)?;
    let mut windows = store
        .backfill_daily(
            &config,
            "2026-03-08".parse()?,
            "2026-03-08".parse()?,
            1_790_000_000,
        )?
        .windows;
    for _ in 0..5 {
        assert_eq!(store.reader_tick(&config, 1)?.errors, 0);
    }
    windows.sort_by(|a, b| a.id.cmp(&b.id));
    let conn = rusqlite::Connection::open(config.state_dir.join("archive.sqlite"))?;
    conn.execute("DELETE FROM reader_dirty", [])?;
    for w in windows.iter().rev() {
        conn.execute(
            "INSERT INTO reader_dirty VALUES(?1,?2,?3,?4,NULL)",
            rusqlite::params![format!("window:{}", w.id), w.id, w.local_date, w.timezone],
        )?;
    }
    assert_eq!(store.reader_tick(&config, 1)?.processed, 1);
    let remaining: String = conn.query_row(
        "SELECT window_id FROM reader_dirty WHERE window_id IS NOT NULL",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(
        remaining, windows[0].id,
        "oldest dirty admission must win over lexically earlier accounts"
    );
    Ok(())
}

#[test]
fn scoped_window_pins_do_not_adopt_another_days_observation() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut config = Config {
        state_dir: root.path().join("state"),
        ..Config::default()
    };
    config.daily.enabled = true;
    config.daily.accounts = vec![WatchAccount {
        key: "one".into(),
        user_id: Some("1".into()),
        ..WatchAccount::default()
    }];
    let store = Store::open(&config.state_dir)?;
    let windows = store
        .backfill_daily(
            &config,
            "2026-03-08".parse()?,
            "2026-03-09".parse()?,
            1_790_000_000,
        )?
        .windows;
    for (index, w) in windows.iter().enumerate() {
        store.record_activity(&ActivityCapture {
            actor: w.account.clone(),
            handle: None,
            display_name: None,
            provider_event_id: Some("8".into()),
            kind: ActivityKind::Original,
            discovery: DiscoveryRun {
                provider: "x".into(),
                key: w.id.clone(),
                provenance: json!({"window_id":w.id}),
            },
            occurrence_key: "one".into(),
            observation_key: index.to_string(),
            activity_time: TimeEvidence::Unknown {
                provenance: json!({}),
            },
            source_time: TimeEvidence::Unknown {
                provenance: json!({}),
            },
            retrieved_at: w.utc_end,
            provenance: json!({}),
            capture: Capture {
                identity: Identity::x("8")?,
                submitted_url: None,
                content: Some(Content {
                    body: format!("Body observed on day {index}"),
                    metadata: ContentMetadata::default(),
                }),
                status: FetchStatus::Complete,
                extractor: "fixture".into(),
                evidence: json!({}),
                raw_digest: None,
                relations: vec![],
            },
            view_policy: ViewPolicy {
                max_nodes: 10,
                max_depth: 3,
            },
        })?;
    }
    for _ in 0..8 {
        assert_eq!(store.reader_tick(&config, 1)?.errors, 0);
    }
    let head = store
        .reader_heads("2026-03-08", "America/Los_Angeles", 100, None)?
        .into_iter()
        .find(|h| h.window_id.is_none())
        .unwrap();
    let export = store.reader_export(&config, &head.manifest_id)?;
    assert_eq!(
        export.sources.values().next().unwrap().body,
        "Body observed on day 0"
    );
    Ok(())
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn verified_remote_reader_traverses_index_with_pdf_fallback_outage_and_report_links()
-> Result<()> {
    use telegram_archive_bot::reader::*;
    let root = tempfile::tempdir()?;
    let tool = root.path().join("ntn.py");
    fs::write(&tool, include_str!("fixtures/fake_ntn_worker.py"))?;
    fs::set_permissions(&tool, fs::Permissions::from_mode(0o700))?;
    let token = root.path().join("token");
    fs::write(&token, "SYNTHETIC_NOTION_WORKER_TOKEN")?;
    fs::set_permissions(&token, fs::Permissions::from_mode(0o600))?;
    fs::write(root.path().join("mode"), "rate")?;
    let mut config = Config {
        state_dir: root.path().join("store"),
        notion: Notion {
            executable: Some(tool),
            token: Some(SecretRef::File(token)),
            destination: Some(NotionDestination::Page {
                id: "11111111-1111-1111-1111-111111111111".into(),
            }),
            retry_seconds: 1,
            ..Notion::default()
        },
        ..Config::default()
    };
    config.daily.enabled = true;
    config.daily.accounts = vec![WatchAccount {
        key: "one".into(),
        user_id: Some("1".into()),
        ..WatchAccount::default()
    }];
    let store = Store::open(&config.state_dir)?;
    let window = store
        .backfill_daily(
            &config,
            "2026-03-08".parse()?,
            "2026-03-08".parse()?,
            1_790_000_000,
        )?
        .windows
        .remove(0);
    let observation = store.record_activity(&ActivityCapture {
        actor: window.account.clone(),
        handle: None,
        display_name: None,
        provider_event_id: Some("8".into()),
        kind: ActivityKind::Quote,
        discovery: DiscoveryRun {
            provider: "x".into(),
            key: window.id.clone(),
            provenance: json!({"window_id":window.id}),
        },
        occurrence_key: "one".into(),
        observation_key: "one".into(),
        activity_time: TimeEvidence::Unknown {
            provenance: json!({}),
        },
        source_time: TimeEvidence::Unknown {
            provenance: json!({}),
        },
        retrieved_at: window.utc_end,
        provenance: json!({}),
        capture: Capture {
            identity: Identity::x("8")?,
            submitted_url: None,
            content: Some(Content {
                body: "Full captured Article text, with an original PDF link.".into(),
                metadata: ContentMetadata {
                    title: Some("Article fixture".into()),
                    ..ContentMetadata::default()
                },
            }),
            status: FetchStatus::Complete,
            extractor: "synthetic-free-hydration".into(),
            evidence: json!({"article":true}),
            raw_digest: None,
            relations: vec![Related {
                target: Identity::x("9")?,
                kind: RelationKind::Quotes,
            }],
        },
        view_policy: ViewPolicy {
            max_nodes: 10,
            max_depth: 3,
        },
    })?;
    let note = SourceEnrichment {
        capture: None,
        source_id: observation.activity.source.id(),
        revision_id: observation.revision_id.clone(),
        attachments: vec![Attachment {
            original_url: "https://example.org/paper".into(),
            landing_url: Some("https://example.org/paper".into()),
            direct_url: Some("https://example.org/paper.pdf".into()),
            upload_state: "unsupported".into(),
            attachment_state: "link_fallback".into(),
            verified_sha256: None,
            page_id: None,
            block_id: None,
            gap: Some("above_20_mib".into()),
        }],
        gaps: vec!["pdf_binary_unavailable".into()],
        failure_report_urls: vec![
            "https://github.com/example/archive/issues/1".into(),
            "https://github.com/example/archive/issues/1".into(),
        ],
    };
    store.reader_note_source(&note)?;
    let cancel = tokio_util::sync::CancellationToken::new();
    assert!(telegram_archive_bot::notion_worker::tick(&store, &config, &cancel).await?);
    assert_eq!(
        store.notion_metrics()?["states"]["retry_wait"],
        1,
        "actual fake 429 retained, no successful publication inferred"
    );
    fs::write(root.path().join("mode"), "normal")?;
    let mut settled = false;
    for _ in 0..160 {
        assert_eq!(store.reader_tick(&config, 1)?.errors, 0);
        telegram_archive_bot::notion_worker::tick(&store, &config, &cancel).await?;
        if store.notion_metrics()?["pending"] == 0 && store.reader_status()?["pending"] == 0 {
            settled = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        settled,
        "bounded fake outbox must reach actual quiescence before checking manifest"
    );
    let head = store
        .reader_heads("2026-03-08", "America/Los_Angeles", 100, None)?
        .into_iter()
        .find(|h| h.window_id.is_none())
        .unwrap();
    let export = store.reader_export(&config, &head.manifest_id)?;
    assert!(
        export.manifest.publication_verified,
        "explicit missing/context/PDF gaps must be remotely published, separately from body completeness"
    );
    assert!(!export.manifest.body_complete);
    assert!(!export.manifest.collection_complete);
    assert_eq!(
        export.activities[0]
            .sources
            .iter()
            .find_map(|s| s.enrichment.as_ref())
            .unwrap()
            .failure_report_urls
            .len(),
        1,
        "one deduplicated DA08 report reference"
    );
    let remote: Value = serde_json::from_slice(&fs::read(root.path().join("remote.json"))?)?;
    fn payload(page: &Value) -> Value {
        let mut captured = false;
        let mut text = String::new();
        for block in page["blocks"].as_array().unwrap() {
            let kind = block["type"].as_str().unwrap();
            let value = block[kind]["rich_text"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["text"]["content"].as_str().unwrap())
                .collect::<String>();
            if kind == "heading_2" && value == "Captured text (managed)" {
                captured = true;
                continue;
            }
            if captured && value.starts_with("END_ARCHIVE_RECORD_V1 ") {
                break;
            }
            if captured {
                text.push_str(&value);
            }
        }
        serde_json::from_str(&text).unwrap()
    }
    fn visit(
        remote: &Value,
        page: &Value,
        events: &mut std::collections::BTreeSet<String>,
    ) -> Result<()> {
        let value = payload(page);
        if let Some(entries) = value["entries"].as_array() {
            for entry in entries {
                events.insert(entry["activity_id"].as_str().unwrap().into());
            }
        }
        for field in ["index", "accounts", "children"] {
            if let Some(refs) = value[field].as_array() {
                for link in refs {
                    let id = link["page_id"]
                        .as_str()
                        .expect("verified remote closure has directly resolvable page IDs");
                    visit(remote, &remote["pages"][id], events)?;
                }
            }
        }
        Ok(())
    }
    // This enumeration reads only synthetic remote page bodies; no bot Store/filesystem joins.
    let root_record = export
        .documents
        .iter()
        .find(|r| {
            serde_json::from_str::<Value>(&r.content)
                .unwrap()
                .get("scope")
                == Some(&json!(export.manifest.scope))
        })
        .unwrap();
    if let telegram_archive_bot::notion::RecordData::DailyManifest { coverage, .. } =
        &root_record.data
    {
        assert_eq!(
            coverage.published, 1,
            "verified activity publication must have an honest count"
        );
    } else {
        panic!("manifest record expected");
    }
    let key = root_record.plan(&config.notion)?.publication_key;
    let root_page = remote["pages"]
        .as_object()
        .unwrap()
        .values()
        .find(|p| {
            p["blocks"]
                .to_string()
                .contains(&format!("ARCHIVE_RECORD_V1 {key}"))
        })
        .unwrap();
    let mut events = std::collections::BTreeSet::new();
    visit(&remote, root_page, &mut events)?;
    assert_eq!(events.len(), 1);
    let before = head.manifest_id;
    for _ in 0..3 {
        store.reader_tick(&config, 1)?;
        telegram_archive_bot::notion_worker::tick(&store, &config, &cancel).await?;
    }
    assert_eq!(
        store
            .reader_heads("2026-03-08", "America/Los_Angeles", 100, None)?
            .into_iter()
            .find(|h| h.window_id.is_none())
            .unwrap()
            .manifest_id,
        before,
        "publishing the root cannot feed its own revision loop"
    );
    assert!(!serde_json::to_string(&export)?.contains("SYNTHETIC_NOTION_WORKER_TOKEN"));
    Ok(())
}
