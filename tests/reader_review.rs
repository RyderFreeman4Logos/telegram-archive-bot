//! DA09 review regressions: admission ties, SOP identities, rendered output bounds.
use anyhow::Result;
use serde_json::json;
use std::{fs, os::unix::fs::PermissionsExt, process::Command};
use telegram_archive_bot::{
    activities::*, config::*, digest, reader::*, sources::*, storage::Store,
};
fn fixture(
    accounts: usize,
) -> Result<(
    tempfile::TempDir,
    Config,
    Store,
    Vec<telegram_archive_bot::planner::DayPlan>,
)> {
    let root = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()?;
    let mut c = Config {
        state_dir: root.path().join("state"),
        ..Config::default()
    };
    c.daily.enabled = true;
    c.notion.enabled = false;
    c.daily.accounts = (1..=accounts)
        .map(|id| WatchAccount {
            key: format!("synthetic-{id}"),
            user_id: Some(id.to_string()),
            ..WatchAccount::default()
        })
        .collect();
    let store = Store::open(&c.state_dir)?;
    let w = store
        .backfill_daily(
            &c,
            "2026-03-08".parse()?,
            "2026-03-08".parse()?,
            1_790_000_000,
        )?
        .windows;
    Ok((root, c, store, w))
}
fn input(
    w: &telegram_archive_bot::planner::DayPlan,
    key: &str,
    body: Option<&str>,
) -> Result<ActivityCapture> {
    Ok(ActivityCapture {
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
        observation_key: key.into(),
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
            content: body.map(|body| Content {
                body: body.into(),
                metadata: ContentMetadata::default(),
            }),
            status: if body.is_some() {
                FetchStatus::Complete
            } else {
                FetchStatus::Missing
            },
            extractor: "synthetic".into(),
            evidence: json!({}),
            raw_digest: None,
            relations: vec![],
        },
        view_policy: ViewPolicy {
            max_nodes: 10,
            max_depth: 3,
        },
    })
}
fn settle(store: &Store, c: &Config) -> Result<()> {
    for _ in 0..10 {
        assert_eq!(store.reader_tick(c, 1)?.errors, 0);
    }
    Ok(())
}
fn combined(store: &Store, c: &Config) -> Result<PortableExport> {
    let h = store
        .reader_heads("2026-03-08", "America/Los_Angeles", 100, None)?
        .into_iter()
        .find(|h| h.window_id.is_none())
        .unwrap();
    store.reader_export(c, &h.manifest_id)
}
#[test]
fn equal_second_latest_admission_wins_even_when_digest_order_prefers_missing() -> Result<()> {
    let (_root, c, store, w) = fixture(1)?;
    let w = &w[0];
    let sample = input(w, "a", None)?;
    let activity = Activity {
        identity: ActivityIdentity::ProviderId {
            provider: "x".into(),
            event_id: "8".into(),
        },
        actor: sample.actor.clone(),
        kind: sample.kind,
        source: sample.capture.identity.clone(),
    };
    let mut keys = ["a", "b"]
        .map(|key| {
            Ok((
                digest(serde_json::to_vec(&(
                    activity.id()?,
                    sample.discovery.id()?,
                    key,
                ))?),
                key,
            ))
        })
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    keys.sort(); // Two fixed keys, deliberately opposite hash/admission order; no retry/random search.
    let old = store.record_activity(&input(w, keys[0].1, None)?)?;
    settle(&store, &c)?;
    let before = combined(&store, &c)?.manifest_id;
    let hydrated = input(w, keys[1].1, Some("Hydrated in the very same second"))?;
    let new = store.record_activity(&hydrated)?;
    assert!(old.id < new.id);
    assert_eq!(old.retrieved_at, new.retrieved_at);
    assert_eq!(
        store.record_activity(&hydrated)?,
        new,
        "identical retry does not create another admission"
    );
    settle(&store, &c)?;
    let export = combined(&store, &c)?;
    assert_ne!(
        export.manifest_id, before,
        "same-second hydration must produce a successor"
    );
    assert_eq!(export.activities[0].observation_id, new.id);
    assert_eq!(
        export.sources.values().next().unwrap().body,
        "Hydrated in the very same second"
    );
    let conn = rusqlite::Connection::open(c.state_dir.join("archive.sqlite"))?;
    let query = "SELECT id FROM activity_observations WHERE activity_id=?1 AND json_extract(manifest,'$.discovery.provenance.window_id')=?2 ORDER BY json_extract(manifest,'$.retrieved_at') DESC,source_observation_id DESC,id DESC LIMIT 1";
    let plan = conn
        .prepare(&format!("EXPLAIN QUERY PLAN {query}"))?
        .query_map(rusqlite::params![new.activity.id()?, w.id], |r| {
            r.get::<_, String>(3)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .join("\n");
    assert!(
        plan.contains("reader_activity_latest") && !plan.contains("TEMP B-TREE"),
        "latest scoped lookup must remain indexed: {plan}"
    );
    Ok(())
}
#[test]
fn page_id_only_sop_configures_both_handoffs_and_accepts_complete_layer() -> Result<()> {
    let (_root, mut c, store, _) = fixture(1)?;
    c.reader.sop.page_id = Some("22222222-2222-2222-2222-222222222222".into());
    c.validate()?;
    settle(&store, &c)?;
    let heads = store.reader_heads("2026-03-08", "America/Los_Angeles", 100, None)?;
    assert_eq!(heads.len(), 2);
    for head in heads {
        let export = store.reader_export(&c, &head.manifest_id)?;
        assert!(
            export.manifest.sop_configured,
            "validated SOP page ID must configure window and combined handoffs"
        );
        let m = &export.manifest;
        let record = export
            .documents
            .iter()
            .find(|d| d.content == serde_json::to_string(m).unwrap())
            .unwrap();
        let checkpoint: LayerCheckpoint = serde_json::from_value(
            json!({"contract":"daily-reader-v1","session_id":"id-only","layer":1,"specification_version":"1.0.0","manifest_id":head.manifest_id,"input_manifest_sha256":record.id,"local_date":m.local_date,"timezone":m.timezone,"utc_start":m.utc_start,"utc_end":m.utc_end,"sop":m.sop,"state":"complete","outputs":[{"page_id":"11111111-1111-1111-1111-111111111111","sha256":"a".repeat(64)}],"cursor":null,"processed_activity_ids":[],"readings":[],"unresolved_sources":[],"provenance":{"worker":"synthetic reader","model":null,"version":"fixture-v1","method":"enumerated explicit empty available corpus"}}),
        )?;
        assert_eq!(
            store
                .record_reader_checkpoint(&c, &checkpoint, false)?
                .state,
            LayerState::Complete
        );
    }
    Ok(())
}
fn rendered_bound(format: &str) -> Result<()> {
    let (root, mut c, store, w) = fixture(1)?;
    store.record_activity(&input(
        &w[0],
        "body",
        Some(&"Captured original paragraph.\n".repeat(400)),
    )?)?;
    settle(&store, &c)?;
    let export = combined(&store, &c)?;
    let compact = serde_json::to_vec(&export)?.len();
    assert!(compact >= 16384);
    c.reader.export_bytes = compact;
    c.validate()?;
    store.reader_export(&c, &export.manifest_id)?; // Compact fits exactly.
    let path = root.path().join("config.toml");
    fs::write(&path, toml::to_string(&c)?)?;
    let output = Command::new(env!("CARGO_BIN_EXE_telegram-archive-bot"))
        .arg("--config")
        .arg(&path)
        .args(["reader-export", &export.manifest_id, "--format", format])
        .output()?;
    assert!(
        !output.status.success(),
        "actual {format} exceeds compact allowance ({compact} versus {} emitted bytes)",
        output.stdout.len()
    );
    assert!(
        output.stdout.is_empty(),
        "oversize export must fail before publishing any stdout"
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("bound"));
    let rendered = if format == "json" {
        export.render_json(1_000_000)?
    } else {
        export.markdown(1_000_000)?
    };
    assert!(rendered.len() > compact);
    if format == "json" {
        assert!(export.render_json(rendered.len() - 1).is_err());
        assert_eq!(export.render_json(rendered.len())?, rendered);
    } else {
        assert!(export.markdown(rendered.len() - 1).is_err());
        assert_eq!(export.markdown(rendered.len())?, rendered);
    }
    c.reader.export_bytes = rendered.len();
    fs::write(&path, toml::to_string(&c)?)?;
    let accepted = Command::new(env!("CARGO_BIN_EXE_telegram-archive-bot"))
        .arg("--config")
        .arg(&path)
        .args(["reader-export", &export.manifest_id, "--format", format])
        .output()?;
    assert!(
        accepted.status.success(),
        "exact rendered cap must be accepted: {}",
        String::from_utf8_lossy(&accepted.stderr)
    );
    assert_eq!(accepted.stdout, rendered.as_bytes());
    Ok(())
}
#[test]
fn pretty_json_limit_applies_before_stdout() -> Result<()> {
    rendered_bound("json")
}
#[test]
fn markdown_limit_applies_before_stdout() -> Result<()> {
    rendered_bound("markdown")
}
#[test]
fn selected_duplicate_event_variants_retain_every_pinned_revision_body() -> Result<()> {
    let (_root, mut c, store, w) = fixture(2)?;
    c.reader.sop.page_id = Some("22222222-2222-2222-2222-222222222222".into());
    let first = store.record_activity(&input(&w[0], "earlier", Some("Older selected body"))?)?;
    let mut later = input(&w[1], "later", Some("Newer selected body"))?;
    // Overlapping discovery windows can name the same authoritative event/actor.
    // The canonical event remains identical; only its observation/view/body differs.
    later.actor = first.activity.actor.clone();
    later.retrieved_at += 1;
    let second = store.record_activity(&later)?;
    assert_eq!(first.activity.id()?, second.activity.id()?);
    store.capture(&Capture {
        identity: Identity::x("999")?,
        submitted_url: None,
        content: Some(Content {
            body: "Unrelated private fixture body".into(),
            metadata: ContentMetadata::default(),
        }),
        status: FetchStatus::Complete,
        extractor: "fixture".into(),
        evidence: json!({}),
        raw_digest: None,
        relations: vec![],
    })?;
    settle(&store, &c)?;
    let export = combined(&store, &c)?;
    assert_eq!(
        export.activities.len(),
        1,
        "one exhaustive activity, retaining immutable variants in selected documents"
    );
    assert!(
        export
            .sources
            .contains_key(first.revision_id.as_ref().unwrap()),
        "selected older pinned revision body must not disappear behind activity dedup"
    );
    assert!(
        export
            .sources
            .contains_key(second.revision_id.as_ref().unwrap())
    );
    assert_eq!(export.sources.len(), 2, "selected closure only");
    assert_eq!(
        export.sources[first.revision_id.as_ref().unwrap()].body,
        "Older selected body"
    );
    assert!(!serde_json::to_string(&export)?.contains("Unrelated private fixture body"));
    let m = &export.manifest;
    let record = export
        .documents
        .iter()
        .find(|d| d.content == serde_json::to_string(m).unwrap())
        .unwrap();
    let reading = |revision: &str, body: &str| SourceRead {
        source_id: first.activity.source.id(),
        revision_id: Some(revision.into()),
        state: ReadState::FullyRead,
        reason: String::new(),
        evidence: Some(ReadEvidence {
            kind: EvidenceKind::CapturedText,
            sha256: digest(body),
            reader: "synthetic reader".into(),
            method: "read selected immutable text".into(),
        }),
    };
    let mut checkpoint: LayerCheckpoint = serde_json::from_value(json!({
        "contract":"daily-reader-v1","session_id":"selected-variants","layer":1,
        "specification_version":"1.0.0","manifest_id":export.manifest_id,
        "input_manifest_sha256":record.id,"local_date":m.local_date,
        "timezone":m.timezone,"utc_start":m.utc_start,"utc_end":m.utc_end,"sop":m.sop,
        "state":"complete","outputs":[{"page_id":"11111111-1111-1111-1111-111111111111","sha256":"a".repeat(64)}],
        "cursor":null,"processed_activity_ids":[first.activity.id()?],
        "readings":[reading(second.revision_id.as_ref().unwrap(), "Newer selected body")],
        "unresolved_sources":[],"provenance":{"worker":"synthetic reader","model":null,
        "version":"fixture-v1","method":"read selected immutable corpus"}
    }))?;
    assert!(
        store
            .record_reader_checkpoint(&c, &checkpoint, false)
            .is_err(),
        "a complete layer must account for the selected older variant as well as the deduplicated event"
    );
    checkpoint.readings.push(reading(
        first.revision_id.as_ref().unwrap(),
        "Older selected body",
    ));
    assert_eq!(
        store
            .record_reader_checkpoint(&c, &checkpoint, false)?
            .state,
        LayerState::Complete,
        "both selected pinned revisions are valid assessment inputs"
    );
    checkpoint.readings.push(SourceRead {
        source_id: Identity::x("999")?.id(),
        revision_id: None,
        state: ReadState::OutOfScope,
        reason: "unrelated private source must stay outside the selected closure".into(),
        evidence: None,
    });
    assert!(
        store
            .record_reader_checkpoint(&c, &checkpoint, false)
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn equal_second_latest_body_survives_semantic_git_restore() -> Result<()> {
    let (root, mut c, store, w) = fixture(1)?;
    let sample = input(&w[0], "a", None)?;
    let activity = Activity {
        identity: ActivityIdentity::ProviderId {
            provider: "x".into(),
            event_id: "8".into(),
        },
        actor: sample.actor.clone(),
        kind: sample.kind,
        source: sample.capture.identity.clone(),
    };
    let mut keys = ["a", "b"]
        .map(|key| {
            Ok((
                digest(serde_json::to_vec(&(
                    activity.id()?,
                    sample.discovery.id()?,
                    key,
                ))?),
                key,
            ))
        })
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    keys.sort();
    let old = store.record_activity(&input(&w[0], keys[1].1, None)?)?;
    let new = store.record_activity(&input(
        &w[0],
        keys[0].1,
        Some("Latest body survives semantic restore"),
    )?)?;
    assert!(old.id > new.id);
    assert!(new.source_observation_id > old.source_observation_id);
    settle(&store, &c)?;
    assert_eq!(combined(&store, &c)?.activities[0].observation_id, new.id);
    c.archive.directory = Some(root.path().join("archive"));
    c.archive.branch = Some("main".into());
    c.archive.push = false;
    let cancel = tokio_util::sync::CancellationToken::new();
    store.project(&c, 100, &cancel).await?;
    let restored = root.path().join("restored");
    assert!(
        Store::restore_git(
            c.archive.directory.as_ref().unwrap(),
            &restored,
            &c,
            &cancel
        )
        .await?
        .external_side_effects_paused
    );
    let restored_store = Store::open(&restored)?;
    let conn = rusqlite::Connection::open(restored.join("archive.sqlite"))?;
    let old_row: i64 = conn.query_row(
        "SELECT rowid FROM activity_observations WHERE id=?1",
        [&old.id],
        |r| r.get(0),
    )?;
    let new_row: i64 = conn.query_row(
        "SELECT rowid FROM activity_observations WHERE id=?1",
        [&new.id],
        |r| r.get(0),
    )?;
    assert!(
        old_row > new_row,
        "actual Git restore reverses incidental rowid order"
    );
    drop(conn);
    restored_store.release_recovery_hold()?;
    restored_store.backfill_daily(
        &c,
        "2026-03-08".parse()?,
        "2026-03-08".parse()?,
        1_790_000_000,
    )?;
    settle(&restored_store, &c)?;
    assert_eq!(
        combined(&restored_store, &c)?.activities[0].observation_id,
        new.id,
        "semantic restore must preserve latest same-second source observation"
    );
    Ok(())
}
