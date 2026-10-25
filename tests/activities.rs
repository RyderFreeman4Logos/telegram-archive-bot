//! DA01 / #68: synthetic account/activity persistence, never provider traffic.
use anyhow::Result;
use serde_json::json;
use std::{fs::Permissions, os::unix::fs::PermissionsExt};
use telegram_archive_bot::{activities::*, digest, sources::*, storage::Store};

fn event(actor: &str, id: Option<&str>, source: &str, body: Option<&str>) -> ActivityCapture {
    ActivityCapture {
        actor: AccountIdentity::ProviderId {
            provider: "x".into(),
            user_id: actor.into(),
        },
        handle: Some(format!("handle_{actor}")),
        display_name: Some(format!("Actor {actor}")),
        provider_event_id: id.map(str::to_owned),
        kind: ActivityKind::Original,
        discovery: DiscoveryRun {
            provider: "x".into(),
            key: "run-1".into(),
            provenance: json!({"fixture": true}),
        },
        occurrence_key: format!("item-{actor}-{source}"),
        observation_key: "retrieval-1".into(),
        activity_time: TimeEvidence::Unknown {
            provenance: json!({"reason":"not exposed"}),
        },
        source_time: TimeEvidence::Instant {
            timestamp: Timestamp::seconds(100),
            precision: TimePrecision::Second,
            provenance: json!({"field":"created_at"}),
        },
        retrieved_at: 1000,
        provenance: json!({"path":"synthetic timeline", "counter": 1}),
        capture: Capture {
            identity: Identity::x(source).unwrap(),
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
            extractor: "da01-synthetic".into(),
            evidence: json!({"fixture":true}),
            raw_digest: None,
            relations: vec![],
        },
        view_policy: ViewPolicy {
            max_nodes: 10,
            max_depth: 5,
        },
    }
}

#[test]
fn da01_ordering_retries_and_renames_preserve_sources_and_distinct_activities() -> Result<()> {
    let mut original = event("10", Some("100"), "100", Some("A"));
    original.activity_time = TimeEvidence::Instant {
        timestamp: Timestamp::seconds(100),
        precision: TimePrecision::Second,
        provenance: json!({"provider":true}),
    };
    let mut quote = event("20", Some("200"), "200", Some("B quotes A"));
    quote.kind = ActivityKind::Quote;
    quote.capture.relations.push(Related {
        target: Identity::x("100")?,
        kind: RelationKind::Quotes,
    });
    let mut reply = event("30", Some("300"), "300", Some("C replies A"));
    reply.kind = ActivityKind::Reply;
    reply.capture.relations.push(Related {
        target: Identity::x("100")?,
        kind: RelationKind::RepliesTo,
    });
    let mut repost = event("40", Some("400"), "100", Some("A"));
    repost.kind = ActivityKind::Repost;
    repost.activity_time = TimeEvidence::Interval {
        earliest: Timestamp::seconds(900),
        latest: Timestamp::seconds(950),
        precision: TimePrecision::Second,
        provenance: json!({"between":"two retrievals"}),
    };
    let mut other = event("50", Some("500"), "100", Some("A"));
    other.kind = ActivityKind::Repost;
    let events = [original, quote, reply, repost, other];
    fn permutations(items: &mut [usize], offset: usize, out: &mut Vec<Vec<usize>>) {
        if offset == items.len() {
            out.push(items.to_vec());
            return;
        }
        for index in offset..items.len() {
            items.swap(offset, index);
            permutations(items, offset + 1, out);
            items.swap(offset, index);
        }
    }
    let mut orders = vec![];
    permutations(&mut [0, 1, 2, 3, 4], 0, &mut orders);
    assert_eq!(orders.len(), 120);
    for order in orders {
        let dir = temp()?;
        let store = Store::open(dir.path())?;
        for i in order {
            store.record_activity(&events[i])?;
        }
        for input in &events {
            store.record_activity(input)?;
        }
        assert_eq!(store.activity_counts()?, (5, 5, 5));
        assert_eq!(store.source_counts()?, (3, 3, 5));
        let view = store.create_view(
            &Identity::x("200")?,
            ViewPolicy {
                max_nodes: 10,
                max_depth: 5,
            },
        )?;
        assert_eq!(view.members.len(), 2);
        assert_eq!(view.edges[0].kind, "quotes");
        let reply_view = store.create_view(
            &Identity::x("300")?,
            ViewPolicy {
                max_nodes: 10,
                max_depth: 5,
            },
        )?;
        assert_eq!(reply_view.edges[0].kind, "replies_to");
        for input in &events {
            let observed = store.record_activity(input)?;
            assert_eq!(store.activity(&observed.activity.id()?)?, observed.activity);
            assert_eq!(observed.activity.kind, input.kind);
            assert_eq!(observed.activity.source, input.capture.identity);
        }
        let first = store.record_activity(&events[3])?;
        assert_eq!(first.source_time, events[3].source_time);
        assert_eq!(first.activity_time, events[3].activity_time);
        assert!(matches!(
            store.record_activity(&events[4])?.activity_time,
            TimeEvidence::Unknown { .. }
        ));
        let mut renamed = events[3].clone();
        renamed.handle = Some("renamed_actor".into());
        renamed.display_name = Some("Renamed display".into());
        renamed.observation_key = "retrieval-2".into();
        renamed.discovery.key = "run-2".into();
        renamed.provenance = json!({"counter":999, "path":"second discovery"});
        renamed.capture.evidence = json!({"counter":999, "extractor_observation":"new"});
        renamed.capture.submitted_url =
            Some("https://x.com/renamed_source_author/status/100".into());
        let second = store.record_activity(&renamed)?;
        assert_eq!(first.activity.id()?, second.activity.id()?);
        assert_eq!(first.revision_id, second.revision_id);
        assert_eq!(store.account_history(&renamed.actor.id()?)?.len(), 2);
        assert_eq!(store.activity_observations(&first.activity.id()?)?.len(), 2);
        assert_eq!(store.activity_counts()?, (5, 5, 6));
        assert_eq!(store.integrity()?, 0);
    }
    Ok(())
}

#[test]
fn da01_unknown_events_and_accounts_are_not_guessed_or_collapsed() -> Result<()> {
    let dir = temp()?;
    let store = Store::open(dir.path())?;
    let mut input = event("40", None, "100", Some("A"));
    input.kind = ActivityKind::Repost;
    input.actor = AccountIdentity::Unresolved {
        provider: "x".into(),
        key: "timeline-unknown-actor-1".into(),
    };
    let first = store.record_activity(&input)?;
    assert_eq!(store.record_activity(&input)?, first);
    input.occurrence_key = "separate occurrence, same actor and target".into();
    let second = store.record_activity(&input)?;
    input.discovery.key = "another-run".into();
    let third = store.record_activity(&input)?;
    assert_ne!(first.activity.id()?, second.activity.id()?);
    assert_ne!(second.activity.id()?, third.activity.id()?);
    assert_eq!(store.activity_counts()?, (1, 3, 3));
    let mut known = input.clone();
    known.actor = AccountIdentity::ProviderId {
        provider: "x".into(),
        user_id: "40".into(),
    };
    known.provider_event_id = Some("known-event".into());
    store.record_activity(&known)?;
    assert_eq!(store.activity_counts()?.0, 2); // A handle alone never merges uncertain identity.
    assert_eq!(store.source_counts()?.1, 1);
    Ok(())
}

#[test]
fn da01_fallback_occurrence_keys_are_scoped_to_actor_and_source() -> Result<()> {
    let dir = temp()?;
    let store = Store::open(dir.path())?;
    let mut input = event("40", None, "100", Some("A"));
    input.kind = ActivityKind::Repost;
    input.occurrence_key = "page-1-position-1".into();
    let first = store.record_activity(&input)?;
    // Different account timelines can have the same provider page position.
    input.actor = AccountIdentity::ProviderId {
        provider: "x".into(),
        user_id: "50".into(),
    };
    let second = store.record_activity(&input)?;
    assert_ne!(first.activity.id()?, second.activity.id()?);
    // The same ambiguous position can also refer to a different known target.
    input.capture.identity = Identity::x("200")?;
    input.capture.content.as_mut().unwrap().body = "B".into();
    let third = store.record_activity(&input)?;
    assert_ne!(second.activity.id()?, third.activity.id()?);
    assert_eq!(store.activity_counts()?, (2, 3, 3));
    assert_eq!(store.source_counts()?, (2, 2, 3));
    Ok(())
}

#[test]
fn da01_concurrent_retries_converge_and_conflicting_keys_roll_back() -> Result<()> {
    let dir = temp()?;
    let store = Store::open(dir.path())?;
    let input = event("10", Some("100"), "100", Some("A"));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let store = store.clone();
            let input = input.clone();
            std::thread::spawn(move || store.record_activity(&input))
        })
        .collect();
    let results = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect::<Result<Vec<_>>>()?;
    assert!(results.iter().all(|r| r == &results[0]));
    assert_eq!(store.activity_counts()?, (1, 1, 1));
    assert_eq!(store.source_counts()?, (1, 1, 1));
    let mut conflict = input.clone();
    conflict.handle = Some("payload changed under same retry key".into());
    assert!(store.record_activity(&conflict).is_err());
    conflict = input.clone();
    conflict.actor = AccountIdentity::ProviderId {
        provider: "x".into(),
        user_id: "another-actor".into(),
    };
    conflict.observation_key = "new-retrieval".into();
    assert!(store.record_activity(&conflict).is_err());
    conflict = input;
    conflict.activity_time = TimeEvidence::Interval {
        earliest: Timestamp::seconds(2),
        latest: Timestamp::seconds(1),
        precision: TimePrecision::Second,
        provenance: json!({}),
    };
    conflict.observation_key = "invalid-time".into();
    assert!(store.record_activity(&conflict).is_err());
    assert_eq!(store.activity_counts()?, (1, 1, 1));
    assert_eq!(store.source_counts()?, (1, 1, 1));
    Ok(())
}

#[test]
fn da01_edits_and_hydration_append_pins_without_rewriting_snapshots() -> Result<()> {
    let dir = temp()?;
    let store = Store::open(dir.path())?;
    let mut input = event("40", Some("400"), "100", None);
    input.kind = ActivityKind::Repost;
    let unavailable = store.record_activity(&input)?;
    assert_eq!(unavailable.missing_reason.as_deref(), Some("missing"));
    input.capture.content = Some(Content {
        body: "A".into(),
        metadata: ContentMetadata::default(),
    });
    input.capture.status = FetchStatus::Complete;
    input.observation_key = "hydrated".into();
    let original = store.record_activity(&input)?;
    let snapshot = store.view(&original.view_id)?;
    input.capture.content.as_mut().unwrap().body = "A edited".into();
    input.observation_key = "edited".into();
    let edited = store.record_activity(&input)?;
    assert_ne!(original.revision_id, edited.revision_id);
    assert_ne!(original.view_id, edited.view_id);
    assert_eq!(store.view(&original.view_id)?, snapshot);
    assert_eq!(store.activity_observation(&unavailable.id)?, unavailable);
    assert!(
        store.view(&unavailable.view_id)?.members[0]
            .revision_id
            .is_none()
    );
    assert_eq!(store.activity_counts()?, (1, 1, 3));
    assert_eq!(store.source_counts()?, (1, 2, 3));
    Ok(())
}

#[tokio::test]
async fn da01_full_and_semantic_backup_restore_preserve_activity_history_and_holds() -> Result<()> {
    use telegram_archive_bot::config::Config;
    use tokio_util::sync::CancellationToken;
    let dir = temp()?;
    let store = Store::open(dir.path().join("state"))?;
    let mut input = event("40", Some("400"), "100", None);
    input.kind = ActivityKind::Repost;
    let missing = store.record_activity(&input)?;
    input.observation_key = "hydration".into();
    input.capture.content = Some(Content {
        body: "A\r\noriginal".into(),
        metadata: ContentMetadata::default(),
    });
    input.capture.status = FetchStatus::Complete;
    input.activity_time = TimeEvidence::Instant {
        timestamp: Timestamp {
            seconds: 999,
            nanoseconds: 123_000_000,
        },
        precision: TimePrecision::Millisecond,
        provenance: json!({"raw":"999.123"}),
    };
    let hydrated = store.record_activity(&input)?;
    input.observation_key = "edit".into();
    input.handle = Some("new_handle".into());
    input.capture.content.as_mut().unwrap().body = "A edited".into();
    let edited = store.record_activity(&input)?;
    let expected = store.activity_observations(&edited.activity.id()?)?;
    assert_eq!(expected.len(), 3);
    let mut uncertain = event("20", None, "200", Some("An unresolved actor's post"));
    uncertain.actor = AccountIdentity::Unresolved {
        provider: "x".into(),
        key: "synthetic-unknown-actor".into(),
    };
    let uncertain_observation = store.record_activity(&uncertain)?;
    let backup = dir.path().join("backup");
    let manifest = store.backup(&backup)?;
    assert_eq!(manifest.schema_version, 1);
    assert_eq!(manifest.database_schema, 13);
    Store::verify_backup(&backup)?;
    let full = dir.path().join("full-restored");
    assert!(Store::restore_backup(&backup, &full)?.external_side_effects_paused);
    let restored = Store::open(&full)?;
    assert!(restored.recovery_paused()?);
    assert_eq!(
        restored.activity_observations(&edited.activity.id()?)?,
        expected
    );
    assert_eq!(
        restored.account_history(&input.actor.id()?)?,
        store.account_history(&input.actor.id()?)?
    );
    assert_eq!(restored.record_activity(&input)?, edited); // Identical retry remains idempotent after recovery.
    let mut config = Config {
        state_dir: store.root().to_owned(),
        ..Config::default()
    };
    config.archive.directory = Some(dir.path().join("archive"));
    config.archive.branch = Some("main".into());
    config.archive.push = false;
    let cancel = CancellationToken::new();
    store.project(&config, 100, &cancel).await?;
    let semantic = dir.path().join("semantic-restored");
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
    assert!(rebuilt.recovery_paused()?);
    assert_eq!(
        rebuilt.activity_observations(&edited.activity.id()?)?,
        expected
    );
    assert_eq!(
        rebuilt.account_activities(&input.actor.id()?)?,
        vec![edited.activity.clone()]
    );
    assert_eq!(rebuilt.activity_observation(&missing.id)?, missing);
    assert_eq!(
        rebuilt.activity_observation(&uncertain_observation.id)?,
        uncertain_observation
    );
    assert_eq!(
        rebuilt.view(&hydrated.view_id)?,
        store.view(&hydrated.view_id)?
    );
    assert_eq!(
        rebuilt
            .revision(hydrated.revision_id.as_ref().unwrap())?
            .content
            .body,
        "A\r\noriginal"
    );
    assert_eq!(rebuilt.record_activity(&input)?, edited);
    fn table_rows(store: &Store, table: &str) -> Result<Vec<Vec<rusqlite::types::Value>>> {
        let conn = rusqlite::Connection::open(store.root().join("archive.sqlite"))?;
        let mut stmt = conn.prepare(&format!("SELECT * FROM {table} ORDER BY 1"))?;
        let count = stmt.column_count();
        Ok(stmt
            .query_map([], |row| {
                (0..count)
                    .map(|i| row.get(i))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }
    for table in [
        "accounts",
        "account_observations",
        "discovery_runs",
        "activities",
        "activity_observations",
    ] {
        assert_eq!(
            table_rows(&store, table)?,
            table_rows(&restored, table)?,
            "full: {table}"
        );
        assert_eq!(
            table_rows(&store, table)?,
            table_rows(&rebuilt, table)?,
            "semantic: {table}"
        );
    }
    assert_eq!(rebuilt.integrity()?, 0);
    assert!(rebuilt.project(&config, 100, &cancel).await.is_err());
    // Re-export in a different private repository after releasing only this fixture's hold.
    rebuilt.release_recovery_hold()?;
    let original_archive = config.archive.directory.clone().unwrap();
    config.state_dir = semantic;
    config.archive.directory = Some(dir.path().join("re-exported"));
    rebuilt.project(&config, 100, &cancel).await?;
    fn files(root: &std::path::Path) -> Result<std::collections::BTreeMap<String, Vec<u8>>> {
        fn walk(
            base: &std::path::Path,
            dir: &std::path::Path,
            result: &mut std::collections::BTreeMap<String, Vec<u8>>,
        ) -> Result<()> {
            for entry in std::fs::read_dir(dir)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    walk(base, &entry.path(), result)?;
                } else {
                    result.insert(
                        entry.path().strip_prefix(base)?.to_string_lossy().into(),
                        std::fs::read(entry.path())?,
                    );
                }
            }
            Ok(())
        }
        let mut result = std::collections::BTreeMap::new();
        walk(root, &root.join("archive-v1"), &mut result)?;
        Ok(result)
    }
    assert_eq!(
        files(&original_archive)?,
        files(config.archive.directory.as_ref().unwrap())?
    );
    Ok(())
}

fn temp() -> Result<tempfile::TempDir> {
    Ok(tempfile::Builder::new()
        .permissions(Permissions::from_mode(0o700))
        .tempdir()?)
}

#[test]
fn da01_schema_upgrade_preserves_sources_views() -> Result<()> {
    let dir = temp()?;
    let source = Identity::x("100")?;
    let content = Content {
        body: "An immutable pre-DA01 source".into(),
        metadata: ContentMetadata::default(),
    };
    let hash = digest(serde_json::to_vec(&content)?);
    let revision = digest(format!("{}:{hash}", source.id()));
    let view = View {
        schema_version: 1,
        root_id: source.id(),
        policy: ViewPolicy {
            max_nodes: 10,
            max_depth: 10,
        },
        members: vec![ViewMember {
            source_id: source.id(),
            revision_id: Some(revision.clone()),
            missing_reason: None,
        }],
        edges: vec![],
        omissions: vec![],
        retrieval: None,
    };
    let conn = rusqlite::Connection::open(dir.path().join("archive.sqlite"))?;
    for migration in [
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
    ] {
        conn.execute_batch(migration)?;
    }
    conn.execute(
        "INSERT INTO sources(id,provider,external_id) VALUES(?1,'x','100')",
        [source.id()],
    )?;
    conn.execute(
        "INSERT INTO source_revisions VALUES(?1,?2,?3,?4,?5)",
        rusqlite::params![
            revision,
            source.id(),
            hash,
            content.body,
            serde_json::to_string(&content.metadata)?
        ],
    )?;
    conn.execute("UPDATE sources SET current_revision=?1", [&revision])?;
    conn.execute(
        "INSERT INTO views VALUES(?1,?2,?3)",
        rusqlite::params![view.id()?, source.id(), serde_json::to_string(&view)?],
    )?;
    conn.execute(
        "INSERT INTO view_members VALUES(?1,0,?2,?3,NULL)",
        rusqlite::params![view.id()?, source.id(), revision],
    )?;
    conn.pragma_update(None, "user_version", 12)?;
    drop(conn);
    // A real supported old-schema backup must also migrate through restore,
    // before any current Store::open can upgrade the original fixture.
    let old_backup = temp()?;
    std::fs::create_dir(old_backup.path().join("blobs"))?;
    std::fs::set_permissions(
        old_backup.path().join("blobs"),
        Permissions::from_mode(0o700),
    )?;
    let old_db = std::fs::read(dir.path().join("archive.sqlite"))?;
    std::fs::write(old_backup.path().join("archive.sqlite"), &old_db)?;
    let manifest = telegram_archive_bot::recovery::BackupManifest {
        schema_version: 1,
        database_schema: 12,
        configuration_schema: 1,
        created_at: 0,
        database: telegram_archive_bot::storage::Blob {
            digest: digest(&old_db),
            size: old_db.len() as u64,
        },
        blobs: vec![],
        credentials_included: false,
        caches_included: false,
    };
    std::fs::write(
        old_backup.path().join("manifest.json"),
        serde_json::to_vec(&manifest)?,
    )?;
    Store::verify_backup(old_backup.path())?;
    let restored_parent = temp()?;
    let destination = restored_parent.path().join("restored");
    assert!(Store::restore_backup(old_backup.path(), &destination)?.external_side_effects_paused);
    let restored = Store::open(&destination)?;
    assert_eq!(restored.revision(&revision)?.content, content);
    assert_eq!(restored.view(&view.id()?)?, view);
    assert_eq!(restored.activity_counts()?, (0, 0, 0));
    assert!(restored.recovery_paused()?);
    assert_eq!(restored.integrity()?, 0);
    let store = Store::open(dir.path())?;
    let conn = rusqlite::Connection::open(dir.path().join("archive.sqlite"))?;
    assert_eq!(
        conn.pragma_query_value(None, "user_version", |r| r.get::<_, u32>(0))?,
        13
    );
    assert_eq!(store.revision(&revision)?.content, content);
    assert_eq!(store.view(&view.id()?)?, view);
    assert_eq!(store.source_counts()?, (1, 1, 0));
    assert_eq!(store.integrity()?, 0);
    Ok(())
}
