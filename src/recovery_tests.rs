use super::*;
use crate::{
    audio::{AudioFinalizer, AudioInput, Profile, Repair},
    delivery::{Delivery, DeliveryWorker},
    jobs::{Lease, Phase},
    sources::*,
    telegram::{Telegram, Update},
};
use serde_json::json;
use std::{process::Command, sync::mpsc};

fn fixture() -> Result<(tempfile::TempDir, Store, Config)> {
    let root = tempfile::Builder::new()
        .prefix("archive-recovery-")
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in("/var/tmp")?;
    let state = root.path().join("state");
    let store = Store::open(&state)?;
    let mut config = Config {
        state_dir: state,
        ..Config::default()
    };
    config.telegram.bot_id = 1;
    config.telegram.allowed_chats = vec![2];
    config.telegram.allowed_users = vec![3];
    config.archive.directory = Some(root.path().join("archive"));
    config.archive.branch = Some("main".into());
    Ok((root, store, config))
}
fn capture(id: &str, text: &str) -> Capture {
    Capture {
        identity: Identity::x(id).unwrap(),
        submitted_url: Some(format!("https://x.com/a/status/{id}")),
        content: Some(Content {
            body: text.into(),
            metadata: ContentMetadata::default(),
        }),
        status: FetchStatus::Complete,
        extractor: "recovery-fixture-v1".into(),
        evidence: json!({"synthetic":true}),
        raw_digest: None,
        relations: vec![],
    }
}
fn seed(store: &Store) -> Result<String> {
    let raw = store.publish_bytes(b"retained raw payload")?;
    let mut a = capture("100", "Original A\r\n二行\rthird");
    a.raw_digest = Some(raw.digest);
    let a_id = store.capture(&a)?.unwrap();
    let mut b = capture("200", "B quotes A");
    b.relations.push(Related {
        target: a.identity.clone(),
        kind: RelationKind::Quotes,
    });
    store.capture(&b)?;
    store.create_view(
        &b.identity,
        ViewPolicy {
            max_nodes: 10,
            max_depth: 4,
        },
    )?;
    store.capture(&capture("100", "Revised A"))?;
    Ok(a_id)
}
fn lease(store: &Store, config: &Config) -> Result<Lease> {
    let u: Update = serde_json::from_value(
        json!({"update_id":1,"message":{"message_id":1,"chat":{"id":2},"from":{"id":3},"text":"synthetic recovery request"}}),
    )?;
    store.ingest(config, &u, &json!({"fixture":"recovery"}))?;
    Ok(store.claim_phase(Phase::Fetch)?.unwrap())
}
fn rows(store: &Store, table: &str) -> Result<Vec<Vec<Value>>> {
    let conn = store.connection()?;
    let mut stmt = conn.prepare(&format!("SELECT * FROM {table} ORDER BY 1,2"))?;
    let n = stmt.column_count();
    Ok(stmt
        .query_map([], |r| {
            (0..n)
                .map(|i| {
                    Ok(match r.get_ref(i)? {
                        rusqlite::types::ValueRef::Null => Value::Null,
                        rusqlite::types::ValueRef::Integer(i) => json!(i),
                        rusqlite::types::ValueRef::Real(x) => json!(x),
                        rusqlite::types::ValueRef::Text(s) => json!(String::from_utf8_lossy(s)),
                        rusqlite::types::ValueRef::Blob(b) => json!(digest(b)),
                    })
                })
                .collect::<rusqlite::Result<Vec<_>>>()
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}
fn tree(root: &Path) -> Result<BTreeMap<String, Vec<u8>>> {
    fn walk(base: &Path, dir: &Path, out: &mut BTreeMap<String, Vec<u8>>) -> Result<()> {
        for e in fs::read_dir(dir)? {
            let e = e?;
            if e.file_type()?.is_dir() {
                walk(base, &e.path(), out)?;
            } else {
                out.insert(
                    e.path().strip_prefix(base)?.to_string_lossy().into(),
                    fs::read(e.path())?,
                );
            }
        }
        Ok(())
    }
    let mut out = BTreeMap::new();
    walk(root, &root.join("archive-v1"), &mut out)?;
    Ok(out)
}
#[tokio::test]
async fn full_backup_restores_all_retained_rows_and_fences_external_effects() -> Result<()> {
    let (root, store, config) = fixture()?;
    seed(&store)?;
    let lease = lease(&store, &config)?;
    let ids = store.enqueue_deliveries(
        &lease,
        &[
            Delivery::Text {
                text: "first".into(),
            },
            Delivery::Text {
                text: "second".into(),
            },
        ],
    )?;
    store.connection()?.execute(
        "UPDATE outbox SET state='sending',attempt=1,lease_until=?1 WHERE id=?2",
        params![now() + 100, ids[0]],
    )?;
    store.connection()?.execute(
        "INSERT INTO delivery_attempts VALUES('in-flight',?1,1,?2,NULL,'sending',0,NULL,NULL)",
        params![ids[0], now()],
    )?;
    let before: BTreeMap<_, _> = [
        "sources",
        "source_revisions",
        "observations",
        "source_aliases",
        "relations",
        "views",
        "view_members",
        "blobs",
        "config_snapshots",
    ]
    .into_iter()
    .map(|t| Ok((t, rows(&store, t)?)))
    .collect::<Result<_>>()?;
    let backup = root.path().join("backup");
    let manifest = store.backup(&backup)?;
    assert!(!manifest.credentials_included && !manifest.caches_included);
    assert_eq!(Store::verify_backup(&backup)?.blobs, manifest.blobs);
    let destination = root.path().join("restored");
    let report = Store::restore_backup(&backup, &destination)?;
    assert_eq!(report.mode, "full");
    assert!(report.external_side_effects_paused);
    let restored = Store::open(&destination)?;
    for (table, expected) in before {
        assert_eq!(expected, rows(&restored, table)?, "{table}");
    }
    assert!(restored.claim_job()?.is_none());
    assert!(restored.heartbeat(&lease).is_err());
    let states: Vec<String> = restored
        .connection()?
        .prepare("SELECT state FROM outbox ORDER BY ordinal")?
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    assert_eq!(states, ["unknown", "paused"]);
    assert_eq!(
        restored.connection()?.query_row(
            "SELECT state FROM delivery_attempts WHERE id='in-flight'",
            [],
            |r| r.get::<_, String>(0)
        )?,
        "unknown"
    );
    let worker = DeliveryWorker {
        store: restored.clone(),
        telegram: Telegram::new("synthetic-not-a-real-token".into())?,
        bot_id: 1,
    };
    assert!(!worker.send_next(&CancellationToken::new()).await?);
    let mut restored_config = config.clone();
    restored_config.state_dir = destination.clone();
    restored_config.archive.directory = Some(root.path().join("restored-archive"));
    assert!(
        restored
            .project(&restored_config, 100, &CancellationToken::new())
            .await
            .is_err()
    );
    assert!(!restored_config.archive.directory.as_ref().unwrap().exists());
    restored.release_recovery_hold()?;
    assert!(!restored.recovery_paused()?);
    assert!(restored.claim_job()?.is_none());
    assert!(!worker.send_next(&CancellationToken::new()).await?);
    Ok(())
}
#[test]
fn backup_corruption_missing_objects_and_destination_collisions_fail_safely() -> Result<()> {
    let (root, store, _) = fixture()?;
    seed(&store)?;
    let backup = root.path().join("backup");
    let manifest = store.backup(&backup)?;
    let blob = &manifest.blobs[0];
    let path = backup.join("blobs").join(&blob.digest);
    let saved = fs::read(&path)?;
    fs::remove_file(&path)?;
    let error = Store::verify_backup(&backup).unwrap_err().to_string();
    assert!(error.contains(&blob.digest));
    assert!(Store::restore_backup(&backup, &root.path().join("missing-restore")).is_err());
    assert!(!root.path().join("missing-restore").exists());
    fs::write(&path, vec![b'X'; saved.len()])?;
    assert!(
        Store::verify_backup(&backup)
            .unwrap_err()
            .to_string()
            .contains(&blob.digest)
    );
    fs::write(&path, saved)?;
    Store::verify_backup(&backup)?;
    let existing = root.path().join("existing");
    private_dir(&existing)?;
    fs::write(existing.join("sentinel"), "preserve")?;
    assert!(Store::restore_backup(&backup, &existing).is_err());
    assert_eq!(fs::read(existing.join("sentinel"))?, b"preserve");
    assert!(store.backup(&backup).is_err());
    let db = backup.join("archive.sqlite");
    let mut bytes = fs::read(&db)?;
    bytes[100] ^= 1;
    fs::write(db, bytes)?;
    assert!(
        Store::verify_backup(&backup)
            .unwrap_err()
            .to_string()
            .contains("database digest")
    );
    Ok(())
}
#[test]
fn online_backup_snapshot_coexists_with_canonical_writers_and_pins_publication() -> Result<()> {
    let (root, store, _) = fixture()?;
    let first = store.publish_bytes(b"existing object")?;
    store.capture(&capture("100", "Before snapshot"))?;
    let writer = store.clone();
    let (start_tx, start_rx) = mpsc::channel();
    let (changed_tx, changed_rx) = mpsc::channel();
    let (published_tx, published_rx) = mpsc::channel();
    let task = std::thread::spawn(move || -> Result<()> {
        start_rx.recv()?;
        let mut capture = capture("200", "After snapshot");
        capture.raw_digest = Some(first.digest);
        writer.capture(&capture)?;
        changed_tx.send(())?;
        writer.publish_bytes(b"new object after snapshot")?;
        published_tx.send(())?;
        Ok(())
    });
    let backup = root.path().join("online");
    let manifest = store.backup_inner(&backup, || {
        start_tx.send(()).unwrap();
        changed_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(published_rx.try_recv().is_err());
    })?;
    task.join().unwrap()?;
    assert!(published_rx.recv_timeout(Duration::from_secs(10)).is_ok());
    assert_eq!(manifest.blobs.len(), 1);
    assert_eq!(store.source_counts()?.0, 2);
    let restored = root.path().join("online-restored");
    let report = Store::restore_backup(&backup, &restored)?;
    assert_eq!(report.sources, 1);
    assert_eq!(report.retained_blobs, 1);
    Ok(())
}
#[tokio::test]
async fn git_only_rebuild_preserves_semantics_and_reports_unavailable_bytes() -> Result<()> {
    let (root, store, config) = fixture()?;
    let original = seed(&store)?;
    let lease = lease(&store, &config)?;
    let text = store.publish_bytes("原样译文\n".as_bytes())?;
    let key = digest("synthetic translated text");
    store.publish_derivative(&lease,&key,"translation",&original,&json!({"schema_version":1,"source_revision":original,"output_digest":text.digest,"semantic_fidelity_verified":false}))?;
    let audio_path = root.path().join("sine.mp3");
    let result = Command::new("/usr/bin/ffmpeg")
        .args([
            "-v",
            "error",
            "-nostdin",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:duration=3",
            "-c:a",
            "libmp3lame",
            "-b:a",
            "96k",
        ])
        .arg(&audio_path)
        .output()?;
    ensure!(result.status.success(), "synthetic audio generation failed");
    let input = AudioInput {
        blob_digest: store.publish_bytes(&fs::read(&audio_path)?)?.digest,
        parent_id: original.clone(),
        producer_tools: json!({"fixture":"sine"}),
        order: 0,
        subtitle_digest: None,
    };
    let artifact = AudioFinalizer {
        store: store.clone(),
    }
    .finalize(
        &lease,
        &original,
        &[input],
        Profile::AudioTrack,
        Repair::Reject,
        &CancellationToken::new(),
    )
    .await?
    .remove(0);
    let cancel = CancellationToken::new();
    store.project(&config, 100, &cancel).await?;
    let archive = config.archive.directory.as_ref().unwrap();
    let exported = tree(archive)?;
    let destination = root.path().join("semantic-restored");
    let report = Store::restore_git(archive, &destination, &config, &cancel).await?;
    assert_eq!(report.mode, "git-only");
    assert!(
        report
            .limitations
            .iter()
            .any(|s| s.contains("delivery receipts"))
    );
    assert!(
        report
            .checkpoint
            .as_ref()
            .unwrap()
            .contains("Archive-Events:")
    );
    let rebuilt = Store::open(&destination)?;
    assert!(rebuilt.recovery_paused()?);
    for table in [
        "sources",
        "source_revisions",
        "source_aliases",
        "relations",
        "views",
        "view_members",
    ] {
        assert_eq!(rows(&store, table)?, rows(&rebuilt, table)?, "{table}");
    }
    assert_eq!(
        rebuilt.revision(&original)?.content.body,
        "Original A\r\n二行\rthird"
    );
    assert!(rebuilt.derivative(&key)?.is_none());
    let derivative = rebuilt.semantic_derivative(&key)?.unwrap();
    assert_eq!(derivative["manifest"]["output_digest"], text.digest);
    assert!(rebuilt.artifact(&artifact.artifact_id).is_err());
    assert_eq!(
        rebuilt
            .connection()?
            .query_row("SELECT count(*) FROM jobs", [], |r| r.get::<_, u64>(0))?,
        0
    );
    rebuilt.release_recovery_hold()?;
    let mut output_config = config.clone();
    output_config.state_dir = destination;
    output_config.archive.directory = Some(root.path().join("re-exported"));
    rebuilt.project(&output_config, 100, &cancel).await?;
    assert_eq!(
        exported,
        tree(output_config.archive.directory.as_ref().unwrap())?
    );
    Ok(())
}
#[tokio::test]
async fn semantic_import_rejects_uncommitted_body_tampering_without_publishing() -> Result<()> {
    let (root, store, config) = fixture()?;
    let revision = seed(&store)?;
    let cancel = CancellationToken::new();
    store.project(&config, 100, &cancel).await?;
    let archive = config.archive.directory.as_ref().unwrap();
    let body = archive.join(format!("archive-v1/revisions/{revision}/body.md"));
    fs::write(body, "corrupt changed body")?;
    let destination = root.path().join("refused");
    let error = Store::restore_git(archive, &destination, &config, &cancel)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("differs from committed checkpoint"),
        "{error}"
    );
    assert!(!destination.exists());
    Ok(())
}
