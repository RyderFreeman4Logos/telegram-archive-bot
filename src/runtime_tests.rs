use super::*;
use crate::jobs::{JobState, Scope};
use std::{fs, os::unix::fs::PermissionsExt};
fn fixture() -> Result<(tempfile::TempDir, Store, Config)> {
    let root = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .prefix("archive-runtime-")
        .tempdir_in("/var/tmp")?;
    let store = Store::open(root.path().join("state"))?;
    let mut config = Config {
        state_dir: store.root().to_owned(),
        ..Default::default()
    };
    config.telegram.bot_id = 1;
    config.telegram.allowed_chats = vec![2];
    config.telegram.allowed_users = vec![3];
    Ok((root, store, config))
}
fn update(id: i64, text: &str) -> Update {
    serde_json::from_value(json!({"update_id":id,"message":{"message_id":id,"chat":{"id":2},"from":{"id":3},"text":text}})).unwrap()
}
fn lease(store: &Store, config: &Config, id: i64, text: &str) -> Result<Lease> {
    store.ingest(config, &update(id, text), &json!({"fixture":"runtime"}))?;
    Ok(store.claim_phase(Phase::Fetch)?.unwrap())
}
fn scope() -> Scope {
    Scope {
        bot_id: 1,
        chat_id: 2,
        user_id: 3,
    }
}
#[test]
fn retry_metric_counts_repeated_phase_attempts_and_excludes_phase_progress() -> Result<()> {
    let (_root, store, config) = fixture()?;
    let first = lease(&store, &config, 1, "Synthetic retry metrics")?;
    assert_eq!(crate::operator::metrics(&store)?["job_retries"], 0);
    store.fail_phase(&first, Failure::Network)?;
    store
        .connection()?
        .execute("UPDATE jobs SET available_at=0", [])?;
    let retry = store.claim_phase(Phase::Fetch)?.unwrap();
    assert_eq!(crate::operator::metrics(&store)?["job_retries"], 1);
    store.finish_phase(&retry, &json!({}), Some(Phase::Metadata), false)?;
    store.claim_phase(Phase::Metadata)?.unwrap();
    assert_eq!(crate::operator::metrics(&store)?["job_retries"], 1);
    Ok(())
}

#[test]
fn queue_metrics_preserve_partial_processing_state() -> Result<()> {
    let (_root, store, config) = fixture()?;
    let job = lease(&store, &config, 1, "Synthetic partial capture")?;
    store.finish_phase(&job, &json!({}), None, true)?;
    assert_eq!(
        store.job_for(scope(), &job.job.id)?.state,
        JobState::Partial
    );
    let metrics = crate::operator::metrics(&store)?;
    assert_eq!(metrics["queues"]["jobs"]["partial"], 1);
    assert!(metrics["queues"]["jobs"].get("complete").is_none());
    Ok(())
}

#[tokio::test]
async fn product_web_text_attachment_and_empty_text_pdf_keep_usable_originals() -> Result<()> {
    let (root, store, mut config) = fixture()?;
    let producer = root.path().join("tts");
    fs::write(
        &producer,
        "#!/usr/bin/python3\nimport sys, shutil\nfrom pathlib import Path\na=sys.argv[1:]\nassert Path(a[1]).read_text().strip()\nshutil.copyfile(Path(__file__).with_suffix('.mp3'),a[a.index('--output')+1])\n",
    )?;
    fs::set_permissions(&producer, fs::Permissions::from_mode(0o700))?;
    assert!(
        std::process::Command::new("/usr/bin/ffmpeg")
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
            .arg(producer.with_extension("mp3"))
            .status()?
            .success()
    );
    config.synthesis.enabled = true;
    config.tools.md2audio = producer;
    let docs = Documents {
        store: store.clone(),
        config: config.clone(),
        http: PublicHttp::default(),
    };
    let cancel = CancellationToken::new();
    let html = format!(
        "<html><head><title>Synthetic article</title></head><body><article><h1>Evidence</h1><p>{}</p></article></body></html>",
        "Original content with separate raw bytes and derived Markdown. ".repeat(15)
    );
    let extraction = docs
        .response(
            crate::network::Response {
                submitted: "https://example.org/article?id=1".parse()?,
                resolved: "https://example.org/article?id=1".parse()?,
                redirects: vec![],
                status: 200,
                content_type: "text/html".into(),
                charset: None,
                bytes: html.into_bytes(),
            },
            &cancel,
        )
        .await?;
    let web = lease(&store, &config, 1, "https://example.org/article?id=1")?;
    store.accept_extraction(&web, &extraction)?;
    let attachment = docs
        .attachment(
            "text-fixture",
            "same.md",
            b"# Synthetic text\n\nKeep **Markdown** unchanged.",
            &cancel,
        )
        .await?;
    let text = lease(&store, &config, 2, "Synthetic document submission")?;
    store.accept_extraction(&text, &attachment)?;
    let pdf = docs
        .attachment(
            "empty-text-pdf",
            "same.pdf",
            include_bytes!("../tests/fixtures/blank.pdf"),
            &cancel,
        )
        .await?;
    assert!(pdf.original.validated && pdf.text_digest.is_none());
    let pdf_job = lease(&store, &config, 3, "Synthetic PDF submission")?;
    store.accept_extraction(&pdf_job, &pdf)?;
    assert_eq!(
        store.job_for(scope(), &pdf_job.job.id)?.phase,
        Phase::Finalize
    );
    for phase in [
        Phase::Metadata,
        Phase::Translate,
        Phase::Download,
        Phase::Synthesize,
        Phase::Finalize,
        Phase::Archive,
    ] {
        while step(&store, phase, &std::env::current_exe()?, &cancel).await? {}
    }
    assert_eq!(
        store.job_for(scope(), &web.job.id)?.state,
        JobState::Complete
    );
    assert_eq!(
        store.job_for(scope(), &text.job.id)?.state,
        JobState::Complete
    );
    assert_eq!(
        store.job_for(scope(), &pdf_job.job.id)?.state,
        JobState::Partial
    );
    let payloads: Vec<String> = store
        .connection()?
        .prepare("SELECT payload FROM outbox WHERE job_id=?1")?
        .query_map([&pdf_job.job.id], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    assert!(
        payloads
            .iter()
            .any(|p| p.contains(&pdf.original.digest) && p.contains("application/pdf"))
    );
    assert!(
        !store
            .phase_outputs(&pdf_job.job.id)?
            .contains_key("translate")
    );
    assert_eq!(
        store.read_blob(&pdf.original.digest, 10000)?,
        include_bytes!("../tests/fixtures/blank.pdf")
    );
    assert!(
        !store
            .phase_outputs(&pdf_job.job.id)?
            .contains_key("synthesize")
    );
    for job in [&web.job.id, &text.job.id] {
        let outputs = store.phase_outputs(job)?;
        assert!(outputs.contains_key("synthesize"));
        let ready: i64 = store.connection()?.query_row(
            "SELECT count(*) FROM outbox WHERE job_id=?1 AND kind='audio'",
            [job],
            |r| r.get(0),
        )?;
        assert_eq!(ready, 1);
    }
    store.integrity()?;
    Ok(())
}
#[tokio::test]
async fn lost_fetch_owner_cannot_capture_select_view_or_finish() -> Result<()> {
    let (_root, store, config) = fixture()?;
    let first = lease(&store, &config, 1, "One synthetic body")?;
    let docs = Documents {
        store: store.clone(),
        config: config.clone(),
        http: PublicHttp::default(),
    };
    let extraction = docs
        .attachment(
            "fixture",
            "same.md",
            b"Source body",
            &CancellationToken::new(),
        )
        .await?;
    store
        .connection()?
        .execute("UPDATE jobs SET lease_until=0 WHERE id=?1", [&first.job.id])?;
    let next = store.claim_phase(Phase::Fetch)?.unwrap();
    assert!(store.accept_extraction(&first, &extraction).is_err());
    assert_eq!(store.source_counts()?.0, 0);
    assert!(store.phase_outputs(&first.job.id)?.is_empty());
    store.accept_extraction(&next, &extraction)?;
    assert_eq!(store.source_counts()?.0, 1);
    let outputs = store.phase_outputs(&next.job.id)?;
    store.view(outputs["fetch"]["view_id"].as_str().unwrap())?;
    assert_eq!(store.job_for(scope(), &next.job.id)?.phase, Phase::Metadata);
    Ok(())
}
#[tokio::test]
async fn cancelled_graph_owner_cannot_publish_any_capture() -> Result<()> {
    let (_root, store, config) = fixture()?;
    let first = lease(&store, &config, 1, "https://x.com/a/status/100")?;
    let identity = Identity::x("100")?;
    let traversal = Traversal {
        root: identity.clone(),
        captures: vec![Capture {
            identity: identity.clone(),
            submitted_url: None,
            content: Some(Content {
                body: "Full A".into(),
                metadata: Default::default(),
            }),
            status: FetchStatus::Complete,
            extractor: "synthetic".into(),
            evidence: json!({}),
            raw_digest: None,
            relations: vec![],
        }],
        omissions: vec![],
        visited: vec![identity.id()],
        bytes: 6,
        max_nodes: 10,
        max_depth: 3,
        reply_ancestors: 2,
    };
    store.cancel_job(scope(), &first.job.id)?;
    assert!(store.accept_traversal(&first, &traversal).is_err());
    assert_eq!(store.source_counts()?.0, 0);
    Ok(())
}
#[tokio::test]
async fn control_job_runs_while_source_fetch_is_leased_and_cancels_it() -> Result<()> {
    let (_root, store, config) = fixture()?;
    let source = lease(&store, &config, 1, "https://example.org/")?;
    store.ingest(
        &config,
        &update(2, &format!("/cancel {}", source.job.id)),
        &json!({"fixture":"runtime"}),
    )?;
    assert!(control_step(&store, &std::env::current_exe()?, &CancellationToken::new()).await?);
    assert_eq!(
        store.job_for(scope(), &source.job.id)?.state,
        JobState::Cancelled
    );
    assert!(store.heartbeat(&source).is_err());
    Ok(())
}
#[tokio::test]
async fn offline_replay_is_repeatable_has_independent_deliveries_and_recovery_hold() -> Result<()> {
    let (root, store, mut config) = fixture()?;
    config.archive.directory = Some(root.path().join("archive"));
    let updates = [
        update(1, "Shared original Markdown **keeps content**."),
        update(2, "Shared original Markdown **keeps content**."),
    ];
    let cancel = CancellationToken::new();
    let first = replay(&config, &updates, &cancel).await?;
    assert_eq!(first["queues"]["jobs"]["complete"], 2);
    assert_eq!(first["queues"]["outbox"]["pending"], 2);
    assert_eq!(store.source_counts()?.1, 1);
    let second = replay(&config, &updates, &cancel).await?;
    assert_eq!(second["queues"], first["queues"]);
    store.integrity()?;
    let backup = root.path().join("backup");
    store.backup(&backup)?;
    let restored = root.path().join("restored");
    Store::restore_backup(&backup, &restored)?;
    config.state_dir = restored;
    assert!(
        replay(&config, &[], &cancel)
            .await
            .unwrap_err()
            .to_string()
            .contains("hold")
    );
    Ok(())
}
#[tokio::test]
async fn finalized_markdown_uses_a_bounded_sanitized_semantic_title() -> Result<()> {
    let (_root, store, config) = fixture()?;
    let cancel = CancellationToken::new();
    lease(&store, &config, 1, "Synthetic original")?;
    store
        .connection()?
        .execute("UPDATE jobs SET lease_until=0", [])?;
    step(&store, Phase::Fetch, &std::env::current_exe()?, &cancel).await?;
    let metadata = store.claim_phase(Phase::Metadata)?.unwrap();
    let title = format!("../中文标题:{}\n", "很长的标题".repeat(30));
    let value = json!({"schema_version":1,"workflow":"metadata-v1","config_id":metadata.job.config_id,"revisions":[],"blocks":[],"result":{"schema_version":1,"title":title,"summary":"A synthetic summary","language":"zh","tags":[],"source_blocks":[]},"semantic_fidelity_verified":false});
    let blob = store.publish_bytes(&serde_json::to_vec(&value)?)?;
    store.finish_phase(
        &metadata,
        &json!({"metadata_digest":blob.digest}),
        Some(Phase::Finalize),
        false,
    )?;
    step(&store, Phase::Finalize, &std::env::current_exe()?, &cancel).await?;
    let payload: String = store
        .connection()?
        .query_row("SELECT payload FROM outbox", [], |r| r.get(0))?;
    let payload: Value = serde_json::from_str(&payload)?;
    let name = payload["name"].as_str().unwrap();
    assert!(name.starts_with("中文标题"), "{name}");
    assert!(name.ends_with(".md") && name.len() <= 200, "{name}");
    assert!(!name.chars().any(|c| c.is_control() || "/\\:".contains(c)));
    Ok(())
}
#[test]
fn missing_optional_tools_disable_only_the_requested_capability() -> Result<()> {
    let (_root, _store, mut config) = fixture()?;
    config.synthesis.enabled = true;
    config.tools.md2audio = "/absent/test-md2audio".into();
    config.model.enabled = false;
    disable_missing_optional(&mut config);
    assert!(!config.synthesis.enabled);
    assert!(!config.model.enabled);
    config.validate()?;
    Ok(())
}
#[tokio::test]
async fn doctor_is_offline_and_redacts_tool_output_and_config_values() -> Result<()> {
    let (root, store, mut config) = fixture()?;
    let secret = root.path().join("key");
    fs::write(&secret, "SYNTHETIC_SECRET_MUST_NOT_ESCAPE")?;
    fs::set_permissions(&secret, fs::Permissions::from_mode(0o600))?;
    config.model.key = Some(crate::config::SecretRef::File(secret));
    let tool = root.path().join("tool");
    fs::write(
        &tool,
        "#!/bin/sh\nprintf 'fixture SYNTHETIC_SECRET_MUST_NOT_ESCAPE\\n'\n",
    )?;
    fs::set_permissions(&tool, fs::Permissions::from_mode(0o700))?;
    config.tools.md2audio = tool;
    let report = crate::operator::doctor(&config, &store, &CancellationToken::new()).await?;
    let text = report.to_string();
    assert!(!text.contains("SYNTHETIC_SECRET_MUST_NOT_ESCAPE"));
    assert_eq!(report["network_probes_performed"], false);
    assert_eq!(report["telegram_messages_sent"], 0);
    assert_eq!(report["database_schema"], 10);
    assert!(text.contains("[REDACTED]"));
    Ok(())
}

#[tokio::test]
async fn native_media_alias_roundtrips_through_independent_git_restore() -> Result<()> {
    let (root, store, config) = fixture()?;
    let lease = lease(&store, &config, 1, "https://youtu.be/synthetic-id")?;
    let native = crate::media::MediaMetadata {
        id: "synthetic-id".into(),
        extractor: "Youtube".into(),
        title: Some("Synthetic media title".into()),
        advisory_duration_seconds: Some(25.0),
    };
    let body = "Provider metadata only; not a transcript.";
    let blob = store.publish_bytes(body.as_bytes())?;
    let extraction = Extraction {
        capture: Capture {
            identity: Identity {
                provider: "media:youtube".into(),
                external_id: native.id.clone(),
            },
            submitted_url: None,
            content: Some(Content {
                body: body.into(),
                metadata: Default::default(),
            }),
            status: FetchStatus::Complete,
            extractor: "yt-dlp-probe-v1".into(),
            evidence: json!({"native":native,"submitted_url":"https://youtu.be/synthetic-id"}),
            raw_digest: None,
            relations: vec![],
        },
        original: crate::documents::Original {
            digest: blob.digest.clone(),
            mime: "text/plain".into(),
            display_name: "metadata.txt".into(),
            validated: true,
        },
        text_digest: Some(blob.digest),
    };
    store.accept_extraction(&lease, &extraction)?;
    let mut exported = config.clone();
    exported.archive.directory = Some(root.path().join("archive"));
    let cancel = CancellationToken::new();
    store.project(&exported, 100, &cancel).await?;
    let target = root.path().join("rebuilt");
    Store::restore_git(
        exported.archive.directory.as_ref().unwrap(),
        &target,
        &config,
        &cancel,
    )
    .await?;
    let rebuilt = Store::open(&target)?;
    assert_eq!(rebuilt.source_counts()?, store.source_counts()?);
    let alias: (String, String) =
        rebuilt
            .connection()?
            .query_row("SELECT url,method FROM source_aliases", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })?;
    assert_eq!(
        alias,
        (
            "https://youtu.be/synthetic-id".into(),
            "yt_dlp_native_v1".into()
        )
    );
    Ok(())
}
