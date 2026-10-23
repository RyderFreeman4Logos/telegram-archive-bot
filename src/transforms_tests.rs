use super::*;
use crate::{
    config::Config,
    delivery::{Delivery, DeliveryWorker},
    jobs::{Phase, Scope},
    sources::*,
    telegram::{Telegram, Update},
};
use anyhow::ensure;
use serde_json::json;
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf, process::Command};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PRODUCER: &str = r#"#!/usr/bin/python3
import sys,time,shutil
from pathlib import Path
args=sys.argv[1:]
text=Path(args[1]).read_text()
calls=Path(__file__).with_suffix('.calls')
with calls.open('a') as f: f.write('called\n')
if 'WAIT_ONCE' in text and len(calls.read_text().splitlines())==1: time.sleep(60)
output=Path(args[args.index('--output')+1])
if 'BAD_AUDIO' in text: output.write_bytes(b'not audio despite successful producer')
else: shutil.copyfile(Path(__file__).with_suffix('.mp3'),output)
"#;
fn fixture(text: &str) -> Result<(tempfile::TempDir, Store, Config, NarrationRequest)> {
    let root = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"))?;
    let producer = root.path().join("tts");
    fs::write(&producer, PRODUCER)?;
    fs::set_permissions(&producer, fs::Permissions::from_mode(0o700))?;
    let raw = root.path().join("tts.mp3");
    let status = Command::new("/usr/bin/ffmpeg")
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
        .arg(&raw)
        .status()?;
    ensure!(status.success(), "fixture generation failed");
    let store = Store::open(root.path())?;
    let mut config = Config {
        state_dir: root.path().to_owned(),
        ..Config::default()
    };
    config.telegram.bot_id = 1;
    config.telegram.allowed_chats = vec![2];
    config.telegram.allowed_users = vec![3];
    config.synthesis.enabled = true;
    config.synthesis.deployment_revision = "synthetic-tts-v1".into();
    config.tools.md2audio = producer;
    config.limits.synthesis_seconds = 10;
    config.limits.media_seconds = 30;
    let source = Capture {
        identity: Identity::x("100")?,
        submitted_url: None,
        content: Some(Content {
            body: text.into(),
            metadata: ContentMetadata::default(),
        }),
        status: FetchStatus::Complete,
        extractor: "synthetic-counting-producer".into(),
        evidence: json!({}),
        raw_digest: None,
        relations: vec![],
    };
    let revision = store.capture(&source)?.unwrap();
    let input_digest = store.publish_bytes(text.as_bytes())?.digest;
    let request = NarrationRequest {
        revision,
        input_digest,
        input_kind: "original".into(),
        view_id: None,
        profile: crate::audio::Profile::AudioTrack,
        repair: crate::audio::Repair::Transcode,
    };
    Ok((root, store, config, request))
}
fn job(store: &Store, config: &Config, update: i64, force: bool) -> Result<Lease> {
    let u: Update = serde_json::from_value(
        json!({"update_id":update,"message":{"message_id":update,"chat":{"id":2},"from":{"id":3},"text":"independent audio request"}}),
    )?;
    store.ingest(
        config,
        &u,
        &json!({"producer":"counted-ffmpeg-sine","policy":"test-only"}),
    )?;
    if force {
        store
            .connection()?
            .execute("UPDATE jobs SET force_run=1 WHERE update_id=?1", [update])?;
    }
    Ok(store.claim_phase(Phase::Fetch)?.unwrap())
}
fn calls(config: &Config) -> usize {
    fs::read_to_string(config.tools.md2audio.with_extension("calls"))
        .unwrap_or_default()
        .lines()
        .count()
}

#[tokio::test]
async fn unpinned_tts_dependencies_do_not_reuse_an_unchanged_launcher() -> Result<()> {
    let (_root, store, mut config, request) = fixture("Original synthetic speech")?;
    config.synthesis.deployment_revision.clear();
    let cancel = CancellationToken::new();
    let first = job(&store, &config, 1, false)?;
    let old = store.narrate(&first, &request, &cancel).await?;
    assert_eq!(
        old.artifacts,
        store.narrate(&first, &request, &cancel).await?.artifacts
    );
    assert_eq!(
        calls(&config),
        1,
        "same-job recovery retains its completed output"
    );
    let launcher = fs::read(&config.tools.md2audio)?;
    assert!(
        Command::new("/usr/bin/ffmpeg")
            .args([
                "-v",
                "error",
                "-nostdin",
                "-y",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=880:duration=3",
                "-c:a",
                "libmp3lame",
                "-b:a",
                "96k"
            ])
            .arg(config.tools.md2audio.with_extension("mp3"))
            .status()?
            .success()
    );
    assert_eq!(launcher, fs::read(&config.tools.md2audio)?);
    let second = job(&store, &config, 2, false)?;
    let new = store.narrate(&second, &request, &cancel).await?;
    assert_eq!(
        calls(&config),
        2,
        "an unpinned Python package/backend can change behind the same launcher"
    );
    assert_ne!(
        store.artifact(&old.artifacts[0])?.blob_digest,
        store.artifact(&new.artifacts[0])?.blob_digest
    );
    Ok(())
}
#[tokio::test]
async fn source_local_audio_coalesces_with_independent_deliveries_and_upload_retry() -> Result<()> {
    let (_root, store, config, request) = fixture("Canonical A body shared by B and C")?;
    let a_id = Identity::x("100")?;
    for (id, kind) in [
        ("101", RelationKind::Quotes),
        ("102", RelationKind::RepliesTo),
    ] {
        let capture = Capture {
            identity: Identity::x(id)?,
            submitted_url: None,
            content: Some(Content {
                body: format!("Independent view {id}"),
                metadata: ContentMetadata::default(),
            }),
            status: FetchStatus::Complete,
            extractor: "synthetic-graph".into(),
            evidence: json!({}),
            raw_digest: None,
            relations: vec![Related {
                target: a_id.clone(),
                kind,
            }],
        };
        store.capture(&capture)?;
        let view = store.create_view(
            &capture.identity,
            ViewPolicy {
                max_nodes: 10,
                max_depth: 3,
            },
        )?;
        assert!(
            view.members
                .iter()
                .any(|m| m.revision_id.as_ref() == Some(&request.revision))
        );
    }
    assert_eq!(
        store.current_revision(&a_id)?.as_ref(),
        Some(&request.revision)
    );
    let first = job(&store, &config, 1, false)?;
    let second = job(&store, &config, 2, false)?;
    let cancel = CancellationToken::new();
    let (a, b) = tokio::join!(
        store.narrate(&first, &request, &cancel),
        store.narrate(&second, &request, &cancel)
    );
    let a = a?;
    let b = b?;
    assert_eq!(a.artifacts, b.artifacts);
    assert_eq!(calls(&config), 1);
    let delivery = Delivery::Audio {
        artifact_id: a.artifacts[0].clone(),
        title: "Synthetic".into(),
        performer: "Fixture".into(),
        caption: "A reference".into(),
    };
    let first_ids = store.enqueue_deliveries(&first, std::slice::from_ref(&delivery))?;
    let second_ids = store.enqueue_deliveries(&second, &[delivery])?;
    assert_ne!(first_ids, second_ids);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        for index in 0..2 {
            let (mut socket, _) = listener.accept().await?;
            let mut bytes = vec![];
            loop {
                let mut chunk = [0; 8192];
                let n = socket.read(&mut chunk).await?;
                ensure!(n > 0, "fixture request disconnected");
                bytes.extend_from_slice(&chunk[..n]);
                if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let header = String::from_utf8_lossy(&bytes[..end]);
                    let size = header
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .and_then(|v| v.parse::<usize>().ok())
                        })
                        .unwrap();
                    if bytes.len() >= end + 4 + size {
                        break;
                    }
                }
            }
            let body=if index==0{json!({"ok":false,"error_code":503,"description":"synthetic unavailable"})}else{json!({"ok":true,"result":{"message_id":55,"chat":{"id":2},"audio":{"duration":3,"mime_type":"audio/mpeg","file_id":"fixture-id","file_unique_id":"fixture-unique"}}})}.to_string();
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await?;
        }
        Ok::<_, anyhow::Error>(())
    });
    let worker = DeliveryWorker {
        store: store.clone(),
        telegram: Telegram::new("synthetic".into())?.fixture_base(base),
        bot_id: 1,
    };
    assert!(worker.send_next(&cancel).await?);
    store.connection()?.execute(
        "UPDATE outbox SET available_at=0 WHERE id=?1",
        [&first_ids[0]],
    )?;
    assert!(worker.send_next(&cancel).await?);
    server.await??;
    assert_eq!(calls(&config), 1);
    let scope = Scope {
        bot_id: 1,
        chat_id: 2,
        user_id: 3,
    };
    assert_eq!(store.outbox_for(scope, &first_ids[0])?.state, "confirmed");
    assert_eq!(store.outbox_for(scope, &second_ids[0])?.state, "pending");
    Ok(())
}
#[tokio::test]
async fn voice_language_speaker_profile_tool_and_force_changes_preserve_prior_audio() -> Result<()>
{
    let (_root, store, config, request) = fixture("**Alice:** Full original text.")?;
    let first = job(&store, &config, 1, false)?;
    let cancel = CancellationToken::new();
    let original = store.narrate(&first, &request, &cancel).await?;
    assert_eq!(calls(&config), 1);
    for (i, change) in ["voice", "language", "speaker", "profile", "deployment"]
        .iter()
        .enumerate()
    {
        let mut config = config.clone();
        let mut request = request.clone();
        match *change {
            "voice" => config.synthesis.voice = "en-US-AriaNeural".into(),
            "language" => config.synthesis.language = "en".into(),
            "deployment" => config.synthesis.deployment_revision = "synthetic-tts-v2".into(),
            "speaker" => {
                config
                    .synthesis
                    .speakers
                    .insert("Alice".into(), "en-US-AriaNeural".into());
            }
            _ => request.profile = crate::audio::Profile::VoiceNote,
        }
        let lease = job(&store, &config, 10 + i as i64, false)?;
        let output = store.narrate(&lease, &request, &cancel).await?;
        assert_ne!(original.artifacts, output.artifacts);
        assert_eq!(calls(&config), i + 2);
    }
    let forced = job(&store, &config, 20, true)?;
    let fresh = store.narrate(&forced, &request, &cancel).await?;
    assert_ne!(original.artifacts, fresh.artifacts);
    assert_eq!(calls(&config), 7);
    store.artifact(&original.artifacts[0])?;
    assert_eq!(
        fresh.artifacts,
        store.narrate(&forced, &request, &cancel).await?.artifacts
    );
    assert_eq!(calls(&config), 7);
    let later = job(&store, &config, 21, false)?;
    assert_eq!(
        fresh.artifacts,
        store.narrate(&later, &request, &cancel).await?.artifacts
    );
    assert_eq!(calls(&config), 7);
    fs::write(
        &config.tools.md2audio,
        format!("{PRODUCER}\n# changed executable version\n"),
    )?;
    let changed = job(&store, &config, 22, false)?;
    assert_ne!(
        fresh.artifacts,
        store.narrate(&changed, &request, &cancel).await?.artifacts
    );
    assert_eq!(calls(&config), 8);
    Ok(())
}
#[tokio::test]
async fn invalid_audio_or_corrupt_retained_bytes_cannot_be_cache_hits() -> Result<()> {
    let (_root, store, config, request) = fixture("BAD_AUDIO")?;
    let lease = job(&store, &config, 1, false)?;
    let cancel = CancellationToken::new();
    assert!(store.narrate(&lease, &request, &cancel).await.is_err());
    assert_eq!(calls(&config), 1);
    assert!(store.narrate(&lease, &request, &cancel).await.is_err());
    assert_eq!(calls(&config), 2);
    let (_other, store, config, request) = fixture("Valid retained audio")?;
    let lease = job(&store, &config, 1, false)?;
    let output = store.narrate(&lease, &request, &cancel).await?;
    let artifact = store.artifact(&output.artifacts[0])?;
    fs::write(store.blob_path(&artifact.blob_digest)?, b"corrupt")?;
    let later = job(&store, &config, 2, false)?;
    assert!(store.narrate(&later, &request, &cancel).await.is_err());
    assert_eq!(calls(&config), 1);
    Ok(())
}
#[tokio::test]
async fn cancelled_owner_does_not_select_audio_and_next_owner_can_produce() -> Result<()> {
    let (_root, store, mut config, request) = fixture("WAIT_ONCE")?;
    config.limits.lease_seconds = 3;
    let first = job(&store, &config, 1, false)?;
    let cancel = CancellationToken::new();
    let pending = store.narrate(&first, &request, &cancel);
    tokio::pin!(pending);
    let cancel_job = async {
        let start = tokio::time::Instant::now();
        while calls(&config) == 0 {
            ensure!(
                start.elapsed() < Duration::from_secs(5),
                "producer failed to start"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        store.cancel_job(
            Scope {
                bot_id: 1,
                chat_id: 2,
                user_id: 3,
            },
            &first.job.id,
        )?;
        Ok::<_, anyhow::Error>(())
    };
    let (result, cancelled) = tokio::join!(&mut pending, cancel_job);
    cancelled?;
    assert!(result.is_err());
    let second = job(&store, &config, 2, false)?;
    let output = store.narrate(&second, &request, &cancel).await?;
    store.artifact(&output.artifacts[0])?;
    assert_eq!(calls(&config), 2);
    Ok(())
}
