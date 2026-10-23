use anyhow::Result;
use serde_json::json;
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf, time::Duration};
use telegram_archive_bot::{
    config::Config,
    jobs::{Failure, Lease, Phase},
    media::{MediaCandidate, MediaDownloader},
    sources::{Capture, Content, ContentMetadata, FetchStatus, Identity},
    storage::Store,
    telegram::Update,
};
use tokio_util::sync::CancellationToken;

const FIXTURE: &str = r#"#!/usr/bin/python3
import json, os, socket, subprocess, sys, time, urllib.request
from pathlib import Path
a=sys.argv[1:]
assert os.readlink('/proc/self/ns/net') != HOST_NAMESPACE
for flag in ['--ignore-config','--no-plugin-dirs','--no-js-runtimes','--no-remote-components','--no-cookies','--no-cache-dir']:
    assert flag in a, flag
for flag in ['--retries','--fragment-retries','--extractor-retries','--file-access-retries']:
    assert a[a.index(flag)+1]=='0'
assert a[-2]=='--'
mode=a[-1].split('/')[-1]
def entry(i):
    return {'id':str(i),'title':'Synthetic Unicode λ','extractor_key':'fixture','duration':3.5,'webpage_url':'https://example.org/item'+str(i), 'url':'https://example.org/signed?PRIVATE_SYNTHETIC_TOKEN=secret'}
if mode == 'namespace':
    # The host listener uses this same port; it must be absent in this network namespace.
    try:
        c=socket.create_connection(('127.0.0.1', HOST_PORT),timeout=0.3)
    except OSError: pass
    else: c.close(); raise AssertionError('direct host-network access')
    proxy=a[a.index('--proxy')+1]
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({'https':proxy}))
    try: opener.open('https://127.0.0.1:'+str(HOST_PORT),timeout=2)
    except OSError: pass
    else: raise AssertionError('proxy reached host-private listener')
if '--dump-single-json' in a:
    assert '--simulate' in a and '--no-simulate' not in a
    assert not Path('candidate.ogg').exists()
    if mode in ['playlist','too_many','nested']:
        if '--yes-playlist' in a:
            assert a[a.index('--playlist-end')+1]=='2'
        else: assert '--no-playlist' in a
        items=[entry(1),entry(2)]
        if mode=='too_many': items.append(entry(3))
        if mode=='nested': items[0]['entries']=[entry(4)]
        print(json.dumps({'_type':'playlist','entries':items}))
    else: print(json.dumps(entry(0)))
    raise SystemExit(0)
assert '--no-simulate' in a and '--no-playlist' in a
assert a[a.index('--print')+1]=='after_move:%(filepath)j'
if mode == 'silent':
    descendant=subprocess.Popen(['/usr/bin/python3','-c','import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(60)'])
    Path('download-started').write_text(str(descendant.pid))
    time.sleep(60)
name='candidate changed "λ".ogg'
Path(name).write_bytes(b'SYNTHETIC_CANDIDATE_NOT_VALIDATED_AUDIO')
if mode == 'outside': name='/etc/passwd'
print(json.dumps(name))
print('arbitrary human diagnostic wording changed completely',file=sys.stderr)
if mode == 'post_failure': raise SystemExit(1)
"#;

fn fixture(
    mode: &str,
    host_port: u16,
    budget: u64,
) -> Result<(tempfile::TempDir, Store, Lease, String, MediaDownloader)> {
    let temp = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"))?;
    let store = Store::open(temp.path())?;
    let script = temp.path().join("fake-yt-dlp");
    let source = format!(
        "HOST_NAMESPACE={:?}\nHOST_PORT={host_port}\n",
        fs::read_link("/proc/self/ns/net")?.to_string_lossy()
    );
    fs::write(
        &script,
        FIXTURE.replacen("import json", &format!("{source}import json"), 1),
    )?;
    fs::set_permissions(&script, fs::Permissions::from_mode(0o700))?;
    let mut config = Config::default();
    config.telegram.bot_id = 1;
    config.telegram.allowed_chats = vec![2];
    config.telegram.allowed_users = vec![3];
    config.sources.media_enabled = true;
    config.tools.yt_dlp = script;
    if mode == "wrapper_missing" {
        config.tools.bubblewrap = temp.path().join("missing-bwrap");
    }
    if mode == "wrapper_failure" {
        config.tools.bubblewrap = "/usr/bin/false".into();
    }
    config.limits.download_seconds = budget;
    let update: Update = serde_json::from_value(
        json!({"update_id":1,"message":{"message_id":1,"chat":{"id":2},"from":{"id":3},"text":"fixture"}}),
    )?;
    store.ingest(&config, &update, &json!({"yt_dlp":"fixture-v1"}))?;
    let lease = store.claim_phase(Phase::Fetch)?.unwrap();
    let url = format!("https://example.org/{mode}");
    let capture = Capture {
        identity: Identity::from_url(&url)?,
        submitted_url: None,
        content: Some(Content {
            body: "Synthetic media reference".into(),
            metadata: ContentMetadata::default(),
        }),
        status: FetchStatus::Complete,
        extractor: "fixture".into(),
        evidence: json!({}),
        raw_digest: None,
        relations: vec![],
    };
    let revision = store.capture(&capture)?.unwrap();
    let downloader = MediaDownloader {
        store: store.clone(),
        relay_executable: PathBuf::from(env!("CARGO_BIN_EXE_telegram-archive-bot")),
    };
    Ok((temp, store, lease, revision, downloader))
}
fn count(store: &Store, table: &str) -> Result<i64> {
    Ok(
        rusqlite::Connection::open(store.root().join("archive.sqlite"))?.query_row(
            &format!("SELECT count(*) FROM {table}"),
            [],
            |r| r.get(0),
        )?,
    )
}

#[tokio::test]
async fn structured_simulation_changed_extension_and_pending_candidate() -> Result<()> {
    let (_temp, store, lease, revision, downloader) = fixture("normal", 1, 10)?;
    let token = CancellationToken::new();
    let metadata = downloader
        .probe(&lease, "https://example.org/normal", &token)
        .await?;
    assert_eq!(metadata.id, "0");
    assert_eq!(count(&store, "derivatives")?, 0);
    let outputs = downloader
        .download(
            &lease,
            &revision,
            "https://example.org/normal",
            None,
            &token,
        )
        .await?;
    assert_eq!(outputs.len(), 1);
    let candidate: MediaCandidate =
        serde_json::from_slice(&store.read_blob(&outputs[0], 1_048_576)?)?;
    assert!(candidate.pending_finalization);
    assert!(candidate.final_path_name.ends_with(".ogg"));
    assert!(candidate.final_path_name.contains('λ'));
    assert_eq!(candidate.source_revision, revision);
    assert_eq!(
        store.verify_blob(&candidate.blob_digest)?.size,
        candidate.byte_length
    );
    assert_eq!(count(&store, "artifacts")?, 0);
    assert!(!serde_json::to_string(&candidate)?.contains("PRIVATE_SYNTHETIC_TOKEN"));
    store.integrity()?;
    Ok(())
}

#[tokio::test]
async fn playlist_requires_opt_in_and_obeys_bound() -> Result<()> {
    for mode in ["playlist", "too_many", "nested"] {
        let (_temp, store, lease, revision, downloader) = fixture(mode, 1, 10)?;
        let url = format!("https://example.org/{mode}");
        let token = CancellationToken::new();
        assert!(
            downloader
                .download(&lease, &revision, &url, None, &token)
                .await
                .is_err()
        );
        assert_eq!(count(&store, "derivatives")?, 0);
        let outputs = downloader
            .download(&lease, &revision, &url, Some(2), &token)
            .await;
        if mode == "playlist" {
            let outputs = outputs?;
            assert_eq!(outputs.len(), 2);
            for (order, hash) in outputs.iter().enumerate() {
                let candidate: MediaCandidate =
                    serde_json::from_slice(&store.read_blob(hash, 1_048_576)?)?;
                assert_eq!(candidate.order, order);
            }
        } else {
            assert!(outputs.is_err());
            assert_eq!(count(&store, "derivatives")?, 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn failed_postprocessing_unsafe_path_and_missing_namespace_never_publish() -> Result<()> {
    for mode in [
        "post_failure",
        "outside",
        "wrapper_missing",
        "wrapper_failure",
    ] {
        let (_temp, store, lease, revision, downloader) = fixture(mode, 1, 10)?;
        assert!(
            downloader
                .download(
                    &lease,
                    &revision,
                    &format!("https://example.org/{mode}"),
                    None,
                    &CancellationToken::new()
                )
                .await
                .is_err()
        );
        assert_eq!(count(&store, "derivatives")?, 0);
        assert_eq!(count(&store, "artifacts")?, 0);
    }
    let (_temp, store, lease, revision, downloader) = fixture("normal", 1, 10)?;
    // A missing payload also fails without a host-network retry.
    fs::remove_file(&lease.config.tools.yt_dlp)?;
    assert!(
        downloader
            .download(
                &lease,
                &revision,
                "https://example.org/normal",
                None,
                &CancellationToken::new()
            )
            .await
            .is_err()
    );
    assert_eq!(count(&store, "derivatives")?, 0);
    Ok(())
}

#[tokio::test]
async fn namespace_and_proxy_cannot_reach_host_private_listener() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let positive = tokio::net::TcpStream::connect(listener.local_addr()?).await?;
    let (accepted, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept()).await??;
    drop(positive);
    drop(accepted);
    let (_temp, _store, lease, _revision, downloader) =
        fixture("namespace", listener.local_addr()?.port(), 10)?;
    downloader
        .probe(
            &lease,
            "https://example.org/namespace",
            &CancellationToken::new(),
        )
        .await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn cancellation_and_deadline_cover_wrapped_tool_and_no_ready_output() -> Result<()> {
    for cancel_run in [true, false] {
        let (_temp, store, lease, revision, downloader) =
            fixture("silent", 1, if cancel_run { 10 } else { 1 })?;
        let token = CancellationToken::new();
        let child = token.clone();
        let work = store.root().join("work");
        let trigger = tokio::spawn(async move {
            if cancel_run {
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        let started = fs::read_dir(&work)?
                            .filter_map(|e| e.ok())
                            .any(|e| e.path().join("download-started").is_file());
                        if started {
                            child.cancel();
                            return Ok::<_, std::io::Error>(());
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await??;
            }
            Ok::<_, anyhow::Error>(())
        });
        let error = downloader
            .download(
                &lease,
                &revision,
                "https://example.org/silent",
                None,
                &token,
            )
            .await
            .unwrap_err();
        trigger.await??;
        assert_eq!(
            error.downcast_ref::<Failure>(),
            Some(if cancel_run {
                &Failure::Cancelled
            } else {
                &Failure::Timeout
            })
        );
        assert_eq!(count(&store, "derivatives")?, 0);
        assert_eq!(count(&store, "artifacts")?, 0);
    }
    Ok(())
}
