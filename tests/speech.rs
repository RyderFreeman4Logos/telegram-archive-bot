use anyhow::Result;
use serde_json::json;
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};
use telegram_archive_bot::{
    config::Config,
    jobs::{Failure, Lease, Phase},
    sources::{Capture, Content, ContentMetadata, FetchStatus, Identity},
    speech::{Md2Audio, SpeechCandidate, SpokenText},
    storage::Store,
    telegram::Update,
};
use tokio_util::sync::CancellationToken;
const SCRIPT: &str = r#"#!/usr/bin/python3
import os,sys,time,tomllib
from pathlib import Path
a=sys.argv[1:]
assert a[0]=='synth' and '--no-strip' in a
assert 'bot_token' not in os.environ and 'LOCAL_ROUTER_API_KEY' not in os.environ
text=Path(a[1]).read_text()
out=Path(a[a.index('--output')+1])
if '--multi-speaker' in a:
    assert '--no-cache' in a and '--seed' not in a
    mapping=tomllib.loads(Path(a[a.index('--speaker-config')+1]).read_text())['speakers']
    assert len(mapping)==1
if '--local-tts' in a:
    assert '--threads' in a and '--volume' in a and '--local-model' in a
    assert a[a.index('--voice')+1]=='0'
if 'SILENT' in text:
    Path('started').write_text('yes')
    time.sleep(60)
out.write_bytes(b'RAW_FIXTURE_ONLY:'+text.encode())
if '--srt' in a:
    out.with_suffix('.srt').write_text('1\n00:00:00,100 --> 00:00:01,000\nSynthetic\n')
if 'FAIL' in text: raise SystemExit(3)
"#;
fn fixture(
    text: &str,
    multi: bool,
    local: bool,
    seconds: u64,
) -> Result<(tempfile::TempDir, Store, Lease, String, String)> {
    let temp = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"))?;
    let script = temp.path().join("tts");
    fs::write(&script, SCRIPT)?;
    fs::set_permissions(&script, fs::Permissions::from_mode(0o700))?;
    let store = Store::open(temp.path())?;
    let mut c = Config::default();
    c.telegram.bot_id = 1;
    c.telegram.allowed_chats = vec![2];
    c.telegram.allowed_users = vec![3];
    c.synthesis.enabled = true;
    c.synthesis.subtitles = true;
    c.synthesis.local = local;
    if local {
        c.synthesis.voice = "0".into();
    }
    if multi {
        c.synthesis
            .speakers
            .insert("Alice".into(), "en-US-AriaNeural".into());
        c.synthesis
            .speakers
            .insert("张三".into(), "zh-CN-YunxiNeural".into());
    }
    c.tools.md2audio = script;
    c.limits.synthesis_seconds = seconds;
    let update: Update = serde_json::from_value(
        json!({"update_id":1,"message":{"message_id":1,"chat":{"id":2},"from":{"id":3},"text":"same name.md"}}),
    )?;
    store.ingest(&c, &update, &json!({"md2audio":"fixture-v1"}))?;
    let lease = store.claim_phase(Phase::Fetch)?.unwrap();
    let revision = store
        .capture(&Capture {
            identity: Identity::from_url("https://example.org/tts")?,
            submitted_url: None,
            content: Some(Content {
                body: text.into(),
                metadata: ContentMetadata::default(),
            }),
            status: FetchStatus::Complete,
            extractor: "fixture".into(),
            evidence: json!({}),
            raw_digest: None,
            relations: vec![],
        })?
        .unwrap();
    let input = store.publish_bytes(text.as_bytes())?.digest;
    Ok((temp, store, lease, revision, input))
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
async fn exact_full_text_and_distinct_translations_settings_identity() -> Result<()> {
    let (_t, store, lease, r, input) = fixture(
        "# 原文\r\n```rust\nfn main(){}\n```\n|a|b|\nhttps://example.org\n# References\nretain",
        false,
        false,
        10,
    )?;
    let adapter = Md2Audio {
        store: store.clone(),
    };
    let out = adapter
        .synthesize(&lease, &r, &input, "original", &CancellationToken::new())
        .await?;
    let raw: SpeechCandidate = serde_json::from_slice(&store.read_blob(&out, 1_000_000)?)?;
    let spoken: SpokenText =
        serde_json::from_slice(&store.read_blob(&raw.spoken_digest, 1_000_000)?)?;
    assert!(spoken.turns[0].text.contains("```rust"));
    assert!(spoken.turns[0].text.ends_with("retain"));
    assert!(!spoken.turns[0].text.contains('\r'));
    assert!(raw.pending_finalization && !raw.segments[0].timing_validated);
    assert!(raw.segments[0].subtitle_digest.is_some());
    assert_eq!(count(&store, "artifacts")?, 0);
    let translation = store.publish_bytes(b"Distinct translated text")?.digest;
    let second = adapter
        .synthesize(
            &lease,
            &r,
            &translation,
            "translation",
            &CancellationToken::new(),
        )
        .await?;
    assert_ne!(out, second);
    let mut changed = lease.clone();
    changed.config = {
        let mut c = lease.config.clone();
        c.synthesis.speed = 1.2;
        c
    };
    assert!(
        adapter
            .synthesize(&changed, &r, &input, "original", &CancellationToken::new())
            .await
            .is_err()
    );
    Ok(())
}
#[tokio::test]
async fn ordered_dialogue_toml_mapping_and_segment_local_subtitles() -> Result<()> {
    let (_t, store, lease, r, input) = fixture(
        "**Alice:** First\nline.\n**张三：** 第二段。",
        true,
        false,
        10,
    )?;
    let out = Md2Audio {
        store: store.clone(),
    }
    .synthesize(&lease, &r, &input, "original", &CancellationToken::new())
    .await?;
    let raw: SpeechCandidate = serde_json::from_slice(&store.read_blob(&out, 1_000_000)?)?;
    let spoken: SpokenText =
        serde_json::from_slice(&store.read_blob(&raw.spoken_digest, 1_000_000)?)?;
    assert_eq!(raw.segments.len(), 2);
    assert_eq!(raw.segments[1].order, 1);
    assert_eq!(spoken.turns[0].text, "First\nline.");
    assert_eq!(spoken.turns[1].voice, "zh-CN-YunxiNeural");
    assert_ne!(raw.segments[0].blob_digest, raw.segments[1].blob_digest);
    for segment in raw.segments {
        assert!(!segment.timing_validated);
        assert!(segment.subtitle_digest.is_some());
    }
    Ok(())
}
#[tokio::test]
async fn explicit_local_mode_and_invalid_combinations_before_tool() -> Result<()> {
    let (_t, store, lease, r, input) = fixture("Local full content", false, true, 10)?;
    Md2Audio {
        store: store.clone(),
    }
    .synthesize(&lease, &r, &input, "original", &CancellationToken::new())
    .await?;
    let mut c = lease.config.clone();
    c.synthesis.pitch = 1;
    assert!(c.validate().is_err());
    c.synthesis.pitch = 0;
    c.synthesis.speakers.insert("Alice".into(), "voice".into());
    assert!(c.validate().is_err());
    Ok(())
}
#[tokio::test]
async fn failures_cancellation_timeout_and_bad_dialogue_publish_no_candidate() -> Result<()> {
    for (text, seconds) in [("FAIL", 10), ("SILENT", 1)] {
        let (_t, store, lease, r, input) = fixture(text, false, false, seconds)?;
        let error = Md2Audio {
            store: store.clone(),
        }
        .synthesize(&lease, &r, &input, "original", &CancellationToken::new())
        .await
        .unwrap_err();
        assert_eq!(
            error.downcast_ref::<Failure>(),
            Some(&if text == "FAIL" {
                Failure::ToolExit
            } else {
                Failure::Timeout
            })
        );
        assert_eq!(count(&store, "artifacts")?, 0);
    }
    let (_t, store, lease, r, input) = fixture("SILENT", false, false, 10)?;
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let root = store.root().join("work");
    let trigger = tokio::spawn(async move {
        loop {
            if fs::read_dir(&root)
                .unwrap()
                .filter_map(Result::ok)
                .any(|e| e.path().join("started").exists())
            {
                token.cancel();
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    });
    let error = Md2Audio {
        store: store.clone(),
    }
    .synthesize(&lease, &r, &input, "original", &cancel)
    .await
    .unwrap_err();
    trigger.await?;
    assert_eq!(error.downcast_ref::<Failure>(), Some(&Failure::Cancelled));
    let (_t, store, lease, r, input) =
        fixture("Nonblank preamble\n**Alice:** hi", true, false, 10)?;
    assert!(
        Md2Audio {
            store: store.clone()
        }
        .synthesize(&lease, &r, &input, "original", &CancellationToken::new())
        .await
        .is_err()
    );
    Ok(())
}
#[tokio::test]
async fn simultaneous_identical_display_names_have_isolated_workspaces() -> Result<()> {
    let (_a, s1, l1, r1, i1) = fixture("First", false, false, 10)?;
    let (_b, s2, l2, r2, i2) = fixture("Second", false, false, 10)?;
    let a = Md2Audio { store: s1.clone() };
    let b = Md2Audio { store: s2.clone() };
    let c = CancellationToken::new();
    let (x, y) = tokio::join!(
        a.synthesize(&l1, &r1, &i1, "original", &c),
        b.synthesize(&l2, &r2, &i2, "original", &c)
    );
    assert_ne!(x?, y?);
    Ok(())
}

#[tokio::test]
async fn legitimate_snapshot_changes_cannot_reuse_incompatible_spoken_plan() -> Result<()> {
    let (_t, store, lease, r, input) = fixture("Full content", false, false, 10)?;
    let adapter = Md2Audio {
        store: store.clone(),
    };
    let original = adapter
        .synthesize(&lease, &r, &input, "original", &CancellationToken::new())
        .await?;
    for index in 0..4 {
        let mut config = lease.config.clone();
        match index {
            0 => config.synthesis.speed = 1.2,
            1 => config.synthesis.voice = "en-US-AriaNeural".into(),
            2 => config.synthesis.language = "en".into(),
            _ => config.synthesis.seed = 123,
        }
        let update: Update = serde_json::from_value(
            json!({"update_id":index+2,"message":{"message_id":index+2,"chat":{"id":2},"from":{"id":3},"text":"same name.md"}}),
        )?;
        store.ingest(&config, &update, &lease.tools)?;
        let changed = store.claim_phase(Phase::Fetch)?.unwrap();
        let next = adapter
            .synthesize(&changed, &r, &input, "original", &CancellationToken::new())
            .await?;
        assert_ne!(original, next);
    }
    Ok(())
}
