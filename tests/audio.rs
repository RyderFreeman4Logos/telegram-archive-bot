use anyhow::{Result, ensure};
use serde_json::json;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};
use telegram_archive_bot::{
    audio::{AudioFinalizer, AudioInput, Profile, Repair, subtitle_cues},
    config::Config,
    digest,
    jobs::{Lease, Phase, Scope},
    storage::Store,
    telegram::Update,
};
use tokio_util::sync::CancellationToken;
fn fixture(limit: u64) -> Result<(tempfile::TempDir, Store, Lease)> {
    fixture_config(limit, |_, _| Ok(()))
}
fn fixture_config(
    limit: u64,
    setup: impl FnOnce(&Path, &mut Config) -> Result<()>,
) -> Result<(tempfile::TempDir, Store, Lease)> {
    let temp = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"))?;
    let store = Store::open(temp.path())?;
    let mut c = Config::default();
    c.telegram.bot_id = 1;
    c.telegram.allowed_chats = vec![2];
    c.telegram.allowed_users = vec![3];
    c.limits.media_bytes = limit;
    c.limits.media_seconds = 30;
    setup(temp.path(), &mut c)?;
    let u: Update = serde_json::from_value(
        json!({"update_id":1,"message":{"message_id":1,"chat":{"id":2},"from":{"id":3},"text":"audio"}}),
    )?;
    store.ingest(&c, &u, &json!({"ffmpeg":"real-local-fixture"}))?;
    let l = store.claim_phase(Phase::Fetch)?.unwrap();
    Ok((temp, store, l))
}
fn generate(root: &Path, name: &str, seconds: u32, codec: &str, extra: &[&str]) -> Result<Vec<u8>> {
    let path = root.join(name);
    let mut c = Command::new("/usr/bin/ffmpeg");
    c.args([
        "-v",
        "error",
        "-nostdin",
        "-f",
        "lavfi",
        "-i",
        &format!("sine=frequency=440:duration={seconds}"),
        "-c:a",
        codec,
        "-b:a",
        "96k",
    ]);
    c.args(extra);
    let r = c.arg(&path).output()?;
    ensure!(
        r.status.success(),
        "fixture generation failed: {}",
        String::from_utf8_lossy(&r.stderr)
    );
    Ok(fs::read(path)?)
}
fn input(store: &Store, bytes: &[u8], order: usize) -> Result<AudioInput> {
    Ok(AudioInput {
        blob_digest: store.publish_bytes(bytes)?.digest,
        parent_id: "synthetic-producer".into(),
        producer_tools: json!({"producer":"synthetic-ffmpeg-fixture"}),
        order,
        subtitle_digest: None,
    })
}
fn count(store: &Store) -> Result<i64> {
    Ok(
        rusqlite::Connection::open(store.root().join("archive.sqlite"))?.query_row(
            "SELECT count(*) FROM artifacts",
            [],
            |r| r.get(0),
        )?,
    )
}
#[tokio::test]
async fn valid_mp3_m4a_opus_pass_through_full_decode_exact_hashes() -> Result<()> {
    let (temp, store, lease) = fixture(49_000_000)?;
    let f = AudioFinalizer {
        store: store.clone(),
    };
    let cancel = CancellationToken::new();
    for (name, codec, profile) in [
        ("normal.mp3", "libmp3lame", Profile::AudioTrack),
        ("normal.m4a", "aac", Profile::AudioTrack),
        ("normal.ogg", "libopus", Profile::VoiceNote),
    ] {
        let bytes = generate(temp.path(), name, 3, codec, &[])?;
        let i = input(&store, &bytes, 0)?;
        let a = f
            .finalize(&lease, "fixture", &[i], profile, Repair::Reject, &cancel)
            .await?
            .remove(0);
        assert_eq!(a.operation, "preserve");
        assert_eq!(a.blob_digest, digest(&bytes));
        assert!((2900..=3100).contains(&a.duration_ms));
        assert!(a.validation.full_decode && a.validation.duration_consistent);
        assert!(!a.validation.original_completeness_verified);
        assert_eq!(
            store.artifact(&a.artifact_id)?.byte_length,
            bytes.len() as u64
        );
    }
    Ok(())
}
#[tokio::test]
async fn explicit_repair_makes_missing_index_seekable_and_profiles_are_distinct() -> Result<()> {
    let (temp, store, lease) = fixture(49_000_000)?;
    let bytes = generate(
        temp.path(),
        "no-xing.mp3",
        4,
        "libmp3lame",
        &["-write_xing", "0"],
    )?;
    let i = input(&store, &bytes, 0)?;
    let f = AudioFinalizer {
        store: store.clone(),
    };
    let cancel = CancellationToken::new();
    let audio = f
        .finalize(
            &lease,
            "fixture",
            std::slice::from_ref(&i),
            Profile::AudioTrack,
            Repair::Remux,
            &cancel,
        )
        .await?
        .remove(0);
    assert!(audio.validation.full_decode);
    let voice = f
        .finalize(
            &lease,
            "fixture",
            std::slice::from_ref(&i),
            Profile::VoiceNote,
            Repair::Transcode,
            &cancel,
        )
        .await?
        .remove(0);
    assert_eq!(voice.operation, "transcode");
    assert_eq!(voice.codec, "opus");
    assert_eq!(voice.mime, "audio/ogg");
    assert_ne!(audio.artifact_id, voice.artifact_id);
    assert!(
        f.finalize(
            &lease,
            "fixture",
            &[i],
            Profile::VoiceNote,
            Repair::Reject,
            &cancel
        )
        .await
        .is_err()
    );
    Ok(())
}
#[tokio::test]
async fn concatenation_demonstrates_remux_success_is_not_validation() -> Result<()> {
    let (temp, store, lease) = fixture(49_000_000)?;
    let first = generate(temp.path(), "first.mp3", 3, "libmp3lame", &["-b:a", "64k"])?;
    let second = generate(
        temp.path(),
        "second.mp3",
        17,
        "libmp3lame",
        &["-b:a", "160k"],
    )?;
    let mut bytes = first;
    bytes.extend(second);
    let cat = temp.path().join("concatenated.mp3");
    fs::write(&cat, &bytes)?;
    let remux = temp.path().join("remux.mp3");
    let r = Command::new("/usr/bin/ffmpeg")
        .args(["-v", "warning", "-nostdin", "-i"])
        .arg(&cat)
        .args(["-c:a", "copy", "-write_xing", "1"])
        .arg(&remux)
        .output()?;
    assert!(
        r.status.success(),
        "remux itself must succeed in this fixture"
    );
    let strict = Command::new("/usr/bin/ffmpeg")
        .args(["-v", "error", "-xerror", "-err_detect", "explode", "-i"])
        .arg(&remux)
        .args(["-f", "null", "-"])
        .output()?;
    assert!(
        !strict.status.success(),
        "remux output must still contain a decode error"
    );
    let f = AudioFinalizer {
        store: store.clone(),
    };
    let i = input(&store, &bytes, 0)?;
    let cancel = CancellationToken::new();
    assert!(
        f.finalize(
            &lease,
            "concat",
            std::slice::from_ref(&i),
            Profile::AudioTrack,
            Repair::Remux,
            &cancel
        )
        .await
        .is_err()
    );
    assert_eq!(count(&store)?, 0);
    let a = f
        .finalize(
            &lease,
            "concat",
            &[i],
            Profile::AudioTrack,
            Repair::Transcode,
            &cancel,
        )
        .await?
        .remove(0);
    assert_eq!(a.operation, "transcode");
    assert!(a.validation.source_had_errors);
    assert!((19_800..=20_500).contains(&a.duration_ms));
    assert_ne!(a.raw_digest, a.blob_digest);
    assert!(!a.validation.original_completeness_verified);
    let report = store.read_blob(&a.validation.source_inspection_digest, 1_000_000)?;
    assert!(
        serde_json::from_slice::<serde_json::Value>(&report)?["decode_report"]["stderr"]
            .as_str()
            .is_some_and(|s| !s.is_empty())
    );
    Ok(())
}
#[tokio::test]
async fn incomplete_oversize_low_quality_and_aborted_batches_publish_nothing() -> Result<()> {
    let (temp, store, lease) = fixture(49_000_000)?;
    let bytes = generate(temp.path(), "full.mp3", 10, "libmp3lame", &[])?;
    let half = &bytes[..bytes.len() / 2];
    let f = AudioFinalizer {
        store: store.clone(),
    };
    let cancel = CancellationToken::new();
    assert!(
        f.finalize(
            &lease,
            "truncated",
            &[input(&store, half, 0)?],
            Profile::AudioTrack,
            Repair::Reject,
            &cancel
        )
        .await
        .is_err()
    );
    assert_eq!(count(&store)?, 0);
    let low = generate(temp.path(), "low.mp3", 3, "libmp3lame", &["-b:a", "32k"])?;
    assert!(
        f.finalize(
            &lease,
            "low",
            &[input(&store, &low, 0)?],
            Profile::AudioTrack,
            Repair::Reject,
            &cancel
        )
        .await
        .is_err()
    );
    let valid = input(&store, &bytes, 0)?;
    let invalid = input(&store, b"NOT AUDIO", 1)?;
    assert!(
        f.finalize(
            &lease,
            "batch",
            &[valid.clone(), invalid],
            Profile::AudioTrack,
            Repair::Reject,
            &cancel
        )
        .await
        .is_err()
    );
    assert_eq!(count(&store)?, 0);
    cancel.cancel();
    assert!(
        f.finalize(
            &lease,
            "cancel",
            &[valid],
            Profile::AudioTrack,
            Repair::Reject,
            &cancel
        )
        .await
        .is_err()
    );
    assert_eq!(count(&store)?, 0);
    let (other, s2, l2) = fixture(1000)?;
    let _keep = other;
    assert!(
        AudioFinalizer { store: s2.clone() }
            .finalize(
                &l2,
                "limit",
                &[input(&s2, &bytes, 0)?],
                Profile::AudioTrack,
                Repair::Transcode,
                &CancellationToken::new()
            )
            .await
            .is_err()
    );
    assert_eq!(count(&s2)?, 0);
    Ok(())
}
#[tokio::test]
async fn ordered_subtitles_use_measured_samples_and_reject_estimated_overrun() -> Result<()> {
    let (temp, store, lease) = fixture(49_000_000)?;
    let a = generate(temp.path(), "one.mp3", 3, "libmp3lame", &[])?;
    let b = generate(temp.path(), "two.mp3", 4, "libmp3lame", &[])?;
    let subtitle = store
        .publish_bytes(b"1\n00:00:00,100 --> 00:00:01,000\nSynthetic\n")?
        .digest;
    let mut first = input(&store, &a, 0)?;
    first.subtitle_digest = Some(subtitle.clone());
    let mut second = input(&store, &b, 1)?;
    second.subtitle_digest = Some(subtitle);
    let f = AudioFinalizer {
        store: store.clone(),
    };
    let out = f
        .finalize(
            &lease,
            "dialogue",
            &[first, second],
            Profile::AudioTrack,
            Repair::Reject,
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(out[1].timeline_offset_ms, out[0].duration_ms);
    let text =
        String::from_utf8(store.read_blob(out[1].subtitle_digest.as_ref().unwrap(), 10000)?)?;
    let cues = subtitle_cues(&text, 10_000, 0)?;
    assert_eq!(cues[0].start_ms, out[0].duration_ms + 100);
    assert!(subtitle_cues("1\n00:00:00,000 --> 00:00:04,000\nOverrun", 3000, 0).is_err());
    assert!(subtitle_cues("1\n00:99:00,000 --> 00:99:01,000\nInvalid", 3000, 0).is_err());
    store.cancel_job(
        Scope {
            bot_id: 1,
            chat_id: 2,
            user_id: 3,
        },
        &lease.job.id,
    )?;
    assert!(
        f.finalize(
            &lease,
            "cancelled",
            &[],
            Profile::AudioTrack,
            Repair::Reject,
            &CancellationToken::new()
        )
        .await
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn absent_duration_requires_remux_and_reinspection() -> Result<()> {
    let (temp, store, lease) = fixture_config(49_000_000, |root, c| {
        let script = root.join("probe-missing-duration");
        fs::write(
            &script,
            r#"#!/usr/bin/python3
import json,subprocess,sys,os
if '-version' in sys.argv: os.execv('/usr/bin/ffprobe',['ffprobe']+sys.argv[1:])
r=subprocess.run(['/usr/bin/ffprobe']+sys.argv[1:],capture_output=True)
if r.returncode: sys.exit(r.returncode)
v=json.loads(r.stdout)
if '/blobs/' in sys.argv[-1]:
    v['format'].pop('duration',None)
    for s in v['streams']: s.pop('duration',None)
print(json.dumps(v))
"#,
        )?;
        fs::set_permissions(&script, fs::Permissions::from_mode(0o700))?;
        c.tools.ffprobe = script;
        Ok(())
    })?;
    let bytes = generate(temp.path(), "duration.mp3", 3, "libmp3lame", &[])?;
    let i = input(&store, &bytes, 0)?;
    let f = AudioFinalizer {
        store: store.clone(),
    };
    let cancel = CancellationToken::new();
    assert!(
        f.finalize(
            &lease,
            "missing-duration",
            std::slice::from_ref(&i),
            Profile::AudioTrack,
            Repair::Reject,
            &cancel
        )
        .await
        .is_err()
    );
    let a = f
        .finalize(
            &lease,
            "missing-duration",
            &[i],
            Profile::AudioTrack,
            Repair::Remux,
            &cancel,
        )
        .await?
        .remove(0);
    assert_eq!(a.operation, "remux");
    assert!(a.validation.duration_consistent);
    Ok(())
}
#[tokio::test]
async fn cancellation_after_decoder_start_and_deadline_publish_no_artifact() -> Result<()> {
    for cancel_run in [true, false] {
        let (temp, store, lease) = fixture_config(49_000_000, |root, c| {
            let script = root.join("blocked-decoder");
            fs::write(
                &script,
                r#"#!/usr/bin/python3
import os,sys,time
from pathlib import Path
if '-version' in sys.argv: os.execv('/usr/bin/ffmpeg',['ffmpeg']+sys.argv[1:])
Path('decoder-started').write_text('started')
time.sleep(60)
"#,
            )?;
            fs::set_permissions(&script, fs::Permissions::from_mode(0o700))?;
            c.tools.ffmpeg = script;
            c.limits.media_seconds = if cancel_run { 10 } else { 1 };
            Ok(())
        })?;
        let bytes = generate(temp.path(), "valid.mp3", 1, "libmp3lame", &[])?;
        let i = input(&store, &bytes, 0)?;
        let token = CancellationToken::new();
        let child = token.clone();
        let root = store.root().join("work");
        let trigger = tokio::spawn(async move {
            for _ in 0..500 {
                if fs::read_dir(&root)?
                    .filter_map(Result::ok)
                    .any(|e| e.path().join("decoder-started").exists())
                {
                    if cancel_run {
                        child.cancel();
                    }
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            anyhow::bail!("decoder did not start")
        });
        let error = AudioFinalizer {
            store: store.clone(),
        }
        .finalize(
            &lease,
            "cancel-during-decode",
            &[i],
            Profile::AudioTrack,
            Repair::Transcode,
            &token,
        )
        .await
        .unwrap_err();
        trigger.await??;
        assert_eq!(
            error.downcast_ref::<telegram_archive_bot::jobs::Failure>(),
            Some(&if cancel_run {
                telegram_archive_bot::jobs::Failure::Cancelled
            } else {
                telegram_archive_bot::jobs::Failure::Timeout
            })
        );
        assert_eq!(count(&store)?, 0);
    }
    Ok(())
}
