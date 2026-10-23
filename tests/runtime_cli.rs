use anyhow::{Result, ensure};
use serde_json::Value;
use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Command};
use telegram_archive_bot::{config::Config, storage::Store};
fn invoke(config: &Path, args: &[&str]) -> Result<std::process::Output> {
    Ok(Command::new(env!("CARGO_BIN_EXE_telegram-archive-bot"))
        .arg("--config")
        .arg(config)
        .args(args)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .output()?)
}
#[test]
fn documented_cli_replay_status_doctor_and_backup_restore() -> Result<()> {
    let root = tempfile::Builder::new()
        .prefix("archive-cli-")
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in("/var/tmp")?;
    let mut config = Config {
        state_dir: root.path().join("state"),
        ..Default::default()
    };
    config.telegram.bot_id = 1;
    config.telegram.allowed_chats = vec![2];
    config.telegram.allowed_users = vec![3];
    config.archive.directory = Some(root.path().join("archive"));
    let path = root.path().join("config.toml");
    fs::write(&path, toml::to_string(&config)?)?;
    let updates = root.path().join("updates.json");
    fs::write(&updates, include_bytes!("../examples/offline-updates.json"))?;
    let replay = invoke(&path, &["replay", updates.to_str().unwrap()])?;
    ensure!(
        replay.status.success(),
        "{}",
        String::from_utf8_lossy(&replay.stderr)
    );
    let metrics: Value = serde_json::from_slice(&replay.stdout)?;
    assert_eq!(metrics["queues"]["jobs"]["complete"], 2);
    assert_eq!(metrics["queues"]["outbox"]["pending"], 2);
    assert!(!String::from_utf8_lossy(&replay.stderr).contains("Keep this content"));
    let doctor = invoke(&path, &["doctor"])?;
    ensure!(doctor.status.success(), "doctor command failed");
    let report: Value = serde_json::from_slice(&doctor.stdout)?;
    assert_eq!(report["network_probes_performed"], false);
    assert_eq!(report["telegram_messages_sent"], 0);
    let status = invoke(&path, &["status"])?;
    assert!(status.status.success());
    let backup = root.path().join("backup");
    let result = invoke(&path, &["backup", backup.to_str().unwrap()])?;
    ensure!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let restored = root.path().join("restored");
    let result = invoke(
        &path,
        &[
            "restore-backup",
            backup.to_str().unwrap(),
            restored.to_str().unwrap(),
        ],
    )?;
    ensure!(result.status.success(), "restore failed");
    assert!(Store::open(&restored)?.recovery_paused()?);
    Ok(())
}

#[test]
fn product_cli_download_finalizes_only_valid_bytes_and_does_not_narrate_metadata() -> Result<()> {
    let root = tempfile::Builder::new()
        .prefix("archive-media-product-")
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in("/var/tmp")?;
    let tool = root.path().join("yt-dlp");
    fs::write(
        &tool,
        r#"#!/usr/bin/python3
import json,sys,shutil
from pathlib import Path
a=sys.argv[1:]
if '--dump-single-json' in a:
    assert '--simulate' in a and '--no-playlist' in a
    print(json.dumps({'id':a[-1].split('/')[-1],'extractor_key':'Fixture','title':'Synthetic source metadata','duration':3}))
else:
    assert '--no-simulate' in a and '--no-playlist' in a
    out=Path('postprocessed final.mp3')
    if a[-1].endswith('invalid'): out.write_bytes(b'not valid audio')
    else: shutil.copyfile(Path(__file__).with_suffix('.mp3'),out)
    print(json.dumps(str(out)))
"#,
    )?;
    fs::set_permissions(&tool, fs::Permissions::from_mode(0o700))?;
    let generated = Command::new("/usr/bin/ffmpeg")
        .args([
            "-v",
            "error",
            "-nostdin",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=660:duration=3",
            "-c:a",
            "libmp3lame",
            "-b:a",
            "96k",
        ])
        .arg(tool.with_extension("mp3"))
        .status()?;
    assert!(generated.success());
    let mut config = Config {
        state_dir: root.path().join("state"),
        ..Default::default()
    };
    config.telegram.bot_id = 1;
    config.telegram.allowed_chats = vec![2];
    config.telegram.allowed_users = vec![3];
    config.sources.media_enabled = true;
    config.tools.yt_dlp = tool;
    config.synthesis.enabled = true;
    config.tools.md2audio = "/usr/bin/false".into();
    config.limits.download_seconds = 60;
    config.limits.media_seconds = 60;
    let path = root.path().join("config.toml");
    fs::write(&path, toml::to_string(&config)?)?;
    let updates = root.path().join("updates.json");
    let inputs=(1..=2).map(|id|serde_json::json!({"update_id":id,"message":{"message_id":id,"chat":{"id":2},"from":{"id":3},"text":format!("https://youtu.be/{}",if id==1 {"valid"} else {"invalid"})}})).collect::<Vec<_>>();
    fs::write(&updates, serde_json::to_vec(&inputs)?)?;
    let result = invoke(&path, &["replay", updates.to_str().unwrap()])?;
    ensure!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let metrics: Value = serde_json::from_slice(&result.stdout)?;
    assert_eq!(metrics["queues"]["jobs"]["complete"], 1);
    assert_eq!(metrics["queues"]["jobs"]["failed"], 1);
    assert_eq!(metrics["queues"]["outbox"]["pending"], 2);
    let conn = rusqlite::Connection::open(config.state_dir.join("archive.sqlite"))?;
    let artifacts: Vec<String> = conn
        .prepare("SELECT manifest FROM artifacts")?
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    assert_eq!(artifacts.len(), 1);
    let artifact: Value = serde_json::from_str(&artifacts[0])?;
    assert_eq!(artifact["validation"]["full_decode"], true);
    assert_eq!(artifact["effective_profile"], "audio_track");
    let spoken: i64 = conn.query_row(
        "SELECT count(*) FROM derivatives WHERE kind='spoken_text'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(spoken, 0, "metadata is not a transcript");
    Store::open(config.state_dir)?.integrity()?;
    Ok(())
}
