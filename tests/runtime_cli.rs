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
