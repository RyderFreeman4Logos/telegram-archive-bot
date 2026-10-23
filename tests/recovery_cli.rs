use anyhow::{Result, ensure};
use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Command};
fn run(home: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new(env!("CARGO_BIN_EXE_telegram-archive-bot"))
        .args(args)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", home)
        .output()?;
    ensure!(
        output.status.success(),
        "CLI {:?}: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}
#[test]
fn documented_full_recovery_commands_keep_effects_paused() -> Result<()> {
    let root = tempfile::Builder::new()
        .prefix("archive-recovery-cli-")
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in("/var/tmp")?;
    let state = root.path().join("state");
    let config = root.path().join("bot.toml");
    fs::write(
        &config,
        format!("state_dir = {}\n", serde_json::to_string(&state)?),
    )?;
    run(root.path(), &["--config", config.to_str().unwrap(), "init"])?;
    let backup = root.path().join("backup");
    run(
        root.path(),
        &[
            "--config",
            config.to_str().unwrap(),
            "backup",
            backup.to_str().unwrap(),
        ],
    )?;
    run(root.path(), &["verify-backup", backup.to_str().unwrap()])?;
    let restored = root.path().join("restored");
    let report = run(
        root.path(),
        &[
            "restore-backup",
            backup.to_str().unwrap(),
            restored.to_str().unwrap(),
        ],
    )?;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&report)?["external_side_effects_paused"],
        true
    );
    fs::write(
        &config,
        format!("state_dir = {}\n", serde_json::to_string(&restored)?),
    )?;
    assert!(
        run(
            root.path(),
            &["--config", config.to_str().unwrap(), "recovery-status"]
        )?
        .contains("true")
    );
    run(
        root.path(),
        &["--config", config.to_str().unwrap(), "verify"],
    )?;
    run(
        root.path(),
        &[
            "--config",
            config.to_str().unwrap(),
            "release-recovery-hold",
        ],
    )?;
    assert!(
        run(
            root.path(),
            &["--config", config.to_str().unwrap(), "recovery-status"]
        )?
        .contains("false")
    );
    Ok(())
}
