use anyhow::Result;
use std::{
    collections::BTreeMap,
    fs::{self, Permissions},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    time::Duration,
};
use telegram_archive_bot::{
    process::{Tool, ToolFailure, confined_file},
    storage::Store,
};
use tokio_util::sync::CancellationToken;

fn store() -> Result<(tempfile::TempDir, Store)> {
    let dir = tempfile::Builder::new()
        .permissions(Permissions::from_mode(0o700))
        .tempdir_in(Path::new(env!("CARGO_MANIFEST_DIR")).join("target"))?;
    let store = Store::open(dir.path())?;
    Ok((dir, store))
}
fn tool(directory: &Path, mode: &str) -> Tool {
    Tool {
        executable: PathBuf::from("/usr/bin/python3"),
        args: vec![
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/fake_tool.py")
                .into_os_string(),
            mode.into(),
        ],
        environment: BTreeMap::new(),
        directory: directory.to_owned(),
        timeout: Duration::from_secs(3),
        log_bytes: 4096,
        work_bytes: 1_000_000,
        secrets: vec![],
    }
}

#[tokio::test]
async fn literal_arguments_isolated_names_and_redacted_environment() -> Result<()> {
    let (_dir, store) = store()?;
    let first = store.workspace("same-job")?;
    let second = store.workspace("same-job")?;
    assert_ne!(first.path(), second.path());
    let mut command = tool(first.path(), "args");
    let literal = "a name; $(touch escaped) `touch escaped` 中文";
    command.args.push(literal.into());
    let output = command.run(&CancellationToken::new()).await?;
    assert_eq!(
        serde_json::from_str::<Vec<String>>(&output.stdout)?,
        vec![literal]
    );
    assert!(!first.path().join("escaped").exists());
    assert_eq!(output.executable_digest.len(), 64);
    let mut command = tool(first.path(), "env");
    command.environment.insert(
        "CHILD_ALLOWED_SECRET".into(),
        "synthetic-sensitive-value".into(),
    );
    command.secrets.push("synthetic-sensitive-value".into());
    let output = command.run(&CancellationToken::new()).await?;
    assert!(!output.stdout.contains("synthetic-sensitive-value"));
    let env: serde_json::Value = serde_json::from_str(&output.stdout)?;
    assert_eq!(env["CHILD_ALLOWED_SECRET"], "[REDACTED]");
    assert!(env.get("PARENT_ONLY_SECRET").is_none());
    assert_eq!(env["HOME"], first.path().to_str().unwrap());
    Ok(())
}

#[test]
fn parent_environment_fixture() -> Result<()> {
    if std::env::var_os("ARCHIVE_ENV_CHILD").is_some() {
        return Ok(());
    }
    let status = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "literal_arguments_isolated_names_and_redacted_environment",
        ])
        .env("ARCHIVE_ENV_CHILD", "1")
        .env("PARENT_ONLY_SECRET", "synthetic-parent-only-value")
        .stdout(std::process::Stdio::null())
        .status()?;
    assert!(status.success());
    Ok(())
}

#[tokio::test]
async fn cancellation_kills_and_reaps_process_group_including_term_resistant_child() -> Result<()> {
    let (_dir, store) = store()?;
    let work = store.workspace("cancel")?;
    let command = tool(work.path(), "spawn");
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let handle = tokio::spawn(async move { command.run(&token).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !work.path().join("child.pid").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let child: i32 = fs::read_to_string(work.path().join("child.pid"))?.parse()?;
    let parent: i32 = fs::read_to_string(work.path().join("parent.pid"))?.parse()?;
    cancel.cancel();
    assert_eq!(handle.await?.unwrap_err(), ToolFailure::Cancelled);
    for pid in [parent, child] {
        assert!(
            !Path::new("/proc").join(pid.to_string()).exists(),
            "process {pid} was not reaped"
        );
    }
    Ok(())
}

#[tokio::test]
async fn output_disk_timeout_exit_and_missing_tools_are_distinct() -> Result<()> {
    let (_dir, store) = store()?;
    for (mode, expected) in [
        ("flood", ToolFailure::LogLimit),
        ("disk", ToolFailure::WorkLimit),
        ("nonzero", ToolFailure::Nonzero),
        ("symlink", ToolFailure::UnsafeOutput),
    ] {
        let work = store.workspace(mode)?;
        let mut command = tool(work.path(), mode);
        if mode == "disk" {
            command.work_bytes = 8192;
        }
        assert_eq!(
            command.run(&CancellationToken::new()).await.unwrap_err(),
            expected
        );
    }
    let work = store.workspace("timeout")?;
    let mut command = tool(work.path(), "spawn");
    command.timeout = Duration::from_millis(100);
    assert_eq!(
        command.run(&CancellationToken::new()).await.unwrap_err(),
        ToolFailure::Timeout
    );
    command.executable = PathBuf::from("/no-such-program");
    assert_eq!(
        command.run(&CancellationToken::new()).await.unwrap_err(),
        ToolFailure::Missing
    );
    command.executable = PathBuf::from("relative-program");
    assert_eq!(
        command.run(&CancellationToken::new()).await.unwrap_err(),
        ToolFailure::InvalidConfiguration
    );
    let report = tool(work.path(), "nonzero")
        .run(&CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(report.report.as_ref().unwrap().exit_code, Some(7));
    Ok(())
}

#[test]
fn reported_paths_reject_symlinks_traversal_and_hardlinks() -> Result<()> {
    let (_dir, store) = store()?;
    let work = store.workspace("paths")?;
    fs::write(work.path().join("valid name.txt"), b"valid")?;
    assert!(confined_file(work.path(), Path::new("valid name.txt")).is_ok());
    assert!(confined_file(work.path(), Path::new("../outside")).is_err());
    assert!(confined_file(work.path(), Path::new("/etc/passwd")).is_err());
    std::os::unix::fs::symlink("/etc/passwd", work.path().join("escape"))?;
    assert!(confined_file(work.path(), Path::new("escape")).is_err());
    fs::hard_link(
        work.path().join("valid name.txt"),
        work.path().join("hardlink"),
    )?;
    assert!(confined_file(work.path(), Path::new("hardlink")).is_err());
    Ok(())
}
