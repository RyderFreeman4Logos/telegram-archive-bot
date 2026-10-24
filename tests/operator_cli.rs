#[test]
fn public_operator_batch_is_read_only_scoped_and_resumable() -> anyhow::Result<()> {
    let root = tempfile::Builder::new()
        .prefix("operator-cli-")
        .tempdir_in("/var/tmp")?;
    let output = std::process::Command::new("python3")
        .arg("tests/fixtures/operator_cli.py")
        .arg(env!("CARGO_BIN_EXE_telegram-archive-bot"))
        .arg(root.path().canonicalize()?)
        .output()?;
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
