//! DA07 / #74: durable PDF publication; synthetic ntn, never real network.
#![cfg(feature = "ntn")]
use anyhow::Context;
use serde_json::json;
use std::{fs::Permissions, os::unix::fs::PermissionsExt};
use telegram_archive_bot::{
    config::{Config, Notion, NotionDestination, SecretRef},
    sources::*,
    storage::Store,
};
#[tokio::test]
async fn da07_plain_text_publication_admits_a_separate_durable_pdf_attachment() -> anyhow::Result<()>
{
    let mut root = tempfile::Builder::new()
        .permissions(Permissions::from_mode(0o700))
        .tempdir()?;
    let executable = root.path().join("ntn.py");
    std::fs::write(&executable, include_str!("fixtures/fake_ntn_worker.py"))?;
    std::fs::set_permissions(&executable, Permissions::from_mode(0o700))?;
    let token = root.path().join("token");
    std::fs::write(&token, "SYNTHETIC_NOTION_WORKER_TOKEN")?;
    std::fs::set_permissions(&token, Permissions::from_mode(0o600))?;
    std::fs::write(root.path().join("mode"), "normal")?;
    let config = Config {
        state_dir: root.path().join("store"),
        notion: Notion {
            executable: Some(executable),
            token: Some(SecretRef::File(token)),
            destination: Some(NotionDestination::Page {
                id: "11111111-1111-1111-1111-111111111111".into(),
            }),
            retry_seconds: 1,
            ..Notion::default()
        },
        ..Config::default()
    };
    let store = Store::open(&config.state_dir).context("fixture Store open")?;
    let raw = store.publish_bytes(b"%PDF-1.4 synthetic canonical original")?;
    // Preflight verifies the retained bytes before the subprocess boundary. Parent integrates
    // the separate storage-owned raw-proof repair; this is not its regression acceptance.
    assert_eq!(store.integrity()?, 1);
    store.capture(&Capture{identity:Identity::from_url("https://example.org/paper.pdf")?,submitted_url:Some("https://example.org/paper.pdf".into()),content:Some(Content{body:"Readable retained PDF source text".into(),metadata:ContentMetadata::default()}),status:FetchStatus::Complete,extractor:"pdf-text".into(),evidence:json!({"submitted_url":"https://example.org/paper.pdf","resolved_url":"https://cdn.example.org/paper.pdf?signature=temporary","retrieved_at":100,"original":{"digest":raw.digest,"mime":"application/pdf","display_name":"paper.pdf","validated":true},"pdf_validation":{"status":"validated"}}),raw_digest:Some(raw.digest.clone()),relations:vec![]}).context("fixture original capture")?;
    let cancel = tokio_util::sync::CancellationToken::new();
    for index in 0..2 {
        telegram_archive_bot::notion_worker::tick(&store, &config, &cancel)
            .await
            .with_context(|| format!("publication tick {index}"))?;
    }
    let rows = store
        .notion_list(100)
        .context("public queued-intent list")?;
    assert_eq!(
        rows.iter()
            .filter(|r| r["kind"] == "pdf_attachment")
            .count(),
        1,
        "verified ordinary text page must durably admit a separate PDF attachment intent"
    );
    let remote: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("remote.json"))?)?;
    assert!(
        remote
            .to_string()
            .contains("Readable retained PDF source text")
    );
    assert_eq!(
        store.read_blob(&raw.digest, 1000)?,
        b"%PDF-1.4 synthetic canonical original"
    );
    let cfg = root.path().join("bot.toml");
    std::fs::write(&cfg, toml::to_string(&config)?)?;
    let calls = std::fs::read(root.path().join("calls"))?;
    root.disable_cleanup(true);
    eprintln!(
        "DA07 public CLI fixture retained until success: {}",
        root.path().display()
    );
    let independent=std::process::Command::new("/usr/bin/python3").args(["-c","import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); print(c.execute('SELECT count(*) FROM notion_pdf').fetchone()[0])",store.root().join("archive.sqlite").to_str().unwrap()]).output()?;
    std::fs::write(
        root.path().join("independent-sqlite.stdout"),
        &independent.stdout,
    )?;
    std::fs::write(
        root.path().join("independent-sqlite.stderr"),
        &independent.stderr,
    )?;
    assert!(
        independent.status.success(),
        "{}",
        String::from_utf8_lossy(&independent.stderr)
    );
    assert_eq!(
        String::from_utf8(independent.stdout)?.trim(),
        "1",
        "independent committed visibility before CLI"
    );
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_telegram-archive-bot"))
        .args([
            "--config",
            cfg.to_str().unwrap(),
            "notion-pdf-status",
            "--limit",
            "10",
        ])
        .env_clear()
        .env("HOME", root.path())
        .env("PATH", "/usr/bin:/bin")
        .output()?;
    std::fs::write(root.path().join("cli.stdout"), &output.stdout)?;
    std::fs::write(root.path().join("cli.stderr"), &output.stderr)?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let portable: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(
        portable[0]["original"]["canonical_url"], "https://example.org/paper.pdf",
        "actual public CLI output={portable}"
    );
    assert_eq!(portable[0]["attachment"], "pending");
    assert_eq!(std::fs::read(root.path().join("calls"))?, calls);
    assert!(!String::from_utf8(output.stdout)?.contains(store.root().to_str().unwrap()));
    root.disable_cleanup(false);
    Ok(())
}

#[test]
fn actual_pre15_database_migrates_to_pdf_without_a_placeholder16() -> anyhow::Result<()> {
    let root = tempfile::Builder::new()
        .permissions(Permissions::from_mode(0o700))
        .tempdir()?;
    {
        let conn = rusqlite::Connection::open(root.path().join("archive.sqlite"))?;
        for sql in [
            include_str!("../migrations/001_canonical.sql"),
            include_str!("../migrations/002_revision_quality.sql"),
            include_str!("../migrations/003_ingress.sql"),
            include_str!("../migrations/004_job_recovery.sql"),
            include_str!("../migrations/005_derivatives.sql"),
            include_str!("../migrations/006_immutable_artifacts.sql"),
            include_str!("../migrations/007_delivery_attempts.sql"),
            include_str!("../migrations/008_projection.sql"),
            include_str!("../migrations/009_recovery.sql"),
            include_str!("../migrations/010_transforms.sql"),
            include_str!("../migrations/011_message_origins.sql"),
            include_str!("../migrations/012_observation_source.sql"),
            include_str!("../migrations/013_account_activities.sql"),
            include_str!("../migrations/014_daily_planner.sql"),
        ] {
            conn.execute_batch(sql)?;
        }
        conn.pragma_update(None, "user_version", 14)?;
    }
    let store = Store::open(root.path())?;
    assert!(store.notion_pdf_availability(10)?.is_empty());
    assert!(store.notion_list(10)?.is_empty());
    assert_eq!(store.integrity()?, 0);
    Ok(())
}
